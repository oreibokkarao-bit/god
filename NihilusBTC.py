import asyncio
import json
import aiohttp
import websockets
import aiosqlite
import numpy as np
from datetime import datetime
from collections import deque
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.console import Console
from rich import box

# --- CONFIGURATION ---
BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"
BYBIT_REST_URL = "https://api.bybit.com/v5/market/orderbook"
DB_NAME = "magnetic_trades.db"
MIN_24H_VOL = 10000000 

# --- MAGNETIC LOGIC CONFIG ---
BASE_R_TP1 = 1.5
BASE_R_TP2 = 3.0
BASE_R_TP3 = 5.0
WALL_THRESHOLD_MULT = 3.0 
SL_WICK_BUFFER = 0.002    

# --- THRESHOLDS ---
STAGE_2_MAX_PRICE_CHANGE = 0.02 
STAGE_3_CVD_THRESHOLD = 50000   

# --- GLOBAL STATE ---
market_state = {} 
graduated_coins = {"Stage 1": set(), "Stage 2": set(), "Stage 3": set()}
active_granular_subs = set()
trade_buffer = {} 
active_signals = {} 

console = Console()

# --- DATABASE ---
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS magnetic_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                time TEXT,
                entry REAL,
                sl REAL,
                tp1 REAL,
                tp2 REAL,
                tp3 REAL,
                magnet_type TEXT
            )
        ''')
        await db.commit()

async def save_signal_to_db(sig):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            """INSERT INTO magnetic_signals 
               (symbol, time, entry, sl, tp1, tp2, tp3, magnet_type) 
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (sig['symbol'], sig['time'], sig['entry'], sig['sl'], 
             sig['tp1'], sig['tp2'], sig['tp3'], sig['magnet'])
        )
        await db.commit()

# --- MAGNETIC ENGINE ---
async def fetch_orderbook_magnets(symbol, entry_price):
    url = f"{BYBIT_REST_URL}?category=linear&symbol={symbol}&limit=50"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.json()
            if data['retCode'] != 0: return None, None
            
            bids = data['result']['b']['b']
            asks = data['result']['b']['a']
            
            if not bids or not asks: return None, None

            bid_vols = np.array([float(x[1]) for x in bids])
            ask_vols = np.array([float(x[1]) for x in asks])
            
            avg_bid_vol = np.median(bid_vols)
            avg_ask_vol = np.median(ask_vols)
            bid_threshold = avg_bid_vol * WALL_THRESHOLD_MULT
            ask_threshold = avg_ask_vol * WALL_THRESHOLD_MULT
            
            significant_bids = [b for b in bids if float(b[1]) > bid_threshold]
            bid_wall_price = float(significant_bids[0][0]) if significant_bids else float(bids[-1][0])

            significant_asks = [a for a in asks if float(a[1]) > ask_threshold]
            ask_wall_price = float(significant_asks[0][0]) if significant_asks else float(asks[-1][0])
            
            return bid_wall_price, ask_wall_price

async def generate_magnetic_signal(symbol, current_price, local_low):
    if symbol in active_signals: return

    bid_wall, ask_wall = await fetch_orderbook_magnets(symbol, current_price)
    math_sl = local_low * (1 - SL_WICK_BUFFER)
    
    final_sl = math_sl
    magnet_note = "Math"
    
    if bid_wall and bid_wall < current_price:
        magnet_sl = bid_wall * (1 - SL_WICK_BUFFER)
        if magnet_sl > math_sl:
            final_sl = magnet_sl
            magnet_note = "🛡️ Protected by Wall"

    risk_delta = current_price - final_sl
    
    math_tp1 = current_price + (risk_delta * BASE_R_TP1)
    math_tp2 = current_price + (risk_delta * BASE_R_TP2)
    math_tp3 = current_price + (risk_delta * BASE_R_TP3)
    
    final_tp1, final_tp2, final_tp3 = math_tp1, math_tp2, math_tp3
    
    if ask_wall and ask_wall > current_price:
        if math_tp2 > ask_wall:
            final_tp2 = ask_wall * 0.999
            magnet_note += " | 🧲 TP2 Magnetized"
        if math_tp3 > ask_wall:
            final_tp3 = ask_wall * 0.999
            magnet_note += " | 🧲 TP3 Magnetized"

    signal = {
        'symbol': symbol,
        'time': datetime.now().strftime("%H:%M:%S"),
        'entry': current_price,
        'sl': final_sl,
        'tp1': final_tp1,
        'tp2': final_tp2,
        'tp3': final_tp3,
        'magnet': magnet_note
    }
    
    active_signals[symbol] = signal
    await save_signal_to_db(signal)

# --- NETWORK & WS ---
async def fetch_tickers():
    url = "https://api.bybit.com/v5/market/tickers?category=linear"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.json()
            if 'result' in data:
                return [i['symbol'] for i in data['result']['list'] if i['symbol'].endswith('USDT') and float(i['turnover24h']) > MIN_24H_VOL]
    return []

async def bybit_feed(symbols):
    async with websockets.connect(BYBIT_WS_URL) as ws:
        # Subscribe Tickers
        chunk_size = 10
        for i in range(0, len(symbols), chunk_size):
            await ws.send(json.dumps({"op": "subscribe", "args": [f"tickers.{s}" for s in symbols[i:i+chunk_size]]}))
            await asyncio.sleep(0.1)

        while True:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                data = json.loads(msg)
                
                if 'topic' in data:
                    if 'tickers' in data['topic']:
                        tick = data['data']
                        sym = tick['symbol']
                        if sym not in market_state: market_state[sym] = {'price': 0.0, 'oi': 0.0}
                        
                        s = market_state[sym]
                        if 'lastPrice' in tick: s['prev_p'] = s['price']; s['price'] = float(tick['lastPrice'])
                        if 'openInterest' in tick: s['prev_oi'] = s['oi']; s['oi'] = float(tick['openInterest'])
                        
                        price_chg = (s['price'] - s.get('prev_p', 0)) / s.get('prev_p', 1)
                        if s['price'] > 0 and s['oi'] > 0: graduated_coins["Stage 1"].add(sym)
                        if sym in graduated_coins["Stage 1"] and abs(price_chg) < STAGE_2_MAX_PRICE_CHANGE:
                            graduated_coins["Stage 2"].add(sym)

                    elif 'publicTrade' in data['topic']:
                        for t in data['data']:
                            sym = t['s']
                            # Placeholder for CVD Logic
                            if sym in graduated_coins["Stage 2"]: 
                                # Simulating Ignition for Logic testing
                                # In production, replace this with CVD > THRESHOLD check
                                graduated_coins["Stage 3"].add(sym)
                                await generate_magnetic_signal(sym, float(t['p']), float(t['p']))

                # Dynamic Subs
                stage_2 = graduated_coins["Stage 2"]
                new = stage_2 - active_granular_subs
                if new:
                    await ws.send(json.dumps({"op": "subscribe", "args": [f"publicTrade.{s}" for s in new]}))
                    active_granular_subs.update(new)

            except Exception: pass

# --- UI VISUALIZATION ---
def make_layout():
    layout = Layout()
    layout.split_column(
        Layout(name="stages", ratio=1),
        Layout(name="signals", ratio=1)
    )
    layout["stages"].split_row(
        Layout(name="s1"),
        Layout(name="s2"),
        Layout(name="s3")
    )
    return layout

def get_stage_table(name, coins, style="blue"):
    t = Table(title=f"{name} ({len(coins)})", expand=True, border_style=style)
    t.add_column("Symbol", justify="center")
    t.add_column("Price", justify="right")
    
    # Sort and take top 8 to fit screen
    sorted_coins = sorted(list(coins))[:8]
    
    for c in sorted_coins:
        p = market_state.get(c, {}).get('price', 0)
        t.add_row(c, f"{p:.4f}")
    
    return Panel(t)

def get_magnetic_table():
    table = Table(title="🧲 MAGNETIC SNIPER SIGNALS 🧲", border_style="bold magenta", box=box.ROUNDED, expand=True)
    table.add_column("Time", style="dim")
    table.add_column("Symbol", style="bold yellow")
    table.add_column("Entry", style="blue")
    table.add_column("🛡️ Smart SL", style="bold red")
    table.add_column("TP1", style="green")
    table.add_column("TP2 (Magnetic)", style="bold green")
    table.add_column("TP3 (Magnetic)", style="bold green")
    table.add_column("Structure Note", style="italic cyan")

    for s in list(active_signals.values())[-5:]:
        table.add_row(
            s['time'], s['symbol'], 
            f"{s['entry']:.4f}", 
            f"{s['sl']:.4f}", 
            f"{s['tp1']:.4f}", 
            f"{s['tp2']:.4f}", 
            f"{s['tp3']:.4f}",
            s['magnet']
        )
    return table

# --- MAIN ---
async def main():
    await init_db()
    console.print("[bold yellow]Initializing Graduation Funnel & Magnetic Engine...[/bold yellow]")
    syms = await fetch_tickers()
    console.print(f"[green]Tracking {len(syms)} Pairs[/green]")
    
    layout = make_layout()
    asyncio.create_task(bybit_feed(syms))
    
    with Live(layout, refresh_per_second=4, screen=True) as live:
        while True:
            # Update Graduation Panels
            layout["s1"].update(get_stage_table("S1: BIAS", graduated_coins["Stage 1"], "green"))
            layout["s2"].update(get_stage_table("S2: COIL", graduated_coins["Stage 2"], "yellow"))
            layout["s3"].update(get_stage_table("S3: IGNITION", graduated_coins["Stage 3"], "red"))
            
            # Update Signal Table
            layout["signals"].update(get_magnetic_table())
            
            await asyncio.sleep(0.5)

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: pass