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
DB_NAME = "sniper_pro.db"

# FILTERS
MIN_24H_VOL = 1000000        # Mid-caps+
RVOL_THRESHOLD = 3.0         # 3x Volume Anomaly
STAGE_3_CVD_THRESHOLD = 25000 
WALL_THRESHOLD_MULT = 3.0    # Orderbook Wall Size

# --- GLOBAL STATE ---
market_state = {} 
graduated_coins = {"Stage 1": set(), "Stage 2": set(), "Stage 3": set()}
active_granular_subs = set()
trade_buffer = {} 
vol_history = {} 
active_signals = {} # {symbol: {entry, sl, tp1, tp2, tp3, magnet_note, time}}

console = Console()

# --- DATABASE ---
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT, time TEXT, entry REAL, sl REAL, 
                tp1 REAL, tp2 REAL, tp3 REAL, rvol REAL
            )
        ''')
        await db.commit()

async def save_signal_db(s):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT INTO signals (symbol, time, entry, sl, tp1, tp2, tp3, rvol) VALUES (?,?,?,?,?,?,?,?)",
            (s['symbol'], s['time'], s['entry'], s['sl'], s['tp1'], s['tp2'], s['tp3'], s['rvol'])
        )
        await db.commit()

# --- 1. MAGNET ENGINE (The Brain) ---
async def calculate_magnetic_levels(symbol, entry_price):
    """Scans Orderbook to find Walls for TP/SL"""
    url = f"{BYBIT_REST_URL}?category=linear&symbol={symbol}&limit=50"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                data = await response.json()
                if data['retCode'] != 0: return None
                
                bids = data['result']['b']['b']
                asks = data['result']['b']['a']
                if not bids or not asks: return None

                # Find Walls (3x Median)
                ask_vols = np.array([float(x[1]) for x in asks])
                threshold = np.median(ask_vols) * WALL_THRESHOLD_MULT
                walls = [float(x[0]) for x in asks if float(x[1]) > threshold]
                
                # Default Mathematical Ladder (1.5R, 3R, 5R)
                # We assume a tight 1% risk for calculation base
                risk = entry_price * 0.01 
                tp1 = entry_price + (1.5 * risk)
                tp2 = entry_price + (3.0 * risk)
                tp3 = entry_price + (5.0 * risk)
                
                magnet_note = "Math"
                
                # Adjust TPs to Front-Run Walls
                if walls:
                    wall = walls[0]
                    if wall < tp2 and wall > entry_price:
                        tp2 = wall * 0.999 # Front-run
                        magnet_note = "🧲 TP2 Magnetized"
                    if wall < tp3 and wall > tp2:
                        tp3 = wall * 0.999
                        magnet_note = "🧲 TP3 Magnetized"

                # Smart SL (Below local structure)
                sl = entry_price * 0.985 # 1.5% fixed safety for violence
                
                return sl, tp1, tp2, tp3, magnet_note
    except:
        return entry_price*0.985, entry_price*1.015, entry_price*1.03, entry_price*1.05, "Fallback"

# --- 2. TRIGGER ENGINE (The Muscle) ---
def calculate_rvol(symbol, current_1m_vol):
    if symbol not in vol_history:
        vol_history[symbol] = deque(maxlen=20)
        vol_history[symbol].append(current_1m_vol)
        return 1.0
    avg_vol = sum(vol_history[symbol]) / len(vol_history[symbol])
    vol_history[symbol].append(current_1m_vol)
    return current_1m_vol / avg_vol if avg_vol > 0 else 1.0

async def trigger_breakout(symbol, price, cvd, vol):
    if symbol in active_signals: return

    # A. Check Violence (RVOL)
    rvol = calculate_rvol(symbol, vol)
    if rvol < RVOL_THRESHOLD: return

    # B. Calculate Magnets
    sl, tp1, tp2, tp3, note = await calculate_magnetic_levels(symbol, price)

    # C. Register Signal
    signal = {
        'symbol': symbol,
        'time': datetime.now().strftime("%H:%M:%S"),
        'entry': price,
        'sl': sl,
        'tp1': tp1,
        'tp2': tp2,
        'tp3': tp3,
        'rvol': rvol,
        'note': note,
        'status': "OPEN"
    }
    active_signals[symbol] = signal
    await save_signal_db(signal)

# --- 3. WS FEED ---
async def bybit_driver():
    # Initial Ticker Fetch
    url = "https://api.bybit.com/v5/market/tickers?category=linear"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            res = await response.json()
            symbols = [i['symbol'] for i in res['result']['list'] if i['symbol'].endswith('USDT') and float(i['turnover24h']) > MIN_24H_VOL]

    async with websockets.connect(BYBIT_WS_URL) as ws:
        for i in range(0, len(symbols), 10):
            await ws.send(json.dumps({"op": "subscribe", "args": [f"tickers.{s}" for s in symbols[i:i+10]]}))
            await asyncio.sleep(0.1)

        while True:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                data = json.loads(msg)
                
                if 'topic' in data:
                    # TICKERS (Light)
                    if 'tickers' in data['topic']:
                        t = data['data']
                        sym = t['symbol']
                        if sym not in market_state: market_state[sym] = {'p': 0.0, 'chg': 0.0}
                        
                        if 'lastPrice' in t: market_state[sym]['p'] = float(t['lastPrice'])
                        if 'price24hPcnt' in t: market_state[sym]['chg'] = float(t['price24hPcnt'])
                        
                        # Funnel Logic
                        chg = market_state[sym]['chg']
                        graduated_coins["Stage 1"].add(sym)
                        if abs(chg) < 0.05: graduated_coins["Stage 2"].add(sym)
                        else: graduated_coins["Stage 2"].discard(sym)

                    # TRADES (Heavy)
                    elif 'publicTrade' in data['topic']:
                        for x in data['data']:
                            sym = x['s']
                            if sym not in trade_buffer: trade_buffer[sym] = deque(maxlen=1000)
                            
                            val = float(x['v']) * float(x['p'])
                            trade_buffer[sym].append({'val': val * (1 if x['S'] == 'Buy' else -1), 'abs': val, 'ts': int(x['T'])})
                            
                            # Cleanup
                            cutoff = int(x['T']) - 60000
                            while trade_buffer[sym] and trade_buffer[sym][0]['ts'] < cutoff: trade_buffer[sym].popleft()
                            
                            cvd = sum(i['val'] for i in trade_buffer[sym])
                            vol = sum(i['abs'] for i in trade_buffer[sym])
                            
                            if cvd > STAGE_3_CVD_THRESHOLD:
                                graduated_coins["Stage 3"].add(sym)
                                await trigger_breakout(sym, float(x['p']), cvd, vol)

                # Dynamic Subs
                new_subs = graduated_coins["Stage 2"] - active_granular_subs
                if new_subs:
                    batch = list(new_subs)[:30]
                    if batch:
                        await ws.send(json.dumps({"op": "subscribe", "args": [f"publicTrade.{s}" for s in batch]}))
                        active_granular_subs.update(batch)

            except Exception: pass

# --- UI RENDERER (TRAFFIC LIGHT SYSTEM) ---
def get_live_signal_table():
    table = Table(title="🚦 ACTIVE TRADES & MAGNETIC LADDER 🚦", style="bold white", border_style="bold red", box=box.HEAVY, expand=True)
    
    table.add_column("Light", justify="center", width=8)
    table.add_column("Symbol", style="bold yellow")
    table.add_column("Current Price", justify="right", style="bold white")
    table.add_column("Entry", style="blue")
    table.add_column("TP1", style="dim green")
    table.add_column("TP2", style="green")
    table.add_column("TP3 (Magnet)", style="bold green")
    table.add_column("Status", justify="center")

    # Filter last 8 signals
    recent = list(active_signals.values())[-8:]
    
    for s in reversed(recent):
        sym = s['symbol']
        curr_p = market_state.get(sym, {}).get('p', s['entry']) # Get live price
        
        # 1. Traffic Light Logic
        if curr_p >= s['entry']:
            light = "🟢 GOOD"
            row_style = ""
        elif curr_p > s['sl']:
            light = "🟡 HOLD"
            row_style = "dim"
        else:
            light = "🔴 EXIT"
            row_style = "strike red"

        # 2. Ladder Tracking
        status_msg = "Chasing..."
        if curr_p >= s['tp3']: status_msg = "🏆 TP3 SMASHED"
        elif curr_p >= s['tp2']: status_msg = "✅ TP2 HIT"
        elif curr_p >= s['tp1']: status_msg = "✅ TP1 HIT"
        elif curr_p <= s['sl']: status_msg = "💀 STOPPED"

        # 3. Render Row
        table.add_row(
            light,
            sym,
            f"{curr_p:.4f}",
            f"{s['entry']:.4f}",
            f"{s['tp1']:.4f}",
            f"{s['tp2']:.4f}",
            f"{s['tp3']:.4f}",
            status_msg,
            style=row_style
        )
    return table

def get_funnel_panels():
    # Sort by Volatility (Best movers on top)
    s1 = sorted(list(graduated_coins["Stage 1"]), key=lambda x: abs(market_state.get(x, {}).get('chg', 0)), reverse=True)[:8]
    s2 = sorted(list(graduated_coins["Stage 2"]), key=lambda x: abs(market_state.get(x, {}).get('chg', 0)), reverse=True)[:8]
    s3 = sorted(list(graduated_coins["Stage 3"]), key=lambda x: abs(market_state.get(x, {}).get('chg', 0)), reverse=True)[:8]

    def make_table(title, coins, col):
        t = Table(title=f"{title} ({len(coins)})", expand=True, border_style=col)
        t.add_column("Sym"); t.add_column("Price"); t.add_column("24h%")
        for c in coins:
            d = market_state.get(c, {})
            t.add_row(c, str(d.get('p',0)), f"{d.get('chg',0)*100:.1f}%")
        return Panel(t)

    layout = Layout()
    layout.split_row(
        Layout(make_table("S1: BIAS", s1, "green")),
        Layout(make_table("S2: COIL", s2, "yellow")),
        Layout(make_table("S3: IGNITION", s3, "red"))
    )
    return layout

async def main():
    await init_db()
    console.print("[bold red]SYSTEM START: TRAFFIC LIGHTS & MAGNETS ACTIVE[/bold red]")
    
    layout = Layout()
    layout.split_column(Layout(name="top", ratio=1), Layout(name="bottom", ratio=1))
    
    asyncio.create_task(bybit_driver())
    
    with Live(layout, refresh_per_second=4) as live:
        while True:
            layout["top"].update(get_funnel_panels())
            layout["bottom"].update(get_live_signal_table())
            await asyncio.sleep(0.1)

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: pass