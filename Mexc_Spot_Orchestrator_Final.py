import asyncio
import sqlite3
import logging
import time
import statistics
import math
import ccxt.async_support as ccxt
from datetime import datetime
from typing import List, Dict, Any, Optional
from collections import deque

# --- RICH UI IMPORTS ---
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich import box
from rich.console import Console
from rich.text import Text

# ============================================================
#   MEXC ORCHESTRATOR FINAL: THE "GOD MODE" ENGINE
# ============================================================

# --- CONFIGURATION ---
MIN_24H_VOLUME_USDT = 50_000      
MAX_24H_VOLUME_USDT = 50_000_000  
MIN_PRICE_CHANGE_24H = 5.0        
MAX_SPREAD_PCT = 2.0              
MIN_BOOK_DEPTH_USDT = 10_000      
VOL_ZSCORE_THRESHOLD = 3.0        
BASELINE_PERIOD = 90              
MAX_OTR_THRESHOLD = 50.0          
SCAN_INTERVAL_SECONDS = 20        
ALERT_THRESHOLD = 80              
DB_NAME = "mexc_orchestrator.sqlite"

# --- STATE MANAGEMENT ---
class ScreenerState:
    def __init__(self):
        self.status = "Initializing Final Engine..."
        self.last_scan_time = "N/A"
        self.candidates: List[Dict] = []
        self.alerts = deque(maxlen=10)
        self.active_trades: Dict[str, 'ActiveTrade'] = {} 

state = ScreenerState()

# --- THE BRAIN: DYNAMIC TP ENGINE (Enhanced) ---
class ActiveTrade:
    def __init__(self, symbol, entry_price, volatility_unit, start_time):
        self.symbol = symbol
        self.entry_price = entry_price
        self.entry_time = start_time
        self.volatility = volatility_unit # Sigma (σ)
        self.current_price = entry_price
        self.highest_price = entry_price
        self.status = "ENTRY" 
        
        # --- DYNAMIC TARGETS (GEARS) ---
        # TP1: Safety Lock (Quick Scalp)
        self.tp1 = entry_price + (1.0 * volatility_unit)
        # TP2: Trend Rider (The Meat)
        self.tp2 = entry_price + (2.0 * volatility_unit)
        # TP3: Moonbag (The Home Run)
        self.tp3 = entry_price + (4.0 * volatility_unit)
        
        # Stop Loss (Volatility Adjusted)
        self.sl = entry_price - (2.0 * volatility_unit) 
        
        self.logs = []
        self.pnl_locked = 0.0

    def update(self, current_price, vol_z_5m, rsi, ask_wall_price=None):
        self.current_price = current_price
        if current_price > self.highest_price:
            self.highest_price = current_price
            
        # --- INTELLIGENT TARGET ADJUSTMENT (Report Insight) ---
        # If a massive sell wall is DETECTED just below our target, front-run it.
        if ask_wall_price:
            if self.status == "ENTRY" and self.tp1 > ask_wall_price > current_price:
                self.tp1 = ask_wall_price * 0.999 # Front-run wall
                self.log_event(f"⚠️ TP1 adjusted below wall: {self.tp1:.5f}")
            elif self.status == "GEAR_1" and self.tp2 > ask_wall_price > current_price:
                self.tp2 = ask_wall_price * 0.999
                self.log_event(f"⚠️ TP2 adjusted below wall: {self.tp2:.5f}")

        # --- GEAR SHIFT LOGIC ---
        
        # GEAR 1: SAFETY LOCK (+1σ)
        if self.status == "ENTRY" and current_price >= self.tp1:
            self.status = "GEAR_1"
            self.sl = self.entry_price * 1.002 # Breakeven + Fees
            self.pnl_locked += (self.tp1 - self.entry_price) * 0.5 # Realize 50%
            self.log_event("✅ TP1 HIT: Sold 50% | SL -> Breakeven")
            update_trade_db(self, "TP1_HIT")

        # GEAR 2: TREND RIDER (+2σ)
        elif self.status == "GEAR_1" and current_price >= self.tp2:
            self.status = "GEAR_2"
            self.pnl_locked += (self.tp2 - self.entry_price) * 0.3 # Realize 30%
            self.log_event("✅ TP2 HIT: Sold 30% | Trailing Active")
            update_trade_db(self, "TP2_HIT")
            
        # GEAR 3: MOONBAG (+4σ)
        elif self.status == "GEAR_2" and current_price >= self.tp3:
            self.status = "GEAR_3"
            self.log_event("🚀 TP3 CROSSED: Moonbag Mode | Panic Sensor ON")
            update_trade_db(self, "TP3_CROSS")

        # --- TRAILING STOP LOGIC ---
        if self.status in ["GEAR_2", "GEAR_3"]:
            # Tighten trail if volume disappears (Report Insight)
            trail_dist = 1.5 * self.volatility
            if vol_z_5m < 0: # Volume dying
                trail_dist = 1.0 * self.volatility # Tighten up
            
            new_sl = self.highest_price - trail_dist
            if new_sl > self.sl:
                self.sl = new_sl

        # --- EXIT ---
        if current_price <= self.sl:
            self.status = "CLOSED"
            reason = "STOP_LOSS" if self.sl < self.entry_price else "TRAILING_PROFIT"
            self.log_event(f"🛑 TRADE CLOSED: {reason} @ {current_price}")
            update_trade_db(self, "CLOSED")
            return False 

        # --- REVERSAL GUARD (Panic Sensor) ---
        if self.status == "GEAR_3":
            # "Climax Pattern": Volume > 5x AND Price Stalling OR RSI > 90
            is_climax = (vol_z_5m > 5.0 and current_price < self.highest_price * 0.99)
            if is_climax or rsi > 90:
                self.status = "CLOSED"
                self.log_event(f"🚨 PANIC SELL: Climax Detected (Vol Z: {vol_z_5m:.1f})")
                update_trade_db(self, "CLOSED_PANIC")
                return False

        return True

    def log_event(self, message):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.logs.append(f"[{timestamp}] {message}")

# --- DATABASE SETUP ---
def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS mexc_alerts (symbol TEXT PRIMARY KEY, alert_timestamp INTEGER, alert_price REAL, volume_24h REAL, change_24h REAL, score INTEGER, tags TEXT)''')
    # Enhanced Trade History Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trade_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            entry_time TEXT,
            entry_price REAL,
            tp1_target REAL,
            tp2_target REAL,
            tp3_threshold REAL,
            exit_price REAL,
            status TEXT,
            last_event TEXT
        )
    ''')
    conn.commit()
    conn.close()

def has_been_alerted(symbol: str) -> bool:
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    # 4 Hour Cooldown
    threshold = int(time.time()) - (4 * 3600)
    cursor.execute("SELECT 1 FROM mexc_alerts WHERE symbol = ? AND alert_timestamp > ?", (symbol, threshold))
    res = cursor.fetchone()
    conn.close()
    return res is not None

def record_alert(data: Dict[str, Any]):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('INSERT OR REPLACE INTO mexc_alerts (symbol, alert_timestamp, alert_price, volume_24h, change_24h, score, tags) VALUES (?, ?, ?, ?, ?, ?, ?)', 
                   (data['symbol'], int(time.time()), data['price'], data['volume'], data['change_24h'], data['score'], ','.join(data['tags'])))
    conn.commit()
    conn.close()
    state.alerts.appendleft({"time": datetime.now().strftime("%H:%M:%S"), "symbol": data['symbol'], "price": data['price'], "change": data['change_24h'], "score": data['score'], "tags": data['tags']})

def update_trade_db(trade: ActiveTrade, event_type: str):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('INSERT INTO trade_history (symbol, entry_time, entry_price, tp1_target, tp2_target, tp3_threshold, exit_price, status, last_event) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                   (trade.symbol, trade.entry_time, trade.entry_price, trade.tp1, trade.tp2, trade.tp3, trade.current_price, trade.status, trade.logs[-1]))
    conn.commit()
    conn.close()

# --- ANALYTICS ENGINE ---
def calculate_z_score(values: List[float]) -> float:
    if len(values) < 10: return 0.0
    history = values[:-1]; current = values[-1]
    mean = statistics.mean(history); stdev = statistics.stdev(history)
    return (current - mean) / stdev if stdev != 0 else 0.0

async def check_liquidity_veto(exchange, symbol: str) -> Optional[Dict]:
    try:
        book = await exchange.fetch_order_book(symbol, limit=20)
        bids = book['bids']; asks = book['asks']
        if not bids or not asks: return None

        best_bid = bids[0][0]; best_ask = asks[0][0]
        mid = (best_bid + best_ask) / 2
        
        spread_pct = ((best_ask - best_bid) / mid) * 100
        if spread_pct > MAX_SPREAD_PCT: return {'failed': True}

        # Calculate Depth
        depth_usd = sum([p*q for p, q in bids if p >= mid * 0.98]) + sum([p*q for p, q in asks if p <= mid * 1.02])
        if depth_usd < MIN_BOOK_DEPTH_USDT: return {'failed': True}

        # Find massive wall (for TP adjustment)
        # Simple logic: Wall is > 5x average order size in top 10
        avg_ask_size = sum([q for p, q in asks[:10]]) / 10
        wall_price = next((p for p, q in asks[:10] if q > avg_ask_size * 5), None)

        return {'failed': False, 'wall_price': wall_price}
    except: return None

async def analyze_metrics(exchange, symbol: str) -> Dict:
    try:
        # Fetch data efficiently
        t_5m = exchange.fetch_ohlcv(symbol, '5m', limit=BASELINE_PERIOD+5)
        t_1h = exchange.fetch_ohlcv(symbol, '1h', limit=BASELINE_PERIOD+5)
        t_trades = exchange.fetch_trades(symbol, limit=50)
        
        data_5m, data_1h, trades = await asyncio.gather(t_5m, t_1h, t_trades)
        if len(data_5m) < BASELINE_PERIOD: return {}

        # Volatility (Sigma)
        closes = [x[4] for x in data_1h]
        recent = closes[-24:]
        sigma = statistics.stdev(recent) if len(recent) > 2 else closes[-1]*0.02

        # Z-Scores
        z_5m = calculate_z_score([x[5] for x in data_5m])
        z_60m = calculate_z_score([x[5] for x in data_1h])

        # RSI
        deltas = [closes[i+1]-closes[i] for i in range(len(closes)-1)]
        gain = sum(x for x in deltas[-14:] if x > 0)
        loss = abs(sum(x for x in deltas[-14:] if x < 0))
        rsi = 100 - (100/(1 + (gain/loss))) if loss != 0 else 50

        # OTR
        now = exchange.milliseconds()
        recent_trades = [t for t in trades if (now - t['timestamp']) < 60000]
        otr = 30 / len(recent_trades) if len(recent_trades) > 0 else 50.0

        return {"z_5m": z_5m, "z_60m": z_60m, "sigma": sigma, "rsi": rsi, "otr": otr}
    except: return {}

# --- PIPELINE ---
async def process_market(exchange):
    state.status = "Fetching Tickers..."
    try: tickers = await exchange.fetch_tickers()
    except: return

    candidates = []
    for s, t in tickers.items():
        if '/USDT' not in s or '3L' in s or '3S' in s: continue
        try:
            vol = float(t['quoteVolume'])
            chg = float(t['percentage'])
            price = float(t['last'])
        except: continue
        
        if vol < MIN_24H_VOLUME_USDT or vol > MAX_24H_VOLUME_USDT: continue
        if chg < MIN_PRICE_CHANGE_24H: continue
        candidates.append({'symbol': s, 'price': price, 'vol': vol, 'chg': chg})
    
    candidates.sort(key=lambda x: x['chg'], reverse=True)
    top_candidates = candidates[:25]
    
    state.candidates = []
    analyzed_batch = []
    
    symbols_to_check = set([c['symbol'] for c in top_candidates] + list(state.active_trades.keys()))
    state.status = f"Vetting {len(symbols_to_check)} Candidates..."

    for symbol in symbols_to_check:
        ticker = tickers.get(symbol)
        if not ticker: continue
        price = float(ticker['last'])

        # Liquidity Check (Get Wall Price)
        liq = await check_liquidity_veto(exchange, symbol)
        wall_price = liq['wall_price'] if liq and not liq['failed'] else None
        if symbol not in state.active_trades and (not liq or liq['failed']): continue

        # Metrics
        m = await analyze_metrics(exchange, symbol)
        if not m: continue

        # Update Active Trades
        if symbol in state.active_trades:
            trade = state.active_trades[symbol]
            alive = trade.update(price, m['z_5m'], m['rsi'], wall_price)
            if not alive: del state.active_trades[symbol]

        # Evaluate New
        cand = next((c for c in top_candidates if c['symbol'] == symbol), None)
        if cand and symbol not in state.active_trades:
            score = 40
            tags = [f"{cand['chg']:.1f}%"]
            
            if m['otr'] > MAX_OTR_THRESHOLD: score = 0; tags.append("⛔MANIPULATED")
            else:
                if m['z_5m']>3 and m['z_60m']>3: score+=50; tags.append("🚀DUAL_Z")
                elif m['z_5m']>3: score+=20; tags.append(f"Z_5m:{m['z_5m']:.1f}")
            
            final = {**cand, "score": score, "tags": tags, "z_5m": m['z_5m'], "otr": m['otr']}
            analyzed_batch.append(final)
            
            if score >= ALERT_THRESHOLD and not has_been_alerted(symbol):
                record_alert(final)
                # SPAWN TRADE ENGINE
                trade = ActiveTrade(symbol, price, m['sigma'], datetime.now().strftime("%H:%M:%S"))
                state.active_trades[symbol] = trade
                update_trade_db(trade, "OPEN_TRADE")

        await asyncio.sleep(exchange.rateLimit / 1000.0)

    analyzed_batch.sort(key=lambda x: x['score'], reverse=True)
    state.candidates = analyzed_batch

# --- VISUAL DASHBOARD ---
def generate_dashboard() -> Layout:
    layout = Layout()
    layout.split(Layout(name="header", size=3), Layout(name="top", ratio=1), Layout(name="middle", ratio=1), Layout(name="bottom", size=8))
    layout["header"].update(Panel(f"🦅 MEXC ORCHESTRATOR FINAL | OTR Shield | Dual Z-Score | Status: {state.status}", style="white on blue"))

    # Scanner
    t_scan = Table(title="🔎 VETTED CANDIDATES", expand=True, box=box.SIMPLE_HEAD)
    t_scan.add_column("Symbol", style="cyan bold")
    t_scan.add_column("Price")
    t_scan.add_column("Z-Score", justify="center")
    t_scan.add_column("OTR", justify="center")
    t_scan.add_column("Score")
    
    for c in state.candidates:
        style = "bold green" if c['score'] >= ALERT_THRESHOLD else "dim"
        otr_style = "green" if c['otr'] < 10 else "bold red"
        url = f"https://www.mexc.com/exchange/{c['symbol'].replace('/','_')}"
        sym = f"[link={url}]{c['symbol']}[/link]"
        t_scan.add_row(sym, f"${c['price']:.6f}", f"{c['z_5m']:.1f}", f"[{otr_style}]{c['otr']:.1f}[/]", f"[{style}]{c['score']}[/]")
    layout["top"].update(Panel(t_scan))

    # Active Trades
    t_trade = Table(title="⚙️ DYNAMIC TP GEARS (Live)", expand=True, box=box.HEAVY_EDGE)
    t_trade.add_column("Symbol", style="yellow bold")
    t_trade.add_column("Status", justify="center")
    t_trade.add_column("Entry")
    t_trade.add_column("Current")
    t_trade.add_column("PnL")
    t_trade.add_column("TP1", style="green")
    t_trade.add_column("TP2", style="green")
    t_trade.add_column("TP3", style="bold green")
    t_trade.add_column("SL", style="red")
    
    if not state.active_trades: t_trade.add_row("-", "No Active Trades", "-", "-", "-", "-", "-", "-", "-")
    else:
        for s, t in state.active_trades.items():
            pnl = ((t.current_price - t.entry_price)/t.entry_price)*100
            pc = "green" if pnl>0 else "red"
            style = "green" if "GEAR_1" in t.status else "bold green" if "GEAR_2" in t.status else "bold magenta" if "GEAR_3" in t.status else "dim"
            t_trade.add_row(s.replace("/USDT",""), f"[{style}]{t.status}[/]", f"${t.entry_price:.5f}", f"${t.current_price:.5f}", f"[{pc}]{pnl:.2f}%[/]", f"${t.tp1:.5f}", f"${t.tp2:.5f}", f"${t.tp3:.5f}", f"${t.sl:.5f}")
    layout["middle"].update(Panel(t_trade))

    # Logs
    t_log = Table(title="📜 TRADE LOGS", expand=True, box=box.MINIMAL, show_header=False)
    all_logs = []
    for t in state.active_trades.values():
        for l in t.logs[-2:]: all_logs.append(f"[yellow]{t.symbol}:[/] {l}")
    if not all_logs: t_log.add_row("Waiting for trade events...")
    for l in all_logs[-5:]: t_log.add_row(l)
    layout["bottom"].update(Panel(t_log))
    return layout

async def main():
    init_db()
    exchange = ccxt.mexc({'enableRateLimit': True, 'options': {'defaultType': 'spot'}})
    try: await exchange.load_markets()
    except: pass

    with Live(generate_dashboard(), refresh_per_second=4, screen=True) as live:
        while True:
            state.last_scan_time = datetime.now().strftime("%H:%M:%S")
            try: await process_market(exchange)
            except Exception as e: state.status = f"Error: {e}"
            for i in range(SCAN_INTERVAL_SECONDS, 0, -1):
                state.status = f"Scanning... {i}s"
                live.update(generate_dashboard())
                await asyncio.sleep(1)
    await exchange.close()

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: print("Exiting...")