import asyncio
import ccxt.async_support as ccxt
import pandas as pd
import pandas_ta as ta
import numpy as np
import logging
import warnings
import math
import sqlite3
import time
import os
from datetime import datetime, timedelta
from collections import deque, defaultdict
from typing import Dict, List, Tuple, Optional

# --- UI DESIGN IMPORTS ---
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.console import Console
from rich import box
from rich.text import Text
from rich.align import Align
from rich.style import Style

# SPEED UPGRADES
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    print("🚀 TITAN APEX: UVLOOP ACTIVATED")
except ImportError: pass

try:
    import orjson as json # FASTER JSON PARSER 
    print("⚡ TITAN APEX: ORJSON ACTIVATED")
except ImportError:
    import json
    print("⚠️ TITAN APEX: ORJSON NOT FOUND (USING STANDARD)")

# ==============================================================================
# 🎨 CONFIG & STYLES
# ==============================================================================
warnings.filterwarnings('ignore', message='Polyfit may be poorly conditioned')
warnings.filterwarnings("ignore", category=RuntimeWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

C_NEON_GREEN = "bright_green"
C_NEON_RED = "bright_red"
C_CYAN = "cyan"
C_GOLD = "gold1"
C_WHITE = "white"
C_BG_FLASH_GREEN = "green"
C_BG_FLASH_RED = "red"
C_OBI_POS = "spring_green1"
C_OBI_NEG = "deep_pink2"

CONFIG = {
    "EXCHANGES": {
        "binance": {"enable": True, "type": "future", "limit": 1500},
        "bybit":   {"enable": True, "type": "swap",   "limit": 1000},
        "mexc":    {"enable": True, "type": "swap",   "limit": 1000},
    },
    "MIN_VOLUME_USD": 500_000,    # Microcap Mode
    "MIN_PROFIT_PCT": 0.05,       
    "BLACKLIST": ["USDC/USDT", "BUSD/USDT", "DAI/USDT", "USDT/USD"],
    "TIMEFRAME": "15m",           
    "LOOKBACK": 120,              
    "MIN_WIN_PROB": 0.70,          
    "AMBULANCE_THRESH": 0.80,
    "DATA_DIR": "titan_data_lake",
    "DECOUPLING_RVOL": 5.0,
    
    # NEW APEX CONFIGS
    "MAX_PUMP_PCT": 60.0,         # Veto coins up >60% (LATE STAGE) 
    "CHANDELIER_MULT": 3.0,       # ATR Multiplier for Trailing Stop 
}

logging.basicConfig(level=logging.CRITICAL)
if not os.path.exists(CONFIG['DATA_DIR']): os.makedirs(CONFIG['DATA_DIR'])

# ==============================================================================
# 🔄 LIFECYCLE ENGINE (PUMP STAGES) 
# ==============================================================================
class LifecycleEngine:
    @staticmethod
    def classify_stage(change_24h: float) -> str:
        if change_24h < 5.0: return "COLD"
        if 5.0 <= change_24h <= 20.0: return "EARLY" # Sweet Spot
        if 20.0 < change_24h <= 60.0: return "MID"
        return "LATE" # Danger Zone

# ==============================================================================
# 🧱 SMART WALL ENGINE (EXIT LOGIC) 
# ==============================================================================
class SmartWallEngine:
    @staticmethod
    def find_resistance_wall(book: Dict, current_price: float) -> float:
        """
        Scans order book for the largest 'Ask Wall' to set TP just below it.
        """
        if not book or not book.get('asks'): return current_price * 1.25 # Default
        
        asks = book['asks']
        # Calculate average size to detect outliers
        sizes = [a[1] for a in asks[:20]]
        avg_size = np.mean(sizes) if sizes else 0
        
        # Look for a wall > 5x average size
        for price, qty in asks:
            if qty > (avg_size * 5.0) and price > current_price:
                return price * 0.999 # Front-run the wall 
        
        return current_price * 1.25 # Fallback if no wall found

# ==============================================================================
# 🛡️ ADVERSARIAL VETO
# ==============================================================================
class AdversarialVeto:
    @staticmethod
    def inspect(book: Dict, side: str, rvol: float, stage: str) -> Tuple[bool, str, float]:
        # 1. LATE STAGE VETO 
        if stage == "LATE":
            return True, "LATE_STAGE_RISK", 0.0

        if not book or not book.get('bids') or not book.get('asks'):
            return True, "EMPTY_BOOK", 0.0

        bids = book['bids']
        asks = book['asks']
        
        # 2. DYNAMIC SPREAD CHECK
        max_spread = 0.01 if rvol > 5.0 else 0.002
        best_bid = bids[0][0]
        best_ask = asks[0][0]
        spread = (best_ask - best_bid) / best_bid
        
        if spread > max_spread: 
            return True, f"WIDE_SPREAD_{spread*100:.2f}%", 0.0

        # 3. OBI CALCULATION
        try:
            bid_vol = sum([b[0] * b[1] for b in bids[:5]]) 
            ask_vol = sum([a[0] * a[1] for a in asks[:5]]) 
            total = bid_vol + ask_vol
            obi = (bid_vol - ask_vol) / total if total > 0 else 0
        except: obi = 0

        if side == 'long' and obi < -0.40:
            return True, f"TOXIC_OBI_{obi:.2f}", obi

        return False, "CLEAN", obi

# ==============================================================================
# 🧠 QUANT LOGIC
# ==============================================================================
class QuantAnalysis:
    @staticmethod
    def calculate_half_life(series: pd.Series) -> float:
        try:
            df_lag = series.shift(1)
            df_delta = series - df_lag
            res = np.polyfit(df_lag.dropna(), df_delta.dropna(), 1)
            lam = res[0]
            if lam >= 0: return 999.0 
            return -np.log(2) / lam
        except: return 999.0

    @staticmethod
    def detect_liquidity_voids(df: pd.DataFrame) -> List[float]:
        voids = []
        for i in range(len(df) - 3, len(df) - 1):
            if df['low'].iloc[i] > df['high'].iloc[i-2]:
                voids.append((df['low'].iloc[i] + df['high'].iloc[i-2]) / 2)
            elif df['high'].iloc[i] < df['low'].iloc[i-2]:
                voids.append((df['high'].iloc[i] + df['low'].iloc[i-2]) / 2)
        return voids

    @staticmethod
    def estimate_ofi(df: pd.DataFrame) -> float:
        delta = df['close'].diff()
        signed_flow = np.where(delta > 0, df['volume'], np.where(delta < 0, -df['volume'], 0))
        ofi_score = pd.Series(signed_flow).ewm(span=3).mean().iloc[-1]
        avg_vol = df['volume'].mean()
        if avg_vol == 0: return 0
        return max(-1.0, min(1.0, ofi_score / avg_vol))
    
    @staticmethod
    def check_vwap_ignition(df: pd.DataFrame) -> bool:
        try:
            df['tp'] = (df['high'] + df['low'] + df['close']) / 3
            df['vwap'] = (df['tp'] * df['volume']).cumsum() / df['volume'].cumsum()
            curr = df.iloc[-1]
            return curr['close'] > curr['vwap']
        except: return False

# ==============================================================================
# 🧠 STATS & PROBABILITY
# ==============================================================================
class ZScoreEngine:
    def __init__(self):
        self.history = defaultdict(lambda: deque(maxlen=50))
        self.stats = {} 

    def inject_warmup_data(self, symbol, vol_mean, vol_std):
        self.stats[symbol] = {"mean": vol_mean, "std": vol_std}

    def get_zscore(self, symbol: str, current_val: float) -> float:
        if symbol in self.stats:
            mean = self.stats[symbol]['mean']
            std = self.stats[symbol]['std']
            self.stats[symbol]['mean'] = (mean * 0.99) + (current_val * 0.01)
            if std > 0: return (current_val - mean) / std
        
        self.history[symbol].append(current_val)
        if len(self.history[symbol]) < 2: return 0.0
        arr = np.array(self.history[symbol])
        std = np.std(arr)
        if std == 0: return 0.0
        return (current_val - np.mean(arr)) / std

class ProbabilityEngine:
    @staticmethod
    def calculate(ofi: float, obi: float, rsi: float, vol_z: float, btc_trend: str, stage: str) -> dict:
        logit = 0.0
        
        # 1. STAGE WEIGHTING 
        if stage == "EARLY": logit += 2.0
        elif stage == "MID": logit += 1.0
        elif stage == "LATE": logit -= 2.0
        
        # 2. VOLUME IGNITION
        if vol_z > 5.0: logit += 3.0
        elif vol_z > 3.0: logit += 2.0
        elif vol_z > 1.5: logit += 1.0
        
        logit += (ofi * 2.0)
        logit += (obi * 3.0)
        
        if btc_trend == "BEAR":
            if vol_z < CONFIG['DECOUPLING_RVOL']: logit -= 2.0
            else: logit += 0.5
        
        if rsi > 85: logit -= 1.0
        elif rsi > 50: logit += 0.5
        
        raw_prob = 1 / (1 + math.exp(-logit))
        calibrated = max(0.05, min(0.99, raw_prob))
        return {"prob": calibrated, "pct": f"{calibrated*100:.1f}%"}

class DynamicRiskEngine:
    @staticmethod
    def optimize(entry: float, atr: float, smart_tp: float, side: str):
        # CHANDELIER STOP LOGIC (Initial) 
        sl_dist = atr * CONFIG['CHANDELIER_MULT']
        
        if side == 'long':
            sl_price = entry - sl_dist
            tp_price = smart_tp # Use the Wall-derived target
            tp_type = "🧱 WALL"
        else:
            sl_price = entry + sl_dist
            tp_price = entry * 0.75
            tp_type = "📉 DUMP"

        risk = abs(entry - sl_price)
        reward = abs(entry - tp_price)
        rr = reward / risk if risk > 0 else 0
        gain = (reward / entry)
        return {"sl": sl_price, "tp": tp_price, "type": tp_type, "rr": rr, "gain": gain}

# ==============================================================================
# 📡 DATA CONNECTION
# ==============================================================================
class ExchangeManager:
    def __init__(self):
        self.exchanges = {}
        self.semaphores = {}
        self.btc_trend = "NEUTRAL"

    async def initialize(self):
        for name, cfg in CONFIG['EXCHANGES'].items():
            if cfg['enable']:
                ex = getattr(ccxt, name)({'enableRateLimit': True, 'options': {'defaultType': cfg['type']}})
                self.exchanges[name] = ex
                self.semaphores[name] = asyncio.Semaphore(20)

    async def update_btc_trend(self):
        try:
            ex = self.exchanges.get('binance') or self.exchanges.get('bybit')
            ohlcv = await ex.fetch_ohlcv("BTC/USDT", "1h", limit=20)
            c = [x[4] for x in ohlcv]
            ema20 = pd.Series(c).ewm(span=20).mean().iloc[-1]
            self.btc_trend = "BULL" if c[-1] > ema20 else "BEAR"
        except: self.btc_trend = "NEUTRAL"

    async def fetch_tickers(self, ex_name):
        ex = self.exchanges.get(ex_name)
        if not ex: return []
        try:
            tickers = await ex.fetch_tickers()
            return [
                s for s, t in tickers.items() 
                if t.get('quoteVolume',0) > CONFIG['MIN_VOLUME_USD'] 
                and abs(t.get('percentage', 0)) > 3.0 
                and s not in CONFIG['BLACKLIST'] and "/USDT" in s
            ]
        except: return []

    async def fetch_oi_snapshot(self, ex_name, symbol):
        ex = self.exchanges.get(ex_name)
        try:
            ticker = await ex.fetch_ticker(symbol)
            return float(ticker.get('openInterest', 0) or 0)
        except: return 0.0

    async def fetch_order_book(self, ex_name, symbol):
        ex = self.exchanges.get(ex_name)
        async with self.semaphores[ex_name]:
            try: return await ex.fetch_order_book(symbol, limit=50) # Deepen fetch for Wall detection
            except: return None

    async def fetch_candles(self, ex_name, symbol):
        async with self.semaphores[ex_name]:
            try:
                ohlcv = await self.exchanges[ex_name].fetch_ohlcv(symbol, CONFIG['TIMEFRAME'], limit=CONFIG['LOOKBACK'])
                return pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']) if ohlcv else None
            except: return None

    async def close_all(self):
        for ex in self.exchanges.values(): await ex.close()

# ==============================================================================
# 🔥 HISTORICAL LOADER
# ==============================================================================
class HistoricalLoader:
    @staticmethod
    async def warm_up_symbol(exchange, symbol: str) -> Dict:
        try:
            ohlcv = await exchange.fetch_ohlcv(symbol, CONFIG['TIMEFRAME'], limit=100)
            if not ohlcv: return None
            vols = [x[5] for x in ohlcv]
            return {"vol_mean": np.mean(vols), "vol_std": np.std(vols)}
        except: return None

# ==============================================================================
# 🧠 SQLITE TRACKER (With Panic Sensor)
# ==============================================================================
class TradeTracker:
    def __init__(self):
        self.conn = sqlite3.connect(':memory:') 
        self.cursor = self.conn.cursor()
        self.setup_db()
    
    def setup_db(self):
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS trades (id INTEGER PRIMARY KEY, timestamp TEXT, exch TEXT, symbol TEXT, side TEXT, entry REAL, sl REAL, tp REAL, status TEXT, exit_price REAL, pnl_pct REAL, highest_price REAL)''')
        self.conn.commit()

    def add_trade(self, exch, symbol, side, entry, sl, tp):
        self.cursor.execute("SELECT id FROM trades WHERE symbol=? AND status='OPEN'", (symbol,))
        if self.cursor.fetchone(): return
        self.cursor.execute('''INSERT INTO trades (timestamp, exch, symbol, side, entry, sl, tp, status, exit_price, pnl_pct, highest_price) VALUES (?, ?, ?, ?, ?, ?, ?, 'OPEN', 0, 0, ?)''', (datetime.now().strftime("%H:%M:%S"), exch, symbol, side, entry, sl, tp, entry))
        self.conn.commit()

    def update_price(self, symbol, current_price, atr):
        """
        Implements 'Panic Sensor' / Chandelier Exit 
        """
        self.cursor.execute("SELECT id, side, entry, sl, tp, highest_price FROM trades WHERE symbol=? AND status='OPEN'", (symbol,))
        for row in self.cursor.fetchall():
            tid, side, entry, sl, tp, high = row
            
            # Update Highest Price
            if current_price > high:
                high = current_price
                self.cursor.execute("UPDATE trades SET highest_price=? WHERE id=?", (high, tid))
            
            # CHANDELIER EXIT LOGIC
            # If price drops 3x ATR from High, PANIC SELL
            panic_level = high - (atr * 3.0)
            
            status, exit_price = "OPEN", 0
            if side == 'long':
                if current_price >= tp: status, exit_price = "WIN", tp
                elif current_price <= max(sl, panic_level): # Dynamic Trailing
                    status, exit_price = "LOSS", current_price
            
            if status != "OPEN":
                pnl = (exit_price - entry) / entry 
                self.cursor.execute("UPDATE trades SET status=?, exit_price=?, pnl_pct=? WHERE id=?", (status, exit_price, pnl, tid))
                self.conn.commit()

    def get_history(self):
        self.cursor.execute("SELECT * FROM trades ORDER BY id DESC LIMIT 8")
        return self.cursor.fetchall()

# ==============================================================================
# 🖥️ VISUAL DASHBOARD
# ==============================================================================
class TitanUI:
    def __init__(self, tracker):
        self.layout = Layout()
        self.signals = deque(maxlen=12) 
        self.tracker = tracker
        self.stats = {"scanned": 0, "vetoed": 0, "ignited": 0, "warmed_up": 0}
        self.console = Console()

    def make_layout(self) -> Layout:
        self.layout.split(Layout(name="header", size=3), Layout(name="main", ratio=1), Layout(name="history", size=10), Layout(name="footer", size=3))
        return self.layout

    def generate_table(self) -> Table:
        table = Table(expand=True, box=box.HEAVY_HEAD, header_style="bold white", border_style="bright_blue", show_lines=True)
        table.add_column("TIME", width=8, justify="center")
        table.add_column("EXCH", width=6, justify="center")
        table.add_column("TICKER", justify="left", style="bold")
        table.add_column("STAGE", justify="center") # NEW
        table.add_column("PROB", justify="center")
        table.add_column("VOL-Z", justify="center") 
        table.add_column("GAIN", justify="center")
        table.add_column("ENTRY", justify="right")
        table.add_column("TARGET", justify="right", style="bold yellow")

        flash_on = int(time.time() * 2.5) % 2 == 0 

        for s in self.signals:
            is_ambulance = float(s['prob_raw']) >= CONFIG['AMBULANCE_THRESH']
            
            exch_txt = f"[{C_CYAN}]{s['exch'][:3]}[/]"
            ticker_txt = f"[{C_WHITE}]{s['sym']}[/]"
            
            # Stage Coloring 
            stage_col = "green" if s['stage'] == "EARLY" else "yellow"
            stage_txt = f"[{stage_col}]{s['stage']}[/]"

            prob_txt = f"[{C_GOLD}]{s['prob_pct']}[/]"
            vol_val = float(s['vol_z'])
            vol_txt = f"[bold magenta]{vol_val:.1f}σ[/]" if vol_val > 5 else f"[dim]{vol_val:.1f}σ[/]"
            gain_txt = f"[bold {C_GOLD}]+{float(s['gain_raw'])*100:.0f}%[/]"

            row_style = Style()
            if is_ambulance:
                if flash_on:
                    row_style = Style(bgcolor=C_BG_FLASH_GREEN, color="white", bold=True)
                    ticker_txt = s['sym']
                else:
                    row_style = Style(bgcolor="black")

            table.add_row(
                s['time'], exch_txt, ticker_txt, stage_txt, prob_txt, vol_txt, gain_txt,
                s['entry'], f"{s['tp']} ({s['tp_type']})", style=row_style
            )
        return table

    def generate_history_table(self) -> Table:
        table = Table(title="📜 [bold white]APEX PORTFOLIO LOG[/]", expand=True, box=box.SIMPLE, border_style="grey30")
        table.add_column("TIME", style="dim"); table.add_column("TICKER", style="bold cyan")
        table.add_column("ENTRY"); table.add_column("HIGH", style="dim"); table.add_column("RESULT", justify="center"); table.add_column("PNL %", justify="right")
        rows = self.tracker.get_history()
        for r in rows:
            # r: 0-id, 1-time, 2-exch, 3-sym, 4-side, 5-ent, 6-sl, 7-tp, 8-stat, 9-exP, 10-pnl, 11-high
            res_style, res_txt = "grey50", "OPEN"
            if r[8] == "WIN": res_style, res_txt = "bold green", "✅ WIN"
            elif r[8] == "LOSS": res_style, res_txt = "bold red", "❌ LOSS"
            pnl_txt = f"[{res_style.split()[-1]}]{r[10]*100:.2f}%[/]" if r[10] != 0 else "-"
            table.add_row(r[1], r[3], f"{r[5]:.4f}", f"{r[11]:.4f}", f"[{res_style}]{res_txt}[/]", pnl_txt)
        return table

# ==============================================================================
# 🚀 ORCHESTRATOR
# ==============================================================================
class TitanBot:
    def __init__(self):
        self.manager = ExchangeManager()
        self.tracker = TradeTracker() 
        self.ui = TitanUI(self.tracker)
        self.z_engine = ZScoreEngine()

    async def analyze_symbol(self, ex_name, symbol):
        self.ui.stats['scanned'] += 1
        
        df = await self.manager.fetch_candles(ex_name, symbol)
        if df is None or len(df) < 50: return
        curr_price = df['close'].iloc[-1]
        curr_vol = df['volume'].iloc[-1]
        
        # Calc 24h Change for Stage Classification
        # Approx 96 bars of 15m = 24h
        if len(df) >= 96:
            start_price = df['close'].iloc[-96]
            change_24h = ((curr_price - start_price) / start_price) * 100
        else: change_24h = 0.0

        # 1. STAGE CLASSIFIER 
        stage = LifecycleEngine.classify_stage(change_24h)
        
        # 2. UPDATE TRACKER (PANIC SENSOR)
        atr = ta.atr(df['high'], df['low'], df['close'], length=14).iloc[-1]
        self.tracker.update_price(symbol.split(':')[0], curr_price, atr)

        vol_z = self.z_engine.get_zscore(symbol, curr_vol)
        
        # 3. VWAP IGNITION
        is_ignited = QuantAnalysis.check_vwap_ignition(df)
        if not is_ignited and vol_z < 3.0: return

        df['atr'] = ta.atr(df['high'], df['low'], df['close'], length=14)
        df['rsi'] = ta.rsi(df['close'], length=14)
        half_life = QuantAnalysis.calculate_half_life(df['close'])
        ofi = QuantAnalysis.estimate_ofi(df)
        voids = QuantAnalysis.detect_liquidity_voids(df)

        # 4. VETO LAYER
        book = await self.manager.fetch_order_book(ex_name, symbol)
        temp_side = 'long' if ofi > 0 else 'short'
        vetoed, reason, obi = AdversarialVeto.inspect(book, temp_side, vol_z, stage)
        if vetoed:
            self.ui.stats['vetoed'] += 1
            return

        # 5. PROBABILITY
        prob_data = ProbabilityEngine.calculate(ofi, obi, df['rsi'].iloc[-1], vol_z, self.manager.btc_trend, stage)
        
        if prob_data['prob'] >= CONFIG['MIN_WIN_PROB']:
            # 6. SMART WALL TARGETING 
            smart_tp = SmartWallEngine.find_resistance_wall(book, curr_price)
            
            plan = DynamicRiskEngine.optimize(curr_price, df['atr'].iloc[-1], voids, temp_side)
            plan['tp'] = smart_tp # Override with Wall logic
            plan['gain'] = (smart_tp / curr_price) - 1.0
            plan['type'] = "🧱 WALL"
            
            if plan['gain'] >= CONFIG['MIN_PROFIT_PCT']:
                self.ui.stats['ignited'] += 1
                clean_sym = symbol.split(':')[0]
                
                self.ui.signals.appendleft({
                    "time": datetime.now().strftime("%H:%M"),
                    "exch": ex_name.upper(),
                    "sym": clean_sym,
                    "side": temp_side,
                    "stage": stage,
                    "prob_pct": prob_data['pct'],
                    "prob_raw": prob_data['prob'],
                    "vol_z": f"{vol_z:.1f}",
                    "gain_raw": plan['gain'],
                    "entry": f"{curr_price:.4f}",
                    "sl": f"{plan['sl']:.4f}",
                    "tp": f"{plan['tp']:.4f}",
                    "tp_type": plan['type']
                })

                if prob_data['prob'] >= CONFIG['AMBULANCE_THRESH']:
                    self.tracker.add_trade(ex_name, clean_sym, temp_side, curr_price, plan['sl'], plan['tp'])

    async def warm_up_all(self):
        self.ui.console.log("[bold yellow]WARMING UP Z-SCORE ENGINE...[/]")
        count = 0
        for ex_name, ex in self.manager.exchanges.items():
            try:
                tickers = await self.manager.fetch_tickers(ex_name)
                for s in tickers[:50]: 
                    data = await HistoricalLoader.warm_up_symbol(ex, s)
                    if data:
                        self.z_engine.inject_warmup_data(s, data['vol_mean'], data['vol_std'])
                        count += 1
            except: pass
        self.ui.stats['warmed_up'] = count
        self.ui.console.log(f"[bold green]WARMUP COMPLETE: {count} ASSETS READY[/]")

    async def run(self):
        await self.manager.initialize()
        await self.warm_up_all()
        layout = self.ui.make_layout()
        with Live(layout, refresh_per_second=10, screen=True) as live: 
            while True:
                await self.manager.update_btc_trend()
                tasks = []
                for ex in self.manager.exchanges:
                    tickers = await self.manager.fetch_tickers(ex)
                    for sym in tickers[:300]: tasks.append(self.analyze_symbol(ex, sym))
                chunk = 30
                for i in range(0, len(tasks), chunk):
                    await asyncio.gather(*tasks[i:i+chunk])
                    btc_col = C_NEON_GREEN if self.manager.btc_trend == "BULL" else C_NEON_RED
                    layout["header"].update(Panel(Align.center(f"[bold italic white]TITAN v16 APEX[/] | BTC: [{btc_col}]{self.manager.btc_trend}[/] | MEM: {self.ui.stats['warmed_up']}"), style="white on blue", box=box.HEAVY_EDGE))
                    layout["main"].update(self.ui.generate_table())
                    layout["history"].update(self.ui.generate_history_table())
                    layout["footer"].update(Panel(Align.center(f"[bold cyan]SCANNED: {self.ui.stats['scanned']} | VETOED: {self.ui.stats['vetoed']} | IGNITED: {self.ui.stats['ignited']}[/]"), style="on #111111"))
                    await asyncio.sleep(0.1)

if __name__ == "__main__":
    bot = TitanBot()
    try: asyncio.run(bot.run())
    except KeyboardInterrupt: asyncio.run(bot.manager.close_all())