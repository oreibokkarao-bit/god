import asyncio
import aiohttp
import logging
import json
import time
import statistics
import numpy as np
from collections import deque, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Any, Optional, Set

import ccxt.async_support as ccxt
import websockets
from rich.console import Console
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich import box
from rich.style import Style

# ============================================================
#   🦅🔥 TITAN V4.1: THE SMART LADDER EDITION
#   Features: Liquidity Wall Detection, Volatility TP, Baby Logic
# ============================================================

# --- CONFIGURATION ---
@dataclass
class Config:
    # 1. API (Leave blank for Read-Only Mode)
    mexc_api_key: str = ""
    mexc_api_secret: str = ""

    # 2. Scanning Filters
    min_vol_24h: float = 50_000         # Min Volume (USDT)
    min_change_24h: float = 3.0         # Min % Move
    
    # 3. "Smart Money" Logic
    require_bybit_oi: bool = False      # Set False to trade MEXC micro-caps
    
    # 4. "Spoof Trap" Logic (Safety)
    spoof_check_delay: float = 0.6      # Seconds to wait between book snaps
    max_liq_pull: float = 0.40          # >40% pull = Trap
    
    # 5. Smart Ladder Settings
    tp_sigma_multipliers: List[float] = field(default_factory=lambda: [2.0, 4.0, 8.0])
    wall_frontrun_pct: float = 0.002    # Place TP 0.2% before the wall
    
cfg = Config()

# --- LOGGING & STATE ---
console = Console()
logging.basicConfig(level=logging.CRITICAL) 

class TitanState:
    def __init__(self):
        self.status = "BOOTING TITAN V4.1..."
        self.active_trades: Dict[str, 'ActiveTrade'] = {}
        self.candidates: List[Dict] = []
        self.alerts = deque(maxlen=8)
        self.bybit_data: Dict[str, Dict] = defaultdict(lambda: {'oi': 0, 'cvd': 0})
        self.monitored_symbols: Set[str] = set()

state = TitanState()

# ============================================================
#   LAYER 1: BYBIT DATA STREAM (Background)
# ============================================================
class BybitStream:
    def __init__(self):
        self.uri = "wss://stream.bybit.com/v5/public/linear"
        self.running = False
        self.subs = set()

    async def run(self):
        self.running = True
        while self.running:
            try:
                async with websockets.connect(self.uri) as ws:
                    self.ws = ws
                    state.status = "🟢 CONNECTED: BYBIT SMART FEED"
                    await self.listen()
            except Exception:
                state.status = "⚠️ RECONNECTING BYBIT..."
                await asyncio.sleep(2)

    async def listen(self):
        while self.running:
            try:
                msg = await self.ws.recv()
                data = json.loads(msg)
                if "tickers" in data.get("topic", ""):
                    self.process(data)
                
                # Dynamic Subscription
                needed = {f"tickers.{s.replace('/USDT','USDT')}" for s in state.monitored_symbols}
                new_subs = list(needed - self.subs)
                if new_subs:
                    for i in range(0, len(new_subs), 10):
                        batch = new_subs[i:i+10]
                        await self.ws.send(json.dumps({"op": "subscribe", "args": batch}))
                        self.subs.update(batch)
            except Exception:
                break

    def process(self, data):
        topic = data.get("topic", "")
        sym = topic.split(".")[-1].replace("USDT", "/USDT")
        item = data.get("data", {})
        if "openInterest" in item:
            state.bybit_data[sym]['oi'] = float(item['openInterest'])

# ============================================================
#   LAYER 2: MEXC ENGINE (Liquidity & Volatility)
# ============================================================
class MexcEngine:
    def __init__(self):
        self.exchange = ccxt.mexc({
            'enableRateLimit': True, 
            'options': {'defaultType': 'spot'}
        })

    async def fetch_order_book_analysis(self, symbol, current_price):
        """
        Scans for Liquidity Walls (HVN Proxy).
        Returns the price of the biggest sell wall within 10% range.
        """
        try:
            # Fetch deeper book for analysis
            book = await self.exchange.fetch_order_book(symbol, limit=50)
            asks = book['asks']
            
            if not asks: return None
            
            # Find the "Wall" (Price level with abnormally high volume)
            avg_vol = statistics.mean([x[1] for x in asks])
            max_vol = 0
            wall_price = None
            
            for price, vol in asks:
                if price > current_price * 1.10: break # Only look 10% up
                if vol > max_vol and vol > (avg_vol * 3): # 3x Avg = Wall
                    max_vol = vol
                    wall_price = price
            
            return wall_price
        except Exception:
            return None

    async def check_spoofing(self, symbol):
        """Snapshots book, waits, snaps again."""
        try:
            book_t0 = await self.exchange.fetch_order_book(symbol, limit=20)
            bids_t0 = sum(x[1] for x in book_t0['bids'])
            await asyncio.sleep(cfg.spoof_check_delay)
            book_t1 = await self.exchange.fetch_order_book(symbol, limit=20)
            bids_t1 = sum(x[1] for x in book_t1['bids'])
            
            if bids_t0 <= 0: return True
            pull_pct = (bids_t0 - bids_t1) / bids_t0
            return pull_pct > cfg.max_liq_pull
        except Exception:
            return True

    async def fetch_techs(self, symbol):
        try:
            ohlcv = await self.exchange.fetch_ohlcv(symbol, '5m', limit=50)
            if len(ohlcv) < 30: return None
            
            closes = np.array([x[4] for x in ohlcv])
            vols = np.array([x[5] for x in ohlcv])
            
            # Sigma (Volatility)
            returns = np.diff(closes) / closes[:-1]
            sigma = np.std(returns)
            
            # Z-Score (Volume Spike)
            vol_mean = np.mean(vols[:-5])
            vol_std = np.std(vols[:-5])
            z_score = (vols[-1] - vol_mean) / vol_std if vol_std > 0 else 0
            
            return {'sigma': sigma, 'z_score': z_score, 'price': closes[-1]}
        except Exception:
            return None

# ============================================================
#   LAYER 3: SMART LADDER MANAGER
# ============================================================
class ActiveTrade:
    def __init__(self, symbol, entry, sigma, wall_price):
        self.symbol = symbol
        self.entry = entry
        self.sigma = sigma
        self.peak = entry
        self.status = "ENTRY"
        self.start_time = time.time()
        
        # --- SMART LADDER CONSTRUCTION ---
        # 1. Volatility Targets (Baseline)
        t1 = entry * (1 + (cfg.tp_sigma_multipliers[0] * sigma))
        t2 = entry * (1 + (cfg.tp_sigma_multipliers[1] * sigma))
        t3 = entry * (1 + (cfg.tp_sigma_multipliers[2] * sigma))
        
        # 2. Liquidity Wall Logic
        # If a massive wall exists before T2 or T3, front-run it.
        self.wall_msg = ""
        if wall_price:
            front_run = wall_price * (1 - cfg.wall_frontrun_pct)
            if t1 < front_run < t3:
                t2 = front_run # Adjust TP2 to be just before the wall
                self.wall_msg = " (Wall)"
        
        self.tp1 = t1
        self.tp2 = t2
        self.tp3 = t3
        self.sl = entry * (1 - (2.0 * sigma)) # Initial SL

    def update(self, price):
        self.peak = max(self.peak, price)
        
        # Gear Shift Logic
        if self.status == "ENTRY" and price >= self.tp1:
            self.status = "GEAR 1 (Locked)"
            self.sl = self.entry * 1.002 # Breakeven
            state.alerts.append(f"🛡️ {self.symbol} TP1 Hit. Risk Off.")
            
        elif "GEAR 1" in self.status and price >= self.tp2:
            self.status = "GEAR 2 (Trailing)"
            
        elif "GEAR 2" in self.status and price >= self.tp3:
            self.status = "GEAR 3 (Moonbag)"

        # Dynamic Trailing
        if "GEAR 2" in self.status or "GEAR 3" in self.status:
            mult = 2.0 if "GEAR 2" in self.status else 4.0
            new_sl = self.peak * (1 - (mult * self.sigma))
            self.sl = max(self.sl, new_sl)

        return price > self.sl

# ============================================================
#   NARRATIVE GENERATOR
# ============================================================
def generate_baby_talk(z, sigma, is_spoof, bybit_oi):
    if is_spoof: return "⛔ TRAP! Fake Walls!"
    if bybit_oi == 0 and cfg.require_bybit_oi: return "👻 Ghost Town."
    if z < 0: return "😴 Napping."
    
    reasons = []
    if z > 5.0: reasons.append("🚀 VOLCANIC VOL")
    elif z > 3.0: reasons.append("🔥 High Vol")
    
    if sigma > 0.03: reasons.append("🎢 Scary Moves")
    
    if not reasons: return "👀 Watching..."
    return " + ".join(reasons)

# ============================================================
#   MAIN LOOP
# ============================================================
async def main():
    mexc = MexcEngine()
    bybit = BybitStream()
    asyncio.create_task(bybit.run())

    try:
        with Live(generate_dashboard(), refresh_per_second=4) as live:
            while True:
                # 1. Broad Scan
                tickers = await mexc.exchange.fetch_tickers()
                candidates = []
                for s, t in tickers.items():
                    if "/USDT" not in s: continue
                    if t['quoteVolume'] < cfg.min_vol_24h: continue
                    if t['percentage'] < cfg.min_change_24h: continue
                    candidates.append(s)
                
                candidates.sort(key=lambda x: tickers[x]['percentage'], reverse=True)
                state.monitored_symbols = set(candidates[:20])
                
                processed = []
                
                # 2. Deep Scan (Top 15)
                for sym in candidates[:15]:
                    # A. Checks
                    spoof = await mexc.check_spoofing(sym)
                    techs = await mexc.fetch_techs(sym)
                    if not techs: continue
                    
                    # B. Wall Scan (Expensive, only do if promising)
                    wall = None
                    if techs['z_score'] > 2:
                        wall = await mexc.fetch_order_book_analysis(sym, techs['price'])

                    # C. Narrative
                    bb_oi = state.bybit_data[sym.replace("/USDT", "/USDT")]['oi']
                    narrative = generate_baby_talk(techs['z_score'], techs['sigma'], spoof, bb_oi)
                    
                    # D. Score
                    score = 0
                    if not spoof: score += 40
                    if techs['z_score'] > 2: score += 30
                    if bb_oi > 0: score += 20
                    if tickers[sym]['percentage'] > 5: score += 10
                    
                    processed.append({
                        'sym': sym, 'p': techs['price'], 'z': techs['z_score'], 
                        's': score, 'n': narrative, 'w': wall
                    })

                    # E. Trade
                    if score >= 70 and sym not in state.active_trades:
                        state.active_trades[sym] = ActiveTrade(sym, techs['price'], techs['sigma'], wall)
                        wall_txt = f" | Wall Found: {wall}" if wall else ""
                        state.alerts.append(f"⚔️ ENTER {sym} {wall_txt}")

                state.candidates = sorted(processed, key=lambda x: x['s'], reverse=True)
                
                # 3. Update Trades
                to_del = []
                for s, t in state.active_trades.items():
                    if not t.update(tickers[s]['last']): to_del.append(s)
                for d in to_del: del state.active_trades[d]

                live.update(generate_dashboard())
                await asyncio.sleep(2)

    except KeyboardInterrupt: pass
    finally: await mexc.exchange.close()

# ============================================================
#   HIGH-DENSITY DASHBOARD
# ============================================================
def generate_dashboard() -> Layout:
    layout = Layout()
    layout.split(
        Layout(name="head", size=3),
        Layout(name="body", ratio=2),
        Layout(name="tail", size=8)
    )
    
    # HEADER
    layout["head"].update(Panel(
        f"🦅 TITAN V4.1 | SMART LADDER ENGAGED | TRADES: {len(state.active_trades)} | {state.status}",
        style="bold white on blue"
    ))
    
    # RADAR TABLE
    table = Table(expand=True, box=box.SIMPLE, border_style="dim")
    table.add_column("Sym", style="bold cyan")
    table.add_column("Price", justify="right")
    table.add_column("Score", justify="center")
    table.add_column("Z-Score", justify="right")
    table.add_column("Liquidity/Wall", justify="right")
    table.add_column("Baby Logic 👶", style="italic yellow")
    
    for c in state.candidates:
        s_col = "green" if c['s'] > 70 else "red" if c['s'] < 40 else "yellow"
        z_col = "magenta" if c['z'] > 5 else "white"
        wall_str = f"🧱 {c['w']:.4f}" if c['w'] else "-"
        
        table.add_row(
            c['sym'].replace("/USDT",""), 
            f"{c['p']:.5f}",
            f"[{s_col}]{c['s']}[/]",
            f"[{z_col}]{c['z']:.1f}[/]",
            wall_str,
            c['n']
        )
    layout["body"].update(Panel(table, title="LIVE MARKET RADAR"))
    
    # TRADES TABLE
    t_table = Table(expand=True, box=box.MINIMAL)
    t_table.add_column("Symbol")
    t_table.add_column("Status")
    t_table.add_column("TP1")
    t_table.add_column("TP2 (Smart)")
    t_table.add_column("TP3")
    t_table.add_column("SL")
    
    if state.active_trades:
        for s, t in state.active_trades.items():
            t_table.add_row(
                s.replace("/USDT",""),
                t.status,
                f"{t.tp1:.4f}",
                f"{t.tp2:.4f}{t.wall_msg}",
                f"{t.tp3:.4f}",
                f"[red]{t.sl:.4f}[/]"
            )
    else:
        t_table.add_row("-", "SCANNING...", "-", "-", "-", "-")
        
    layout["tail"].update(Panel(t_table, title="ACTIVE TRADES (SMART LADDER)"))
    
    return layout

if __name__ == "__main__":
    asyncio.run(main())