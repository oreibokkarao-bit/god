import os
import asyncio
import logging
import datetime
import sqlite3
import re
import math
from collections import deque, defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any

# --- AI IMPORTS ---
import wordsegment
from transformers import pipeline
import praw

# -----------------------------------------------------------------------------
# OPTIMIZATIONS & JSON
# -----------------------------------------------------------------------------
try:
    import uvloop
    uvloop.install()
except Exception:
    pass

try:
    import orjson
    def json_loads(s: Any) -> Any: return orjson.loads(s)
    def json_dumps(obj: Any) -> str: return orjson.dumps(obj).decode('utf-8')
except Exception:
    import json
    def json_loads(s: Any) -> Any: return json.loads(s)
    def json_dumps(obj: Any) -> str: return json.dumps(obj)

try:
    import pandas as pd
    HAVE_PANDAS = True
except ImportError:
    HAVE_PANDAS = False
    def simple_zscore(history, current):
        if len(history) < 5: return 0.0
        avg = sum(history) / len(history)
        variance = sum([((x - avg) ** 2) for x in history]) / len(history)
        std = variance ** 0.5
        if std == 0: return 0.0
        return (current - avg) / std

import aiohttp
import websockets
from websockets.exceptions import ConnectionClosed, ConnectionClosedError
import numpy as np
from rich.live import Live
from rich.table import Table
from rich.layout import Layout
from rich import box
from rich.panel import Panel
from rich.text import Text

# -----------------------------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------------------------
DB_NAME = "arsenal_trades.sqlite"

# --- API KEYS (REQUIRED FOR SOCIAL) ---
REDDIT_CLIENT_ID = 'YOUR_ID'
REDDIT_SECRET = 'YOUR_SECRET'
REDDIT_USER_AGENT = 'CryptoBot/1.0'

# Configs
BINANCE_FUTURES_TICKER_WS = "wss://fstream.binance.com/stream?streams=!ticker@arr"
BYBIT_LINEAR_WS = "wss://stream.bybit.com/v5/public/linear" 
BYBIT_SPOT_WS = "wss://stream.bybit.com/v5/public/spot"
BYBIT_LINEAR_INFO = "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000"

# --- AGGRESSIVE TESTING PARAMETERS ---
MIN_CONF_FOR_ENTRY = 20.0       # Lowered to 20 to FORCE trades for testing
DYNAMIC_SUB_THRESHOLD = 5.0     # Subscribe to trade streams very quickly

# Strategy Settings
ATR_PERIOD = 20                 
SL_MULTIPLIER = 2.5             
TP1_MULTIPLIER = 1.5            
TP2_MULTIPLIER = 3.0            
TP3_MULTIPLIER = 6.0  
HIGH_NEG_FUNDING = -0.015

# Social
REDDIT_SUBREDDITS = ["CryptoCurrency", "SatoshiStreetBets", "Solana", "Defi", "AltStreetBets"]

logging.basicConfig(level=logging.ERROR, format="%(asctime)s [%(levelname)s] %(message)s")

# -----------------------------------------------------------------------------
# INTELLIGENCE ENGINE (NLP)
# -----------------------------------------------------------------------------
class IntelligenceEngine:
    def __init__(self):
        try:
            wordsegment.load()
            # CPU-friendly model
            self.classifier = pipeline(
                "text-classification", 
                model="ElKulako/cryptobert", 
                tokenizer="ElKulako/cryptobert",
                top_k=None,
                device=-1 
            )
        except Exception as e:
            logging.error(f"AI Load Failed: {e}")

    def analyze(self, text: str) -> Dict:
        try:
            words = text.split()
            cleaned = []
            for word in words:
                if word.startswith("#"):
                    cleaned.extend(wordsegment.segment(word[1:]))
                else:
                    cleaned.append(word)
            clean_text = " ".join(cleaned)[:512]
            results = self.classifier(clean_text)[0]
            scores = {res['label']: res['score'] for res in results}
            return scores
        except:
            return {}

# -----------------------------------------------------------------------------
# HELPER
# -----------------------------------------------------------------------------
def normalize_to_spot(symbol: str) -> str:
    """Strips 1000 prefix for spot mapping"""
    return re.sub(r'^\d+', '', symbol)

# -----------------------------------------------------------------------------
# DATA MODELS
# -----------------------------------------------------------------------------
@dataclass
class ActiveTrade:
    symbol: str
    entry_price: float
    sl: float
    tp1: float
    tp2: float
    tp3: float
    funding_score_at_entry: float
    side: str = "LONG"
    start_time: float = 0.0
    tp1_hit: bool = False
    tp2_hit: bool = False
    status: str = "OPEN" 

@dataclass
class MarketState:
    symbol: str
    
    # Price & Data
    price_history: deque = field(default_factory=lambda: deque(maxlen=50))
    current_price: float = 0.0
    vwap: float = 0.0
    volatility_atr: float = 0.0
    oi_history: deque = field(default_factory=lambda: deque(maxlen=50))
    current_oi: float = 0.0
    oi_change_pct: float = 0.0
    funding_rate: float = 0.0
    funding_history: deque = field(default_factory=lambda: deque(maxlen=50))
    funding_z_score: float = 0.0
    cvd_cumulative: float = 0.0
    cvd_history: deque = field(default_factory=lambda: deque(maxlen=50))
    spot_cvd_cumulative: float = 0.0
    spot_cvd_history: deque = field(default_factory=lambda: deque(maxlen=50))
    
    # Social
    social_volume: int = 0
    unique_authors: set = field(default_factory=set) 
    spam_ratio: float = 0.0
    sentiment_score: float = 0.0 
    
    # Analysis
    active_tags: List[str] = field(default_factory=list)
    confidence: float = 0.0
    funding_score: float = 0.0
    active_trade: Optional[ActiveTrade] = None

    def update_price(self, price: float):
        if price <= 0: return
        self.current_price = price
        self.price_history.append(price)
        if len(self.price_history) > 5:
            self.vwap = float(np.mean(self.price_history))
        if len(self.price_history) > 10:
            self.volatility_atr = float(np.std(self.price_history))

    def update_oi(self, oi: float):
        if oi <= 0: return
        self.current_oi = oi
        self.oi_history.append(oi)
        if len(self.oi_history) > 10:
            prev = self.oi_history[-10]
            if prev > 0:
                self.oi_change_pct = ((oi - prev) / prev) * 100.0

    def update_funding(self, rate: float):
        self.funding_rate = rate
        self.funding_history.append(rate)
        if HAVE_PANDAS and len(self.funding_history) > 10:
            s = pd.Series(list(self.funding_history))
            rolling_mean = s.mean()
            rolling_std = s.std()
            if rolling_std > 0:
                self.funding_z_score = (rate - rolling_mean) / rolling_std
            else:
                self.funding_z_score = 0.0
        elif not HAVE_PANDAS:
            self.funding_z_score = simple_zscore(list(self.funding_history), rate)

    def update_perp_cvd(self, side: str, qty: float):
        if side == "Buy": self.cvd_cumulative += qty
        elif side == "Sell": self.cvd_cumulative -= qty
        self.cvd_history.append(self.cvd_cumulative)

    def update_spot_cvd(self, side: str, qty: float):
        if side == "Buy": self.spot_cvd_cumulative += qty
        elif side == "Sell": self.spot_cvd_cumulative -= qty
        self.spot_cvd_history.append(self.spot_cvd_cumulative)

    def update_social(self, author: str, sentiment: Dict):
        self.social_volume += 1
        self.unique_authors.add(author)
        authors_count = len(self.unique_authors)
        if authors_count > 0:
            self.spam_ratio = self.social_volume / authors_count
        score = 0
        if sentiment.get('Bullish', 0) > 0.7: score = 1
        elif sentiment.get('Bearish', 0) > 0.7: score = -1
        self.sentiment_score = (self.sentiment_score * 0.9) + (score * 0.1)

market_data: Dict[str, MarketState] = {}

# -----------------------------------------------------------------------------
# WORKERS
# -----------------------------------------------------------------------------
async def reddit_worker_smart(nlp_engine: IntelligenceEngine):
    if REDDIT_CLIENT_ID == 'YOUR_ID':
        return
    try:
        reddit = praw.Reddit(client_id=REDDIT_CLIENT_ID, client_secret=REDDIT_SECRET, user_agent=REDDIT_USER_AGENT)
        subreddit = reddit.subreddit("CryptoCurrency+Solana+Bitcoin+Ethereum+SatoshiStreetBets")
        loop = asyncio.get_event_loop()
        def stream_comments():
            for comment in subreddit.stream.comments(skip_existing=True):
                try:
                    text = comment.body
                    author = comment.author.name if comment.author else "unknown"
                    coin = None
                    if "btc" in text.lower(): coin = "BTCUSDT"
                    elif "sol" in text.lower(): coin = "SOLUSDT"
                    elif "eth" in text.lower(): coin = "ETHUSDT"
                    elif "bnb" in text.lower(): coin = "BNBUSDT"
                    elif "doge" in text.lower(): coin = "DOGEUSDT"
                    if coin and coin in market_data:
                        sentiment = nlp_engine.analyze(text)
                        market_data[coin].update_social(author, sentiment)
                except: continue
        await loop.run_in_executor(None, stream_comments)
    except Exception:
        pass

async def bybit_linear_manager():
    """Futures Data with Aggressive Subscribing"""
    subscribed_trades = set()
    while True:
        try:
            symbols = []
            async with aiohttp.ClientSession() as session:
                async with session.get(BYBIT_LINEAR_INFO) as resp:
                    if resp.status == 200:
                        res = await resp.json()
                        symbols = [i['symbol'] for i in res['result']['list'] if i['quoteCoin'] == 'USDT' and i['status'] == 'Trading']
            if not symbols:
                await asyncio.sleep(5)
                continue
            for s in symbols: 
                if s not in market_data: market_data[s] = MarketState(symbol=s)
            
            async with websockets.connect(BYBIT_LINEAR_WS, ping_interval=20, ping_timeout=20) as ws:
                # Tickers first
                for i in range(0, len(symbols), 10):
                    args = [f"tickers.{s}" for s in symbols[i:i+10]]
                    await ws.send(json_dumps({"op": "subscribe", "args": args}))
                    await asyncio.sleep(0.02)
                
                while True:
                    # AGGRESSIVE DYNAMIC SUB
                    hot_coins = [s for s, m in market_data.items() if m.confidence > DYNAMIC_SUB_THRESHOLD and s not in subscribed_trades]
                    if hot_coins:
                        for s in hot_coins[:10]:
                            await ws.send(json_dumps({"op": "subscribe", "args": [f"publicTrade.{s}"]}))
                            subscribed_trades.add(s)
                            await asyncio.sleep(0.02)

                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    except asyncio.TimeoutError:
                        await ws.send(json_dumps({"op": "ping"}))
                        continue

                    data = json_loads(msg)
                    if "topic" not in data: continue
                    topic = data["topic"]
                    payload = data["data"]

                    if topic.startswith("tickers."):
                        sym = topic.split(".")[1]
                        if "openInterest" in payload: market_data[sym].update_oi(float(payload["openInterest"]))
                        if "fundingRate" in payload: market_data[sym].update_funding(float(payload["fundingRate"]))
                    elif topic.startswith("publicTrade."):
                        sym = topic.split(".")[1]
                        for t in payload: market_data[sym].update_perp_cvd(t["S"], float(t["v"]))
        except:
            subscribed_trades.clear()
            await asyncio.sleep(5)

async def bybit_spot_manager():
    """Spot Data"""
    subscribed_spot = set()
    while True:
        try:
            async with websockets.connect(BYBIT_SPOT_WS, ping_interval=20, ping_timeout=20) as ws:
                while True:
                    hot_coins = [s for s, m in market_data.items() if m.confidence > DYNAMIC_SUB_THRESHOLD]
                    for sym_perp in hot_coins:
                        if sym_perp not in subscribed_spot:
                            sym_spot = normalize_to_spot(sym_perp)
                            await ws.send(json_dumps({"op": "subscribe", "args": [f"publicTrade.{sym_spot}"]}))
                            subscribed_spot.add(sym_perp)
                            await asyncio.sleep(0.02)
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    except asyncio.TimeoutError:
                        await ws.send(json_dumps({"op": "ping"}))
                        continue
                    data = json_loads(msg)
                    if "topic" not in data: continue
                    topic = data["topic"]
                    if topic.startswith("publicTrade."):
                        sym_spot = topic.split(".")[1]
                        for k in market_data:
                            if normalize_to_spot(k) == sym_spot:
                                for t in data["data"]: market_data[k].update_spot_cvd(t["S"], float(t["v"]))
                                break
        except:
            subscribed_spot.clear()
            await asyncio.sleep(5)

async def binance_ticker():
    while True:
        try:
            async with websockets.connect(BINANCE_FUTURES_TICKER_WS, ping_interval=20, ping_timeout=20) as ws:
                while True:
                    msg = await ws.recv()
                    data = json_loads(msg)
                    for item in data.get("data", []):
                        s = item["s"]
                        if s in market_data: market_data[s].update_price(float(item["c"]))
        except:
            await asyncio.sleep(5)

# -----------------------------------------------------------------------------
# STRATEGY
# -----------------------------------------------------------------------------
def calculate_funding_score(st: MarketState) -> float:
    score = 0.0
    if len(st.funding_history) < 5: return 0.0
    curr_rate = st.funding_rate
    prev_rate = st.funding_history[-5] 
    if curr_rate <= HIGH_NEG_FUNDING:
        score += 3.0
        if "bear_trap" not in st.active_tags: st.active_tags.append("bear_trap")
    if prev_rate > 0.005 and curr_rate < 0:
        score += 3.0
        if "fund_flip" not in st.active_tags: st.active_tags.append("fund_flip")
    if st.funding_z_score < -2.0:
        score += 4.0
        if "z_score_low" not in st.active_tags: st.active_tags.append("z_score_low")
    return score

def check_for_entry(st: MarketState) -> bool:
    if len(st.price_history) < 20: return False
    if st.active_trade is not None: return False

    tags = []
    score = 0

    if st.current_price > st.vwap: score += 10
    if st.oi_change_pct > 2.0:
        tags.append("oi_shock")
        score += 30

    if len(st.cvd_history) > 5:
        perp_slope = st.cvd_history[-1] - st.cvd_history[-5]
        spot_slope = 0
        if len(st.spot_cvd_history) > 5:
            spot_slope = st.spot_cvd_history[-1] - st.spot_cvd_history[-5]

        if perp_slope > 0 and spot_slope > 0:
            score += 15
            tags.append("✅Real")
        elif perp_slope < 0 and spot_slope > 0:
            score += 25
            tags.append("🐋Absorb")
        elif perp_slope > 0 and spot_slope < 0:
            score -= 100 
            tags.append("💀Trap")
        elif perp_slope > 0 and st.current_price > st.vwap:
            score += 15 

    funding_pts = calculate_funding_score(st)
    if funding_pts > 0: score += (funding_pts * 2) 

    if st.social_volume > 0:
        if st.spam_ratio < 3.0:
            if st.sentiment_score > 0.2:
                score += 20
                tags.append("🦜Hype")
        else:
            score -= 10 
            tags.append("🤖Spam")

    st.funding_score = funding_pts
    st.active_tags = list(set(st.active_tags + tags)) 
    st.confidence = score

    return score >= MIN_CONF_FOR_ENTRY

def manage_active_trade(st: MarketState):
    t = st.active_trade
    if not t: return
    curr = st.current_price
    pnl_pct = ((curr - t.entry_price) / t.entry_price) * 100
    
    if curr <= t.sl:
        log_trade_outcome(t.symbol, t.side, t.entry_price, t.sl, "STOP_LOSS 🛑", -abs(pnl_pct), ",".join(st.active_tags), t.funding_score_at_entry)
        st.active_trade = None
        return
    if not t.tp1_hit and curr >= t.tp1:
        t.tp1_hit = True
        t.status = "TP1 HIT 💰"
        t.sl = t.entry_price * 1.001 
    if not t.tp2_hit and curr >= t.tp2:
        t.tp2_hit = True
        t.status = "TP2 HIT 💰💰"
        t.sl = t.tp1 
    if curr >= t.tp3:
        log_trade_outcome(t.symbol, t.side, t.entry_price, t.tp3, "TP3 MOON 🚀", pnl_pct, ",".join(st.active_tags), t.funding_score_at_entry)
        st.active_trade = None
        return

async def engine_loop():
    while True:
        try:
            await asyncio.sleep(1)
            for sym, st in market_data.items():
                if st.active_trade:
                    manage_active_trade(st)
                elif check_for_entry(st):
                    vol = st.volatility_atr if st.volatility_atr > 0 else (st.current_price * 0.01)
                    entry = st.current_price
                    sl = entry - (vol * SL_MULTIPLIER)
                    tp1 = entry + (vol * TP1_MULTIPLIER)
                    tp2 = entry + (vol * TP2_MULTIPLIER)
                    tp3 = entry + (vol * TP3_MULTIPLIER)
                    st.active_trade = ActiveTrade(
                        symbol=sym, entry_price=entry, sl=sl, 
                        tp1=tp1, tp2=tp2, tp3=tp3, start_time=datetime.datetime.utcnow().timestamp(),
                        funding_score_at_entry=st.funding_score
                    )
        except Exception:
            pass

# -----------------------------------------------------------------------------
# DB & UI
# -----------------------------------------------------------------------------
def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS trade_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            symbol TEXT,
            side TEXT,
            entry_price REAL,
            exit_price REAL,
            outcome TEXT,
            pnl_percent REAL,
            tags TEXT,
            funding_score REAL
        )
    """)
    conn.commit()
    conn.close()

def log_trade_outcome(symbol, side, entry, exit_p, outcome, pnl, tags, f_score):
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO trade_history (timestamp, symbol, side, entry_price, exit_price, outcome, pnl_percent, tags, funding_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (datetime.datetime.utcnow().isoformat(), symbol, side, entry, exit_p, outcome, pnl, tags, f_score))
        conn.commit()
        conn.close()
    except Exception:
        pass

def fetch_trade_history(limit=10):
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("SELECT timestamp, symbol, side, entry_price, exit_price, outcome, pnl_percent, funding_score FROM trade_history ORDER BY id DESC LIMIT ?", (limit,))
        rows = cursor.fetchall()
        conn.close()
        return rows
    except Exception:
        return []

def generate_dashboard() -> Layout:
    layout = Layout()
    layout.split(Layout(name="header", size=3), Layout(name="upper", ratio=1), Layout(name="lower", ratio=1))
    layout["upper"].split_row(Layout(name="scanner", ratio=1), Layout(name="active_trades", ratio=2))
    layout["lower"].update(Panel(generate_history_table(), title="📜 ARSENAL LEDGER (SQLite)"))
    layout["header"].update(Panel(Text("🚀 ORCHESTRATOR: ARSENAL INSTANT 🚀", justify="center", style="bold white on blue")))
    layout["scanner"].update(Panel(generate_scanner_table(), title="📡 SCANNER (Active)"))
    layout["active_trades"].update(Panel(generate_active_table(), title="⚡ ACTIVE TRADES"))
    return layout

def generate_scanner_table() -> Table:
    table = Table(expand=True, box=box.HORIZONTALS, show_lines=True, title_style="bold cyan", header_style="bold yellow")
    table.add_column("Sym", style="cyan bold", width=12)
    table.add_column("Conf", justify="right", style="bold white", width=6)
    table.add_column("⚡ OI (Raw)", justify="center", width=10)     
    table.add_column("🌊 CVD (Count)", justify="center", width=10)    
    table.add_column("🏦 Spot", justify="center", width=6) 
    table.add_column("🦜 Soc", justify="center", width=6)    
    table.add_column("💸 Fund", justify="center", width=10)   

    raw_candidates = [s for s in market_data.values() if not s.active_trade and s.confidence > 10]
    candidates = sorted(raw_candidates, key=lambda x: (x.confidence, x.funding_score), reverse=True)[:25]

    for s in candidates:
        if s.confidence >= MIN_CONF_FOR_ENTRY: conf_str = f"[bold green on black]{s.confidence:.0f}[/]"
        elif s.confidence >= 15: conf_str = f"[yellow]{s.confidence:.0f}[/]"
        else: conf_str = f"[dim]{s.confidence:.0f}[/]"

        # DEBUG OI VIEW
        if s.current_oi > 0:
            val = s.current_oi
            if val > 1_000_000: val_str = f"{val/1_000_000:.1f}M"
            elif val > 1_000: val_str = f"{val/1_000:.1f}K"
            else: val_str = f"{val:.0f}"
            if abs(s.oi_change_pct) > 0:
                c = "green" if s.oi_change_pct > 0 else "red"
                oi_cell = f"{val_str} ([{c}]{s.oi_change_pct:.2f}%[/])"
            else: oi_cell = f"[dim]{val_str}[/]"
        else: oi_cell = "[red]No Data[/]"

        # DEBUG CVD VIEW
        history_len = len(s.cvd_history)
        if history_len > 0:
            val = s.cvd_cumulative
            c = "green" if val > 0 else "red"
            cvd_cell = f"[{c}]{val:.0f}[/] [dim](n={history_len})[/]"
        else: cvd_cell = "[yellow]Subscribing..[/]"
        
        if len(s.spot_cvd_history) > 0:
             val = s.spot_cvd_cumulative
             c = "green" if val > 0 else "red"
             spot_cell = f"[{c}]{val:.0f}[/]"
        else: spot_cell = "[dim]..[/]"

        soc_cell = f"[green]Hype[/]" if "🦜Hype" in s.active_tags else "[dim]-[/]"
        fund_points = s.funding_score * 2
        fund_cell = f"[green]+{fund_points:.0f}[/] [dim]({s.funding_z_score:.1f}σ)[/]" if fund_points > 0 else "[dim]-[/]"

        table.add_row(s.symbol, conf_str, oi_cell, cvd_cell, spot_cell, soc_cell, fund_cell)
    return table

def generate_active_table() -> Table:
    table = Table(expand=True, box=box.SIMPLE_HEAD)
    table.add_column("Symbol", style="bold white")
    table.add_column("PnL", justify="right")
    table.add_column("Entry")
    table.add_column("🛑 SL", style="red")
    table.add_column("🎯 TP1", style="cyan")
    table.add_column("🎯 TP2", style="blue")
    table.add_column("🎯 TP3", style="magenta")
    table.add_column("Fund", justify="right")
    table.add_column("Status")

    active = [s.active_trade for s in market_data.values() if s.active_trade]
    for t in active:
        curr = market_data[t.symbol].current_price
        pnl = ((curr - t.entry_price) / t.entry_price) * 100
        pnl_str = f"[green]+{pnl:.2f}%[/]" if pnl > 0 else f"[red]{pnl:.2f}%[/]"
        status_emoji = "🟢 Running"
        if "TP1" in t.status: status_emoji = "💰 TP1"
        if "TP2" in t.status: status_emoji = "💰💰 TP2"

        table.add_row(f"{t.symbol}", pnl_str, f"{t.entry_price:.4f}", f"{t.sl:.4f}", f"{t.tp1:.4f}", f"{t.tp2:.4f}", f"{t.tp3:.4f}", f"{t.funding_score_at_entry:.1f}", status_emoji)
    return table

def generate_history_table() -> Table:
    table = Table(expand=True, box=box.SIMPLE_HEAD)
    table.add_column("Time", style="dim")
    table.add_column("Symbol")
    table.add_column("Outcome", style="bold")
    table.add_column("Funding")
    rows = fetch_trade_history(10)
    for r in rows:
        ts = r[0].split("T")[1][:8]
        outcome_style = "green" if "TP" in r[5] else "red"
        fund_sc = r[7] if len(r) > 7 else 0.0
        table.add_row(ts, r[1], f"[{outcome_style}]{r[5]}[/]", f"{fund_sc:.1f}")
    return table

async def ui_loop(live: Live):
    while True:
        try:
            live.update(generate_dashboard())
            await asyncio.sleep(0.5)
        except Exception:
            await asyncio.sleep(1)

async def main():
    init_db()
    nlp = IntelligenceEngine()
    asyncio.create_task(binance_ticker())
    asyncio.create_task(bybit_linear_manager())
    asyncio.create_task(bybit_spot_manager())
    asyncio.create_task(reddit_worker_smart(nlp))
    asyncio.create_task(engine_loop())
    with Live(generate_dashboard(), refresh_per_second=4, screen=True) as live:
        await ui_loop(live)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Shutting down...")