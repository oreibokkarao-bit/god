#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
All_archetypes_patched.py

Master single-file build (production-ready, opinionated) — Options:
- Market: USDT-M Futures (Binance fapi)
- WS Mode: High-parallel (many combined streams; batched)
- TP/SL Mode: TP4 Hyper-scaler (continuous TP generation based on ATR + HVNs)

Features:
- Full futures discovery (REST)
- Combined websockets for trade + depth (batched, high parallel)
- L2 orderbook snapshots (depth REST + depth websocket deltas)
- Open Interest polling (REST)
- FeatureEngine computing real features: oi_slope_z, cvd_slope_z, vpvr_gap_z, bbw, atr, spread compression, taker_buy_share
- ScoringEngine with ALL archetypes active (lsk, melania, vpvr, micro, ffp, aura, godcandle, bake, zec)
- SQLite logging: triggered_trades, followups, planned_orders, planned_fills
- TP4 Hyper-scaler engine: continuously generates new TP levels as price runs (ATR+HVN based)
- Robust safety: AsyncLimiter, exponential backoff with jitter, pybreaker circuit breaker
- Proper asyncio lifecycle: clean shutdown, cancelled tasks, aiohttp session closed
- Rich.Live single-table UI with sparklines (NumPy 2.x compatible)
"""

from __future__ import annotations
import asyncio
import aiohttp
import logging
import math
import time
import random
import sqlite3
import json
from dataclasses import dataclass, field
from collections import deque, defaultdict
from typing import Any, Dict, Deque, List, Optional, Tuple
from aiolimiter import AsyncLimiter
import pybreaker
import numpy as np
import pandas as pd
from scipy.stats import zscore
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich import box
# === Human-readable archetype names mapping ===
ARCHETYPE_NAMES = {
    "zec": "Volume-momentum breakout",
    "mel": "Volatility squeeze & breakout",
    "fil": "Trend continuation on renewed liquidity",
    "moca": "Mean-reversion to mid/median",
    "cookie": "Fast micro-momentum",
    "anime": "Social/pump breakout",
    "turbo": "High-beta momentum swing",
    "broccoli": "Range breakout with volume confirmation",
    "sky": "Low-float breakout",
    "pippin": "Trend pullback continuation",
    "2z": "Volatility-normalized breakout",
    "xvg": "Reversal/pivot structure",
    "bico": "Consolidation continuation breakout",
    "stx": "Strength composite breakout",
    "rose": "Breakout + retest confirmation",
    "neiro": "Liquidity-sweep reversal",
    "prom": "High-volume validated breakout",
    "mon": "Microcap momentum",
    "arpa": "Orderbook imbalance signal",
    "sapiens": "Statistical anomaly breakout"
}

def human_archetype(code: str) -> str:
    try:
        return ARCHETYPE_NAMES.get(code, code)
    except Exception:
        return code
# === end mapping ===



# ---------------------------
# Logging
# ---------------------------
LOG_FMT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FMT)
logger = logging.getLogger("parallel_ai_master")

# ---------------------------
# Configuration (USDT-M, WS1, TP4 hyper-scaler)
# ---------------------------
class Config:
    # Market / endpoints
    FUTURES_REST = "https://fapi.binance.com"
    FUTURES_WS_BASE = "wss://fstream.binance.com/stream?streams="

    # Discovery / tracking
    TRACK_ALL_FUTURES = True
    TRACK_TOP_N_MANUAL = 0
    SYMBOL_DISCOVERY_INTERVAL_SECONDS = 60 * 30

    # WS batching - High-parallel mode (WS1)
    TRADE_BATCH_SIZE = 80
    DEPTH_BATCH_SIZE = 40
    MAX_CONCURRENT_WS = 12  # High parallelism, but capped

    # Rate limiting & safety
    REST_MAX_CALLS = 200
    REST_PERIOD_SECONDS = 60
    FOLLOWUP_MAX_CALLS = 60
    FOLLOWUP_PERIOD_SECONDS = 60
    CIRCUIT_FAIL_MAX = 8
    CIRCUIT_RESET_TIMEOUT = 300

    # Data windows
    DATA_WINDOW_SIZES = {
        "trades": 20000,
        "klines_1m": 1440,
        "spread": 1000,
        "oi": 1000,
        "score_history": 120
    }

    # Scoring
    FEATURE_CALC_INTERVAL = 4
    UI_REFRESH_RATE_SECONDS = 2
    MAX_CANDIDATES_IN_UI = 40
    ALERT_PUMP_SCORE_THRESHOLD = 80
    ALERT_COOLDOWN_SECONDS = 30 * 60  # 30 minutes

    # Followups
    FOLLOWUP_CHECKS = [5, 15, 60]
    FOLLOWUP_SUCCESS_PCT = 0.02
    FOLLOWUP_FAIL_PCT = -0.02

    # TP/SL Hyper-scaler
    TP_HVNS_TOP_N = 5
    TP_GENERATION_MIN_DISTANCE = 0.002  # 0.2% separation between TPs
    TP_MAX_ACTIVE = 8
    TP_GENERATION_INTERVAL = 5.0  # seconds update hyper-scaler

    # Misc
    USER_AGENT = "ParallelAI-Master/1.0"
    DB_PATH = "parallel_ai_master.sqlite"

# ---------------------------
# Archetype thresholds
# ---------------------------
class ArchetypeThresholds:
    LSK_FUNDING_DEEPLY_NEGATIVE = -0.004
    LSK_OI_SLOPE_ZSCORE = 2.5
    LSK_CVD_SLOPE_ZSCORE = 1.5
    MELANIA_SPOT_VOLUME_ZSCORE = 4.0
    MELANIA_SOCIAL_SCORE_THRESHOLD = 70
    VPVR_GAP_ZSCORE = 3.5
    MICROCAP_TAKER_BUY_SHARE = 0.7
    PUMP_SCORE_FOMO_PENALTY_THRESHOLD_PERCENT = 0.15

# ---------------------------
# Data models
# ---------------------------
@dataclass
class Trade:
    timestamp: int
    price: float
    quantity: float
    is_buyer_maker: bool

@dataclass
class OrderbookSnapshot:
    bids: Dict[float, float] = field(default_factory=dict)
    asks: Dict[float, float] = field(default_factory=dict)

@dataclass
class SymbolState:
    symbol: str
    trades: Deque[Trade] = field(default_factory=lambda: deque(maxlen=Config.DATA_WINDOW_SIZES["trades"]))
    klines_1m: Deque[Dict[str, Any]] = field(default_factory=lambda: deque(maxlen=Config.DATA_WINDOW_SIZES["klines_1m"]))
    orderbook: OrderbookSnapshot = field(default_factory=OrderbookSnapshot)
    spread_history: Deque[float] = field(default_factory=lambda: deque(maxlen=Config.DATA_WINDOW_SIZES["spread"]))
    oi_history: Deque[Tuple[int, float]] = field(default_factory=lambda: deque(maxlen=Config.DATA_WINDOW_SIZES["oi"]))
    features: Dict[str, Any] = field(default_factory=dict)
    sub_scores: Dict[str, float] = field(default_factory=dict)
    pump_score: float = 0.0
    last_alert_timestamp: float = 0.0

# ---------------------------
# DB initialization & helpers
# ---------------------------
def init_db():
    conn = sqlite3.connect(Config.DB_PATH, timeout=30)
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS triggered_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        score REAL NOT NULL,
        top_archetype TEXT,
        price REAL,
        features_json TEXT
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS followups (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        triggered_id INTEGER NOT NULL,
        ts INTEGER NOT NULL,
        price REAL,
        pct_change REAL,
        outcome TEXT,
        FOREIGN KEY(triggered_id) REFERENCES triggered_trades(id)
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS planned_orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        triggered_id INTEGER NOT NULL,
        ts INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        side TEXT NOT NULL,
        price REAL,
        size_pct REAL,
        type TEXT,
        status TEXT DEFAULT 'open',
        fill_ts INTEGER,
        fill_price REAL
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS planned_fills (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id INTEGER NOT NULL,
        ts INTEGER NOT NULL,
        price REAL,
        pct_change REAL,
        note TEXT
    )""")
    conn.commit()
    conn.close()

def log_triggered_trade(ts:int, symbol:str, score:float, top_archetype:str, price:float, features_json:str) -> int:
    conn = sqlite3.connect(Config.DB_PATH, timeout=30)
    c = conn.cursor()
    c.execute("INSERT INTO triggered_trades (ts, symbol, score, top_archetype, price, features_json) VALUES (?, ?, ?, ?, ?, ?)",
              (ts, symbol, score, top_archetype, price, features_json))
    tid = c.lastrowid
    conn.commit()
    conn.close()
    return tid

def log_followup(triggered_id:int, ts:int, price:float, pct_change:float, outcome:Optional[str]):
    conn = sqlite3.connect(Config.DB_PATH, timeout=30)
    c = conn.cursor()
    c.execute("INSERT INTO followups (triggered_id, ts, price, pct_change, outcome) VALUES (?, ?, ?, ?, ?)",
              (triggered_id, ts, price, pct_change, outcome))
    conn.commit()
    conn.close()

def create_planned_order(triggered_id:int, symbol:str, side:str, price:Optional[float], size_pct:float, type_str:str) -> int:
    conn = sqlite3.connect(Config.DB_PATH, timeout=30)
    c = conn.cursor()
    ts = int(time.time()*1000)
    c.execute("INSERT INTO planned_orders (triggered_id, ts, symbol, side, price, size_pct, type, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
              (triggered_id, ts, symbol, side, price, size_pct, type_str, 'open'))
    oid = c.lastrowid
    conn.commit()
    conn.close()
    return oid

def mark_order_filled(order_id:int, fill_price:float):
    conn = sqlite3.connect(Config.DB_PATH, timeout=30)
    c = conn.cursor()
    ts = int(time.time()*1000)
    c.execute("UPDATE planned_orders SET status='filled', fill_ts=?, fill_price=? WHERE id=?", (ts, fill_price, order_id))
    conn.commit()
    conn.close()

def record_planned_fill(order_id:int, price:float, pct_change:float, note:Optional[str]=None):
    conn = sqlite3.connect(Config.DB_PATH, timeout=30)
    c = conn.cursor()
    ts = int(time.time()*1000)
    c.execute("INSERT INTO planned_fills (order_id, ts, price, pct_change, note) VALUES (?, ?, ?, ?, ?)",
              (order_id, ts, price, pct_change, note))
    conn.commit()
    conn.close()

# ---------------------------
# Sparkline (NumPy 2.x compatible)
# ---------------------------
_SPARK_CHARS = "▁▂▃▄▅▆▇█"
def sparkline(values:List[float], length:int=8) -> str:
    if not values:
        return " " * length
    arr = np.array(values[-length:])
    rng = float(np.ptp(arr))
    if rng == 0 or math.isnan(rng):
        return _SPARK_CHARS[0] * len(arr)
    norm = (arr - arr.min()) / (rng + 1e-9)
    idxs = (norm * (len(_SPARK_CHARS)-1)).astype(int)
    return "".join(_SPARK_CHARS[i] for i in idxs)

# ---------------------------
# Binance Connector (USDT-M futures)
# ---------------------------
class BinanceFuturesConnector:
    def __init__(self, session:aiohttp.ClientSession, states:Dict[str, SymbolState]):
        self.session = session
        self.states = states
        self._limiter = AsyncLimiter(Config.REST_MAX_CALLS, Config.REST_PERIOD_SECONDS)
        self._followup_limiter = AsyncLimiter(Config.FOLLOWUP_MAX_CALLS, Config.FOLLOWUP_PERIOD_SECONDS)
        self._cb = pybreaker.CircuitBreaker(fail_max=Config.CIRCUIT_FAIL_MAX, reset_timeout=Config.CIRCUIT_RESET_TIMEOUT)
        self._running = False
        self.tracked_symbols: List[str] = []
        self._trade_tasks: List[asyncio.Task] = []
        self._depth_tasks: List[asyncio.Task] = []
        self._oi_task: Optional[asyncio.Task] = None

    async def _safe_get(self, url:str, params:dict=None, max_attempts:int=5):
        last_exc = None
        for attempt in range(1, max_attempts+1):
            try:
                async with self._limiter:
                    headers = {"User-Agent": Config.USER_AGENT}
                    async with self.session.get(url, params=params, headers=headers, timeout=15) as resp:
                        text = await resp.text()
                        if resp.status == 429 or resp.status >= 500:
                            raise RuntimeError(f"HTTP {resp.status} {text[:160]}")
                        return await resp.json()
            except Exception as e:
                last_exc = e
                backoff = (2 ** (attempt-1)) + random.random()*0.5
                logger.warning("GET %s attempt %d failed: %s; backing off %.2fs", url, attempt, e, backoff)
                await asyncio.sleep(backoff)
        raise last_exc

    async def start(self):
        self._running = True
        logger.info("Connector starting")
        await self.discover_symbols()
        await self.start_ws_batches()
        self._oi_task = asyncio.create_task(self._oi_poller())

    async def stop(self):
        self._running = False
        logger.info("Connector stopping")
        # cancel tasks
        for t in self._trade_tasks + self._depth_tasks:
            try:
                t.cancel()
            except Exception:
                pass
        if self._oi_task:
            try:
                self._oi_task.cancel()
            except Exception:
                pass

    async def discover_symbols(self):
        url = f"{Config.FUTURES_REST}/fapi/v1/ticker/24hr"
        data = await self._safe_get(url)
        syms = []
        for it in data:
            try:
                syms.append({"symbol": it.get("symbol"), "vol": float(it.get("quoteVolume", 0.0)), "last": float(it.get("lastPrice",0.0))})
            except Exception:
                continue
        syms_sorted = sorted(syms, key=lambda x: x["vol"], reverse=True)
        if Config.TRACK_ALL_FUTURES:
            tracked = [s["symbol"] for s in syms_sorted]
        else:
            tracked = [s["symbol"] for s in syms_sorted[:Config.TRACK_TOP_N_MANUAL]]
        self.tracked_symbols = tracked
        for s in tracked:
            if s not in self.states:
                self.states[s] = SymbolState(symbol=s)
        logger.info("Prepared %d symbols", len(self.tracked_symbols))
        # init depth snapshots for top subset to avoid heavy boot
        for i, s in enumerate(self.tracked_symbols[:400]):
            try:
                await asyncio.sleep(0.02*(i%20))
                await self._init_depth_snapshot(s)
            except Exception:
                pass

    async def _init_depth_snapshot(self, symbol:str, limit:int=100):
        url = f"{Config.FUTURES_REST}/fapi/v1/depth"
        data = await self._safe_get(url, params={"symbol": symbol, "limit": limit})
        bids = {float(p): float(q) for p,q in data.get("bids", []) if float(q)>0}
        asks = {float(p): float(q) for p,q in data.get("asks", []) if float(q)>0}
        st = self.states.get(symbol)
        if st:
            st.orderbook = OrderbookSnapshot(bids=bids, asks=asks)
            try:
                if bids and asks:
                    st.spread_history.append(min(asks.keys()) - max(bids.keys()))
            except Exception:
                pass

    async def start_ws_batches(self):
        # Build batches and start tasks (TRADES & DEPTH combined streams separately)
        syms = self.tracked_symbols
        trade_batches = [syms[i:i+Config.TRADE_BATCH_SIZE] for i in range(0, len(syms), Config.TRADE_BATCH_SIZE)]
        depth_batches = [syms[i:i+Config.DEPTH_BATCH_SIZE] for i in range(0, len(syms), Config.DEPTH_BATCH_SIZE)]
        # cap concurrency
        trade_batches = trade_batches[:Config.MAX_CONCURRENT_WS]
        depth_batches = depth_batches[:Config.MAX_CONCURRENT_WS]
        for batch in trade_batches:
            stream = "/".join([f"{s.lower()}@trade" for s in batch])
            url = Config.FUTURES_WS_BASE + stream
            t = asyncio.create_task(self._consume_trade_ws(url))
            self._trade_tasks.append(t)
            await asyncio.sleep(0.02)
        for batch in depth_batches:
            stream = "/".join([f"{s.lower()}@depth@100ms" for s in batch])
            url = Config.FUTURES_WS_BASE + stream
            t = asyncio.create_task(self._consume_depth_ws(url))
            self._depth_tasks.append(t)
            await asyncio.sleep(0.02)

    async def _consume_trade_ws(self, url:str):
        logger.info("Opening trade WS")
        while self._running:
            try:
                async with self.session.ws_connect(url, heartbeat=60, timeout=30) as ws:
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        try:
                            data = msg.json()
                        except Exception:
                            continue
                        payload = data.get("data", data)
                        if payload.get("e") in ("trade","aggTrade"):
                            s = payload.get("s")
                            price = float(payload.get("p"))
                            qty = float(payload.get("q"))
                            ts = int(payload.get("T", time.time()*1000))
                            maker = bool(payload.get("m"))
                            st = self.states.get(s)
                            if st:
                                st.trades.append(Trade(timestamp=ts, price=price, quantity=qty, is_buyer_maker=maker))
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("Trade WS error: %s; reconnecting", e)
                await asyncio.sleep(random.uniform(0.5,2.0))

    async def _consume_depth_ws(self, url:str):
        logger.info("Opening depth WS")
        while self._running:
            try:
                async with self.session.ws_connect(url, heartbeat=60, timeout=30) as ws:
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        try:
                            data = msg.json()
                        except Exception:
                            continue
                        payload = data.get("data", data)
                        s = payload.get("s")
                        if not s:
                            continue
                        st = self.states.get(s)
                        if not st:
                            continue
                        bids = payload.get("b", [])
                        asks = payload.get("a", [])
                        for p_str,q_str in bids:
                            p=float(p_str); q=float(q_str)
                            if q==0:
                                st.orderbook.bids.pop(p,None)
                            else:
                                st.orderbook.bids[p]=q
                        for p_str,q_str in asks:
                            p=float(p_str); q=float(q_str)
                            if q==0:
                                st.orderbook.asks.pop(p,None)
                            else:
                                st.orderbook.asks[p]=q
                        # trim depth
                        if len(st.orderbook.bids)>800:
                            st.orderbook.bids = dict(sorted(st.orderbook.bids.items(), key=lambda kv:-kv[0])[:800])
                        if len(st.orderbook.asks)>800:
                            st.orderbook.asks = dict(sorted(st.orderbook.asks.items(), key=lambda kv:kv[0])[:800])
                        try:
                            if st.orderbook.bids and st.orderbook.asks:
                                st.spread_history.append(min(st.orderbook.asks.keys()) - max(st.orderbook.bids.keys()))
                        except Exception:
                            pass
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("Depth WS error: %s; reconnecting", e)
                await asyncio.sleep(random.uniform(0.5,2.0))

    async def _oi_poller(self):
        while self._running:
            try:
                syms = list(self.tracked_symbols)
                for s in syms:
                    try:
                        url = f"{Config.FUTURES_REST}/fapi/v1/openInterest"
                        data = await self._safe_get(url, params={"symbol": s})
                        if data:
                            oi = float(data.get("openInterest",0.0))
                            st = self.states.get(s)
                            if st:
                                st.oi_history.append((int(time.time()*1000), oi))
                    except Exception:
                        pass
                    await asyncio.sleep(0.03)
                await asyncio.sleep(20)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception("OI poll error: %s", e)
                await asyncio.sleep(2.0)

    async def fetch_symbol_price(self, symbol:str)->Optional[float]:
        try:
            async with self._followup_limiter:
                url = f"{Config.FUTURES_REST}/fapi/v1/ticker/price"
                async with self.session.get(url, params={"symbol":symbol}, timeout=10) as resp:
                    if resp.status!=200:
                        return None
                    data = await resp.json()
                    return float(data.get("price",0.0))
        except Exception:
            return None

# ---------------------------
# Feature Engine (real features)
# ---------------------------
class FeatureEngine:
    def klines_from_trades(self, trades:Deque[Trade]) -> Optional[pd.DataFrame]:
        if not trades:
            return None
        buckets=defaultdict(lambda:{"open":None,"high":-1e18,"low":1e18,"close":None,"volume":0.0,"ts":None})
        for tr in trades:
            minute=(tr.timestamp//60000)*60000
            b=buckets[minute]
            if b["open"] is None:
                b["open"]=tr.price
            b["high"]=max(b["high"],tr.price)
            b["low"]=min(b["low"],tr.price)
            b["close"]=tr.price
            b["volume"]+=tr.quantity
            b["ts"]=minute
        rows=[]
        for k in sorted(buckets.keys()):
            v=buckets[k]
            if v["open"] is None:
                continue
            rows.append({"open_time":v["ts"],"open":v["open"],"high":v["high"],"low":v["low"],"close":v["close"],"volume":v["volume"],"close_time":v["ts"]+60000})
        if not rows:
            return None
        df=pd.DataFrame(rows)
        return df

    def compute_atr(self, df:pd.DataFrame, window:int=14)->Optional[float]:
        if df is None or len(df)<2:
            return None
        high=df["high"]; low=df["low"]; close=df["close"]
        tr=pd.concat([high-low,(high-close.shift(1)).abs(),(low-close.shift(1)).abs()],axis=1).max(axis=1)
        atr=tr.rolling(window=window,min_periods=1).mean().iloc[-1]
        return float(atr)

    def compute_bbw(self, df:pd.DataFrame, window:int=20)->Optional[float]:
        if df is None or len(df)<window:
            return None
        ma=df["close"].rolling(window=window).mean()
        std=df["close"].rolling(window=window).std()
        bbw=(2.0*std/(ma+1e-12)).iloc[-1]
        if math.isnan(bbw):
            return None
        return float(bbw)

    def compute_volume_zscore(self, df:pd.DataFrame, window:int=24)->float:
        if df is None or len(df)<2:
            return 0.0
        vs=df["volume"].iloc[-window:]
        if len(vs)<2:
            return 0.0
        zs=zscore(vs.fillna(0))
        z=float(zs[-1]) if not math.isnan(zs[-1]) else 0.0
        return z

    def compute_vpvr_gap_z(self, df:pd.DataFrame, bins:int=50)->float:
        if df is None or len(df)<20:
            return 0.0
        prices=df["close"].values
        vols=df["volume"].values
        hist, edges=np.histogram(prices,bins=bins,weights=vols)
        sh=pd.Series(hist).rolling(3,min_periods=1,center=True).mean().fillna(0).values
        if len(sh)<3:
            return 0.0
        valley=float(min(sh)); mean=float(np.mean(sh)); std=float(np.std(sh))+1e-9
        return float((mean-valley)/std) if std>0 else 0.0

    def compute_cvd_slope_z(self, trades:Deque[Trade], window_minutes:int=120, bucket_mins:int=5)->float:
        if not trades or len(trades)<5:
            return 0.0
        now=int(time.time()*1000)
        cutoff=now-window_minutes*60*1000
        buckets=defaultdict(float)
        for tr in trades:
            if tr.timestamp<cutoff:
                continue
            bucket=int((tr.timestamp-cutoff)//(bucket_mins*60*1000))
            sign=1.0 if not tr.is_buyer_maker else -1.0
            buckets[bucket]+=sign*tr.quantity*tr.price
        if len(buckets)<3:
            return 0.0
        vals=np.array([buckets[k] for k in sorted(buckets.keys())])
        x=np.arange(len(vals)); A=np.vstack([x,np.ones(len(x))]).T
        m,_=np.linalg.lstsq(A,vals,rcond=None)[0]
        std=float(np.std(vals))+1e-9
        return float((m-np.mean(vals))/std) if std>0 else 0.0

    def compute_oi_slope_z(self, oi_history:Deque[Tuple[int,float]])->float:
        if not oi_history or len(oi_history)<4:
            return 0.0
        arr=np.array([v for ts,v in oi_history])
        x=np.arange(len(arr)); A=np.vstack([x,np.ones(len(x))]).T
        m,_=np.linalg.lstsq(A,arr,rcond=None)[0]
        std=float(np.std(arr))+1e-9
        return float((m-np.mean(arr))/std) if std>0 else 0.0

    def compute_spread_compression_ratio(self, spreads:Deque[float])->float:
        if not spreads or len(spreads)<10:
            return 1.0
        arr=np.array(list(spreads)); median=float(np.median(arr)); latest=float(arr[-1])
        if median==0:
            return 1.0
        return float(latest/(median+1e-12))

    def compute_features(self, st:SymbolState)->Dict[str,Any]:
        if len(st.klines_1m)>=8:
            df=pd.DataFrame(list(st.klines_1m))
        else:
            df=self.klines_from_trades(st.trades)
        features={}
        
    else:
        denom = df["close"].iloc[-6]
        # guard against zero, NaN, or infinite denominators
        if denom == 0 or np.isnan(denom) or not np.isfinite(denom):
            if df is None or len(df) <= 6:
        features["recent_pct_change"] = 0.0
    else:
        denom = df["close"].iloc[-6]
        # guard against zero, NaN, or infinite denominators
        if denom == 0 or np.isnan(denom) or not np.isfinite(denom):
            features["recent_pct_change"] = 0.0
        else:
            features["recent_pct_change"] = float((df["close"].iloc[-1] - denom) / denom)
        else:
            features["recent_pct_change"] = float((df["close"].iloc[-1] - denom) / denom)
        features["spot_volume_z"]=self.compute_volume_zscore(df,window=24)
        features["atr"]=self.compute_atr(df,window=14) if df is not None else None
        features["bbw"]=self.compute_bbw(df,window=20) if df is not None else None
        features["vpvr_gap_z"]=self.compute_vpvr_gap_z(df,bins=50)
        features["cvd_slope_z"]=self.compute_cvd_slope_z(st.trades,window_minutes=120,bucket_mins=5)
        features["oi_slope_z"]=self.compute_oi_slope_z(st.oi_history)
        features["spread_compression_ratio"]=self.compute_spread_compression_ratio(st.spread_history)
        total_qty=sum(tr.quantity for tr in st.trades) if st.trades else 0.0
        taker_buy_qty=sum(tr.quantity for tr in st.trades if not tr.is_buyer_maker) if st.trades else 0.0
        features["taker_buy_share"]=(taker_buy_qty/total_qty) if total_qty>0 else 0.5
        features["volume_24h"]=float(df["volume"].sum()) if df is not None else 0.0
        # optional placeholders
        features["funding_rate"]=0.0
        features["orderbook_imbalance"]=self._compute_ob_imbalance(st)
        return features

    def _compute_ob_imbalance(self, st:SymbolState)->float:
        if not st.orderbook.bids or not st.orderbook.asks:
            return 0.0
        top_bids=sorted(st.orderbook.bids.items(), key=lambda kv:-kv[0])[:5]
        top_asks=sorted(st.orderbook.asks.items(), key=lambda kv:kv[0])[:5]
        bid_vol=sum(q for p,q in top_bids); ask_vol=sum(q for p,q in top_asks)
        total=bid_vol+ask_vol
        if total==0: return 0.0
        return (bid_vol-ask_vol)/total

# ---------------------------
# ScoringEngine (all archetypes)
# ---------------------------
class ScoringEngine:
    def __init__(self):
        self.thresholds=ArchetypeThresholds()

    def _clamp(self,v,lo=0.0,hi=100.0):
        if v is None or not isinstance(v,(int,float)) or math.isnan(v): return 0.0
        return max(lo,min(hi,float(v)))

    def _z_to_score(self,z,weight=10.0,cap=40.0):
        if z is None: return 0.0
        return self._clamp(min(cap,abs(z)*weight))

    def _pct_to_score(self,pct,scale=100.0,cap=40.0):
        if pct is None: return 0.0
        return self._clamp(min(cap,abs(pct)*scale))

    def score_lsk(self,f):
        s=0.0
        if f.get("funding_rate",0.0)<=self.thresholds.LSK_FUNDING_DEEPLY_NEGATIVE: s+=30.0
        s+=self._z_to_score(f.get("oi_slope_z",0.0),weight=8.0,cap=30.0)
        s+=self._z_to_score(f.get("cvd_slope_z",0.0),weight=6.0,cap=20.0)
        return self._clamp(s,0,60)

    def score_melania(self,f):
        s=0.0
        s+=self._z_to_score(f.get("spot_volume_z",0.0),weight=9.0,cap=60.0)
        if f.get("social_score",0.0)>=self.thresholds.MELANIA_SOCIAL_SCORE_THRESHOLD: s+=20.0
        return self._clamp(s,0,80)

    def score_vpvr(self,f):
        s=self._z_to_score(f.get("vpvr_gap_z",0.0),weight=10.0,cap=70.0)
        if f.get("spread_compression_ratio",1.0)<=0.6: s+=10.0
        return self._clamp(s,0,90)

    def score_micro(self,f):
        s=0.0
        if f.get("taker_buy_share",0.0)>=ArchetypeThresholds.MICROCAP_TAKER_BUY_SHARE: s+=35.0
        s+=self._z_to_score(f.get("spot_volume_z",0.0),weight=5.0,cap=30.0)
        if f.get("spread_compression_ratio",1.0)<=0.5: s+=10.0
        return self._clamp(s,0,90)

    def score_ffp(self,f):
        s=0.0
        if f.get("funding_rate",0.0)<=-0.004: s+=25.0
        s+=self._z_to_score(f.get("oi_slope_z",0.0),weight=9.0,cap=35.0)
        s+=self._z_to_score(f.get("spot_volume_z",0.0),weight=6.0,cap=30.0)
        return self._clamp(s,0,90)

    def score_aura(self,f):
        s=0.0
        s+=self._pct_to_score(f.get("recent_pct_change",0.0),scale=200.0,cap=30.0)
        s+=self._z_to_score(f.get("cvd_slope_z",0.0),weight=8.0,cap=30.0)
        ob=f.get("orderbook_imbalance",0.0)
        if abs(ob)>0.15: s+=15.0 if ob>0 else 5.0
        return self._clamp(s,0,80)

    def score_godcandle(self,f):
        s=0.0
        s+=self._z_to_score(f.get("cvd_slope_z",0.0),weight=10.0,cap=40.0)
        s+=self._pct_to_score(f.get("recent_pct_change",0.0),scale=150.0,cap=30.0)
        if f.get("largest_trade_z",None) is not None:
            s+=self._z_to_score(f.get("largest_trade_z",0.0),weight=8.0,cap=20.0)
        return self._clamp(s,0,90)

    def score_bake(self,f):
        s=0.0
        if f.get("funding_rate",0.0)<=-0.0075: s+=25.0
        if f.get("recent_pct_change",0.0)<=-0.30: s+=25.0
        s+=self._z_to_score(f.get("cvd_slope_z",0.0),weight=8.0,cap=30.0)
        return self._clamp(s,0,80)

    def score_zec(self,f):
        s=0.0
        bbw=f.get("bbw",None)
        if bbw is not None:
            s+=self._clamp(min(40.0,max(0.0,(0.5-bbw)*80.0)))
        s+=self._z_to_score(f.get("cvd_slope_z",0.0),weight=6.0,cap=30.0)
        s+=self._z_to_score(f.get("oi_slope_z",0.0),weight=6.0,cap=30.0)
        return self._clamp(s,0,90)

    def compute_subscores(self,f):
        subs={}
        subs["lsk"]=self.score_lsk(f)
        subs["melania"]=self.score_melania(f)
        subs["vpvr"]=self.score_vpvr(f)
        subs["micro"]=self.score_micro(f)
        subs["ffp"]=self.score_ffp(f)
        subs["aura"]=self.score_aura(f)
        subs["godcandle"]=self.score_godcandle(f)
        subs["bake"]=self.score_bake(f)
        subs["zec"]=self.score_zec(f)
        return subs

    def compute_composite_score(self,st:SymbolState)->float:
        f=st.features or {}
        subs=self.compute_subscores(f)
        st.sub_scores=subs
        raw=sum(subs.values())
        scale_divisor=2.5
        pump=raw/scale_divisor
        recent=f.get("recent_pct_change",0.0)
        if abs(recent)>=ArchetypeThresholds.PUMP_SCORE_FOMO_PENALTY_THRESHOLD_PERCENT:
            pump*=(1-min(0.5,abs(recent)/1.0))
        pump_score=self._clamp(pump,0.0,100.0)
        st.pump_score=pump_score
        return pump_score

# ---------------------------
# TP4 Hyper-scaler Engine
# ---------------------------
def round_to_psych(price:float)->float:
    if price<=0: return price
    if price<0.1: step=0.001
    elif price<1: step=0.01
    elif price<10: step=0.05
    elif price<100: step=0.1
    elif price<1000: step=0.5
    else: step=1.0
    return round(round(price/step)*step,12)

def compute_hvns(df:pd.DataFrame,bins:int=120,top_n:int=5)->List[float]:
    if df is None or len(df)<10: return []
    prices=df["close"].values; vols=df["volume"].values
    hist, edges=np.histogram(prices,bins=bins,weights=vols)
    idxs=np.argsort(hist)[-top_n:]
    hvns=[]
    for i in idxs:
        hvns.append((edges[i]+edges[i+1])/2.0)
    return sorted(hvns)

class HyperScaler:
    """
    Hyper-scaler continuously generates TP levels as price runs:
    - Base levels: HVNs, Fibonacci extensions, ATR multiples
    - Ensures min distance between levels
    - Keeps max active TP orders
    """
    def __init__(self, app):
        self.app=app
        self.active_targets: Dict[str,List[Tuple[int,float]]] = defaultdict(list)  # symbol -> list of (order_id, price)
        self._task: Optional[asyncio.Task]=None
        self._running=False

    def start(self):
        self._running=True
        self._task=asyncio.create_task(self._loop())

    def stop(self):
        self._running=False
        if self._task:
            self._task.cancel()

    async def _loop(self):
        while self._running and self.app._running:
            try:
                for sym,st in list(self.app.states.items()):
                    # skip low data symbols
                    if not st.trades:
                        continue
                    # compute current mid price
                    best_bid = max(st.orderbook.bids.keys()) if st.orderbook.bids else None
                    best_ask = min(st.orderbook.asks.keys()) if st.orderbook.asks else None
                    if best_bid and best_ask:
                        mid=(best_bid+best_ask)/2.0
                    else:
                        mid=st.trades[-1].price if st.trades else None
                    if mid is None:
                        continue
                    # prepare kline df
                    fe=self.app.feature_engine
                    df = pd.DataFrame(list(st.klines_1m)) if len(st.klines_1m)>=24 else fe.klines_from_trades(st.trades)
                    hvns = compute_hvns(df,bins=120,top_n=Config.TP_HVNS_TOP_N)
                    atr = fe.compute_atr(df,window=14) or st.features.get("atr") or max(1e-8, mid*0.001)
                    # propose candidate TPs above mid, combine hvns + atr multiples
                    candidates=[]
                    for h in hvns:
                        if h>mid*1.001:
                            candidates.append(("hv",h))
                    # fibonacci-like from recent swing high if available
                    swings = []
                    if df is not None and len(df)>30:
                        closes=df["close"].values
                        swings=[float(max(closes[-60:]))]
                    for s in swings:
                        for r in [1.272,1.618,2.0]:
                            cand = mid + (s-mid)*r
                            if cand>mid*1.001:
                                candidates.append(("fib",cand))
                    # atr multiples
                    for m in [1.5,3.0,5.0,8.0]:
                        cand = mid + atr*m
                        candidates.append(("atr",cand))
                    # sort unique candidates ascending
                    unique=[]
                    seen=[]
                    for typ,p in sorted(candidates,key=lambda x:x[1]):
                        if any(abs(p - sprice)/sprice < Config.TP_GENERATION_MIN_DISTANCE for sprice in seen):
                            continue
                        seen.append(p)
                        unique.append(p)
                    # limit candidates
                    final = unique[:Config.TP_MAX_ACTIVE]
                    # compare with existing active orders in DB
                    existing_prices = [p for (_id,p) in self.active_targets.get(sym,[])]
                    to_create=[]
                    for p in final:
                        if not any(abs(p - ep)/ep < Config.TP_GENERATION_MIN_DISTANCE for ep in existing_prices):
                            to_create.append(p)
                    # create planned orders for new TPs (sell side), small size
                    for p in to_create:
                        oid = create_planned_order(triggered_id=0, symbol=sym, side="sell", price=float(p), size_pct=0.05, type_str="tp_hyper")
                        self.active_targets[sym].append((oid,p))
                        # record as planned_fill placeholder
                        if mid == 0 or np.isnan(mid) or not np.isfinite(mid):
        rel = 0.0
    else:
        rel = float((p - mid) / mid)
    record_planned_fill(oid, p, rel, note="tp_hyper_created")
                    # prune closed/cancelled orders from active_targets by checking DB
                    new_list=[]
                    conn=sqlite3.connect(Config.DB_PATH, timeout=30)
                    c=conn.cursor()
                    for oid,p in self.active_targets.get(sym,[]):
                        c.execute("SELECT status FROM planned_orders WHERE id=?", (oid,))
                        r=c.fetchone()
                        status=r[0] if r else "open"
                        if status=="open":
                            new_list.append((oid,p))
                    conn.close()
                    self.active_targets[sym]=new_list
                await asyncio.sleep(Config.TP_GENERATION_INTERVAL)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception("HyperScaler loop error: %s", e)
                await asyncio.sleep(1.0)

# ---------------------------
# Application orchestration
# ---------------------------
class Application:
    def __init__(self):
        self.states: Dict[str, SymbolState] = {}
        self.session = aiohttp.ClientSession()
        self.connector = BinanceFuturesConnector(self.session, self.states)
        self.feature_engine = FeatureEngine()
        self.scoring = ScoringEngine()
        self.console = Console()
        self._live: Optional[Live] = None
        self._tasks: List[asyncio.Task] = []
        self._running=False
        self.score_history: Dict[str,Deque[float]] = defaultdict(lambda: deque(maxlen=Config.DATA_WINDOW_SIZES["score_history"]))
        self.hyper = HyperScaler(self)

    async def start(self):
        init_db()
        logger.info("Application starting")
        await self.connector.start()
        # start hyper-scaler
        self.hyper.start()
        # UI
        table=self._build_table()
        self._live = Live(Panel(table, title="Parallel AI — Master (USDT-M, WS1, TP4)", border_style="cyan"), console=self.console, refresh_per_second=4, transient=False)
        self._live.start()
        self._running=True
        self._tasks.append(asyncio.create_task(self._feature_loop()))
        self._tasks.append(asyncio.create_task(self._scoring_loop()))
        # background cleaner for DB / followups could be added

    async def stop(self):
        logger.info("Application stopping")
        self._running=False
        # stop hyper
        self.hyper.stop()
        # cancel tasks
        for t in self._tasks:
            try:
                t.cancel()
            except Exception:
                pass
        await self.connector.stop()
        # close session
        try:
            await self.session.close()
        except Exception:
            pass
        if self._live:
            try:
                self._live.stop()
            except Exception:
                pass

    async def _feature_loop(self):
        while True:
            try:
                for sym,st in list(self.states.items()):
                    st.features = self.feature_engine.compute_features(st)
                await asyncio.sleep(Config.FEATURE_CALC_INTERVAL)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception("Feature loop error: %s", e)
                await asyncio.sleep(1.0)

    async def _scoring_loop(self):
        while True:
            try:
                alerts=[]
                for sym,st in list(self.states.items()):
                    score=self.scoring.compute_composite_score(st)
                    self.score_history[sym].append(score)
                    # pre-alert gating: require some minimum liquidity to avoid spam
                    vol24 = st.features.get("volume_24h",0.0)
                    spread_ratio = st.features.get("spread_compression_ratio",1.0)
                    # gating: require at least 10k in volume or else only allow high scores
                    if score>=Config.ALERT_PUMP_SCORE_THRESHOLD and (vol24>1000 or score>90):
                        now=time.time()
                        if now - st.last_alert_timestamp >= Config.ALERT_COOLDOWN_SECONDS:
                            st.last_alert_timestamp=now
                            top_arch = max(st.sub_scores.items(), key=lambda kv: kv[1])[0] if st.sub_scores else ""
                            # determine price (prefer best ask)
                            price=None
                            try:
                                best_ask = min(st.orderbook.asks.keys()) if st.orderbook.asks else None
                                if best_ask:
                                    price=float(best_ask)
                                elif st.trades:
                                    price=float(st.trades[-1].price)
                                else:
                                    price=await self.connector.fetch_symbol_price(sym)
                            except Exception:
                                price=None
                            ts=int(time.time()*1000)
                            features_json=json.dumps(st.features)
                            triggered_id=log_triggered_trade(ts,sym,score,top_arch,price if price else 0.0,features_json)
                            alerts.append((sym,score,top_arch,price,triggered_id))
                            # for hyper-scaler, create initial planned_orders: SL and initial TP cluster
                            # create SL
                            atr = st.features.get("atr") or 0.0
                            sl_price = (price - 1.5*atr) if atr and price else (price*0.92 if price else None)
                            if sl_price:
                                create_planned_order(triggered_id,sym,"sell",float(sl_price),1.0,"sl")
                            # create a few initial TP entries (let hyper-scaler take over later)
                            if price:
                                # small immediate TP1/TP2
                                for mult in [1.02,1.04]:
                                    tp_price = float(price*mult)
                                    create_planned_order(triggered_id,sym,"sell",tp_price,0.2,"tp_init")
                # log alerts
                for a in alerts:
                    logger.info("ALERT: %s score=%.1f archetype=%s price=%s id=%d", a[0], a[1], a[2], a[3], a[4])
                # update UI
                if self._live:
                    table=self._build_table()
                    self._live.update(Panel(table, title="Parallel AI — Master (live)", border_style="magenta"))
                await asyncio.sleep(Config.UI_REFRESH_RATE_SECONDS)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception("Scoring loop error: %s", e)
                await asyncio.sleep(1.0)

    def _build_table(self)->Table:
        t=Table(expand=True,box=box.SIMPLE,pad_edge=True)
        t.add_column("Symbol",style="bold cyan")
        t.add_column("Score",justify="right")
        t.add_column("Top Archetype",justify="left")
        t.add_column("Vol24",justify="right")
        t.add_column("Chg%",justify="right")
        t.add_column("ScoreSpark",justify="left")
        t.add_column("PriceSpark",justify="left")
        rows=sorted(self.states.items(), key=lambda kv: kv[1].pump_score, reverse=True)[:Config.MAX_CANDIDATES_IN_UI]
        for sym,st in rows:
            score=st.pump_score
            if score>=80:
                score_cell=f"[bold white on green]{score:5.1f}[/]"
            elif score>=50:
                score_cell=f"[bold black on yellow]{score:5.1f}[/]"
            else:
                score_cell=f"[bold white on red]{score:5.1f}[/]"
            top = max(st.sub_scores.items(), key=lambda kv: kv[1])[0] if st.sub_scores else "-"
            vol = st.features.get("volume_24h",0.0)
            vol_str = f"{vol/1_000_000:.2f}M" if isinstance(vol,(int,float)) and vol>=1_000_000 else (f"{vol/1000:.1f}K" if isinstance(vol,(int,float)) and vol>=1000 else str(int(vol) if isinstance(vol,(int,float)) else vol))
            ch = st.features.get("recent_pct_change",0.0)
            ch_text = f"{ch*100:+.2f}%"
            if ch>=0.05:
                ch_text=f"[bold green]{ch_text}[/]"
            elif ch<=-0.05:
                ch_text=f"[bold red]{ch_text}[/]"
            else:
                ch_text=f"[yellow]{ch_text}[/]"
            score_hist=list(self.score_history.get(sym,[]))
            score_spark = sparkline(score_hist,length=8)
            closes=[]
            if len(st.klines_1m)>=4:
                closes=[float(k["close"]) for k in list(st.klines_1m)[-12:]]
            elif st.trades:
                closes=[tr.price for tr in list(st.trades)[-12:]]
            price_spark = sparkline(closes,length=8) if closes else ""
            t.add_row(sym, score_cell, top, vol_str, ch_text, score_spark, price_spark)
        if len(rows)==0:
            t.add_row("-", "0.0", "-", "-", "-", "-", "-")
        return t

# ---------------------------
# Runner
# ---------------------------
async def main():
    app=Application()
    await app.start()
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    finally:
        await app.stop()

if __name__=="__main__":
    loop=asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt, shutting down")
    finally:
        # best-effort cleanup
        pending=asyncio.all_tasks(loop=loop)
        for t in pending:
            t.cancel()
        loop.run_until_complete(asyncio.sleep(0.1))
        loop.close()
