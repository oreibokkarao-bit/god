#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Parallel Breakout Setup Monitor (24 Archetypes) with Tier-1 + Tier-2 upgrades:

Tier 1:
- Robust normalized composite score (logistic-reg-style) using per-feature z-scores.
- 24 archetypes, all running in parallel (non-cancelling), gated by composite alpha.
- Multi-timeframe alignment (approx 1h/4h trend from trades).
- Magnetic confluence zones from REAL data:
  - Recent swing highs/lows (from true prices),
  - Fib extensions of the swing range,
  - Local swing-based pivots,
  - Intraday VWAP & bands (from real trade stream),
  - Local volume-profile HVN/LVN.
- TP1/TP2/TP3 & SL derived from these zones, not % guesses, with RR guards.
- SQLite trade life log: OPEN -> TP/SL verdict with RR filter.
- Tier-1 leverage governor (vol/liquidity/RR aware).
- Archetype internals fully normalized via z-scores + RVOL + volatility regimes.
- RVOL vs own 1h history for volume heat.
- Noise-aware SL using 15m range + realized vol to avoid whipsaw without killing big winners.

Tier 2:
- Async I/O for all WebSocket + REST (aiohttp + asyncio).
- Safe REST wrapper with token-bucket rate limiter and 418/429-aware exponential backoff.
- Structured JSON logs on trade open/close.
- Simple Prometheus-style /metrics endpoint (localhost:8001/metrics) for monitoring.

Honesty / data sources:
- All signals (price, volume, OI, orderbook, trades) come directly from Binance Futures public endpoints.
- CVD is computed from the raw @trade stream using Binance's taker/maker flag (isBuyerMaker → taker side = not isBuyerMaker).
- Magnetic levels are built entirely from real traded prices & orderbook in the current window (no synthetic candles).
"""

import asyncio
import aiohttp
from aiohttp import web
import logging
import math
import time
import random
from collections import deque
from typing import Dict, List, Deque, Optional, Tuple

import numpy as np
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich import box

import json
from math import isfinite
from dataclasses import dataclass, field
import sqlite3
import os

# ======================================================================
# Logging
# ======================================================================

logger = logging.getLogger("parallel_ai_full")
logger.setLevel(logging.INFO)
_handler = logging.StreamHandler()
_handler.setFormatter(
    logging.Formatter(
        "%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
)
logger.addHandler(_handler)

# ======================================================================
# Config / Engine parameters
# ======================================================================

BINANCE_FUTURES_WS = "wss://fstream.binance.com/stream"
BINANCE_FUTURES_REST = "https://fapi.binance.com"

UNIVERSE_LIMIT = 650
TRADE_WS_SHARDS = 8
DEPTH_WS_SHARDS = 8

DEPTH_LEVEL = 10  # top-10 book levels

CVD_LOOKBACK_SEC = 5 * 60
VOL_LOOKBACK_SEC = 5 * 60
RET_LOOKBACK_SEC = 15 * 60
OI_LOOKBACK_SEC = 30 * 60

MIN_NOTION_USDT = 200_000  # min 5m notional
MIN_PRICE = 0.0005

TABLE_REFRESH_SEC = 2.0
OI_POLL_INTERVAL_SEC = 30.0

MAX_ROWS = 10          # setups rows, fixed height
OPEN_ROWS = 10         # open trades rows, fixed height
CLOSED_ROWS = 8        # closed trades rows, fixed height

# ----- Composite score engine -----

COMPOSITE_WEIGHTS = {
    "vol_sum_5m": 1.0,
    "ret_15m": 1.0,
    "cvd_slope_5m": 1.0,
    "ob_imb": 1.0,
    "oi_chg": 1.0,
    "rv_15m": 1.0,
    "range_15m": 1.0,
}

SCORE_DECAY_ALPHA = 0.3  # EWMA alpha memory

GLOBAL_RAW_SCORE_GATE = 4.0   # hard gate on raw composite
ALPHA_THRESHOLD = 0.0         # soft gate on EWMA alpha

# Risk management
MIN_RR = 2.0   # minimum RR to surface & trigger

# Volatility-aware & sanity guards for SL / RR
MIN_SL_PCT = 0.003      # 0.3% minimum stop distance from entry
MAX_SL_PCT = 0.02       # 2% maximum stop distance from entry
MAX_RR_DEFAULT = 8.0    # cap fantasy RR
TOO_CLOSE_EPS = 0.0008  # skip trades where SL is glued to entry

# LAST-RESORT fallback if zones are degenerate
SL_PCT_FALLBACK = 0.01  # 1% SL
TP_PCT_FALLBACK = 0.03  # 3/6/9% TP ladder

# Volatility-aware SL noise floor (15m range + realized vol)
NOISE_RANGE_MULT = 0.5   # at least 0.5 * 15m range
NOISE_RV_MULT    = 3.0   # or ~3x realized vol

# Rate limiting for all REST calls
REST_MAX_REQ_PER_SEC = 8.0
REST_BUCKET_CAPACITY = 16.0

# SQLite trade log
TRADES_DB_PATH = os.path.join(os.path.dirname(__file__), "breakout_trades.db")
MAX_CLOSED_SHOWN = 50

# RVOL / regime configs (per-symbol 5m vol vs its own history)
RVOL_HISTORY_SEC = 3600.0     # 1h history window
RVOL_MIN_OBS     = 12         # need at least 12 snapshots
RVOL_HIGH        = 1.8        # >1.8x median => elevated rvol
RVOL_EXTREME     = 3.0        # >3x median => extreme rvol

# ======================================================================
# Archetype metadata (24 archetypes)
# ======================================================================

ARCHETYPE_INFO: Dict[str, Tuple[str, str]] = {
    # original 8
    "zec": ("Breakout", "Fresh breakout – price already moving up with strong volume & real buyers"),
    "mel": ("Squeeze", "Volatility squeeze – heavy volume but price still flat (coiling)"),
    "fil": ("Trend ride", "Trend continuation – uptrend with renewed volume & open interest"),
    "moca": ("Bounce", "Deep dip bounce – sharp drop with strong bids underneath"),
    "cookie": ("Fast scalp", "Fast micro-move – violent short-term spike"),
    "anime": ("Hype/pump", "Hype / pump-style breakout – strong move plus leverage"),
    "turbo": ("High-beta", "High-beta squeeze – very strong move on a volatile coin"),
    "broccoli": ("Range coil", "Range coil – sideways price but big volume & stable OI (pre-breakout)"),

    # new 16
    "breakout_short": ("Breakdown", "Downside breakout – strong sell move on heavy volume"),
    "slow_grind_up": ("Slow grind up", "Gentle uptrend – small positive return, low realized vol"),
    "slow_grind_down": ("Slow grind down", "Gentle downtrend – small negative return, low realized vol"),
    "capitulation": ("Capitulation dump", "Brutal dump – very negative return, huge volume, heavy CVD sell, OI flush"),
    "bid_wall": ("Bid wall", "Strong bid wall – big bid imbalance, price holding"),
    "ask_wall": ("Ask wall", "Strong ask wall – big ask imbalance, price capped"),
    "oi_build_long": ("OI build long", "Open interest building on the long side"),
    "oi_build_short": ("OI build short", "Open interest building on the short side"),
    "oi_flush": ("OI flush", "Open interest flushing out with a strong move"),
    "cvd_div_up": ("CVD div up", "Bullish CVD divergence – CVD rising while price not confirming"),
    "cvd_div_down": ("CVD div down", "Bearish CVD divergence – CVD falling while price not confirming"),
    "liquidity_vacuum": ("Liquidity vacuum", "Big price move on modest volume and thin book"),
    "mean_revert_band": ("Mean reversion band", "Tight chop – tiny move, balanced book, tiny CVD"),
    "micro_chop": ("Micro chop", "Ultra-boring micro-chop – very low vol, low volume"),
    "shock_up": ("Shock up", "Shock up move – huge positive return, big volume, strong buy CVD"),
    "shock_down": ("Shock down", "Shock down move – huge negative return, big volume, strong sell CVD"),
}

ARCHETYPE_DIRECTION: Dict[str, str] = {
    # long-biased
    "zec": "long",
    "mel": "long",
    "fil": "long",
    "moca": "long",
    "cookie": "long",
    "anime": "long",
    "turbo": "long",
    "broccoli": "long",
    "slow_grind_up": "long",
    "cvd_div_up": "long",
    "bid_wall": "long",
    "shock_up": "long",
    "oi_build_long": "long",

    # short-biased
    "breakout_short": "short",
    "slow_grind_down": "short",
    "capitulation": "short",
    "ask_wall": "short",
    "oi_build_short": "short",
    "cvd_div_down": "short",
    "shock_down": "short",

    # neutral
    "oi_flush": "neutral",
    "liquidity_vacuum": "neutral",
    "mean_revert_band": "neutral",
    "micro_chop": "neutral",
}

# ======================================================================
# Helpers
# ======================================================================

def now_ms() -> int:
    return int(time.time() * 1000)


def now_s() -> float:
    return time.time()


def safe_div(a: float, b: float) -> float:
    if not b:
        return 0.0
    return a / b


def ema(prev: float, value: float, alpha: float) -> float:
    if prev is None or not isfinite(prev):
        return value
    return alpha * value + (1.0 - alpha) * prev


_SPARK_CHARS = "▁▂▃▄▅▆▇█"


def sparkline(values: List[float], length: int = 8) -> str:
    """NumPy 2.x safe sparkline."""
    if not values:
        return " " * length
    arr = np.array(values[-length:], dtype=float)
    ptp_val = np.ptp(arr)
    if ptp_val == 0:
        return _SPARK_CHARS[0] * len(arr)
    norm = (arr - arr.min()) / (ptp_val + 1e-9)
    return "".join(
        _SPARK_CHARS[int(round(v * (len(_SPARK_CHARS) - 1)))]
        for v in norm
    )


def compute_robust_scaler(values: List[float]) -> Tuple[float, float]:
    if not values:
        return 0.0, 1.0
    arr = np.array(values, dtype=float)
    med = float(np.median(arr))
    q1, q3 = np.percentile(arr, [25, 75])
    iqr = float(q3 - q1)
    if iqr <= 1e-9:
        iqr = 1.0
    return med, iqr


def detect_trend_from_prices(prices: Deque[Tuple[float, float]]) -> str:
    """Very lightweight trend detector for 1h / 4h deques."""
    if len(prices) < 10:
        return "chop"
    arr = np.array([p for _, p in prices], dtype=float)
    if arr[0] <= 0 or not np.isfinite(arr[0]) or not np.isfinite(arr[-1]):
        return "chop"
    change = (arr[-1] - arr[0]) / arr[0]
    if change > 0.01:
        return "up"
    if change < -0.01:
        return "down"
    return "chop"


def passes_mtf_filters(state: "SymbolState", direction: str) -> bool:
    """Simple Tier-1 gate for 1h/4h context."""
    if direction == "long":
        if state.trend_1h == "down" or state.trend_4h == "down":
            return False
    elif direction == "short":
        if state.trend_1h == "up" or state.trend_4h == "up":
            return False
    return True


def suggest_leverage_for_symbol(
    feats: Dict[str, float],
    rr: float,
    direction: str,
) -> float:
    """Tier-1 leverage governor using vol, liquidity, RR."""
    price = feats.get("last_price", 0.0)
    vol5m = feats.get("vol_sum_5m", 0.0)
    range_15m = feats.get("range_15m", 0.0)

    if price <= 0:
        return 1.0

    # volatility bucket from 15m range
    if range_15m < 0.01:
        base_max = 10.0
    elif range_15m < 0.02:
        base_max = 5.0
    else:
        base_max = 3.0

    # liquidity adjustment: low volume => cut leverage
    if vol5m < MIN_NOTION_USDT:
        base_max = 1.0
    elif vol5m < 2 * MIN_NOTION_USDT:
        base_max = min(base_max, 3.0)

    # borderline RR trades don't get biggest leverage
    if rr < MIN_RR + 0.5:
        base_max = min(base_max, 5.0)

    # bucketize
    if base_max >= 8.0:
        return 10.0
    if base_max >= 4.0:
        return 5.0
    if base_max >= 2.0:
        return 3.0
    return 1.0

# ======================================================================
# Rate limiter
# ======================================================================

class RateLimiter:
    def __init__(self, rate_per_sec: float, capacity: float):
        self.rate = float(rate_per_sec)
        self.capacity = float(capacity)
        self.tokens = float(capacity)
        self.updated_at = now_s()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: float = 1.0):
        async with self._lock:
            while True:
                now = now_s()
                elapsed = now - self.updated_at
                self.updated_at = now
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return
                missing = tokens - self.tokens
                wait_time = missing / self.rate if self.rate > 0 else 0.1
                await asyncio.sleep(wait_time)

# ======================================================================
# Symbol state
# ======================================================================

@dataclass
class SymbolState:
    symbol: str
    last_price: float = 0.0
    last_ts: float = 0.0

    trades: Deque[Tuple[float, float, float, str]] = field(default_factory=deque)  # (ts, price, qty, side)
    prices: Deque[Tuple[float, float]] = field(default_factory=deque)              # ~15m window
    prices_1h: Deque[Tuple[float, float]] = field(default_factory=deque)           # ~1h window
    prices_4h: Deque[Tuple[float, float]] = field(default_factory=deque)           # ~4h window

    cvd: float = 0.0
    cvd_history: Deque[Tuple[float, float]] = field(default_factory=deque)
    volume_history: Deque[Tuple[float, float]] = field(default_factory=deque)

    bids: List[Tuple[float, float]] = field(default_factory=list)
    asks: List[Tuple[float, float]] = field(default_factory=list)
    ob_imbalance: float = 0.0

    oi_history: Deque[Tuple[float, float]] = field(default_factory=deque)
    oi_ema: Optional[float] = None

    archetype_scores: Dict[str, float] = field(default_factory=dict)
    archetype_reason: Dict[str, str] = field(default_factory=dict)

    composite_alpha: float = 0.0
    composite_raw: float = 0.0

    feature_z: Dict[str, float] = field(default_factory=dict)
    vol_regime: str = "normal"  # 'calm' / 'normal' / 'spicy'
    trend_1h: str = "chop"      # 'up' / 'down' / 'chop'
    trend_4h: str = "chop"

    # Per-symbol 5m volume history for RVOL vs own history
    vol_5m_history: Deque[Tuple[float, float]] = field(default_factory=deque)

    # ---- Events ----

    def on_trade(self, ts: float, price: float, qty: float, side: str):
        self.last_ts = ts
        self.last_price = price
        self.trades.append((ts, price, qty, side))
        self.prices.append((ts, price))
        self.prices_1h.append((ts, price))
        self.prices_4h.append((ts, price))

        vol_notional = price * qty
        self.volume_history.append((ts, vol_notional))

        if side == "buy":
            self.cvd += vol_notional
        else:
            self.cvd -= vol_notional
        self.cvd_history.append((ts, self.cvd))

        # trim
        cutoff_cvd = ts - CVD_LOOKBACK_SEC
        while self.cvd_history and self.cvd_history[0][0] < cutoff_cvd:
            self.cvd_history.popleft()
        cutoff_vol = ts - VOL_LOOKBACK_SEC
        while self.volume_history and self.volume_history[0][0] < cutoff_vol:
            self.volume_history.popleft()
        cutoff_price = ts - RET_LOOKBACK_SEC
        while self.prices and self.prices[0][0] < cutoff_price:
            self.prices.popleft()

        # 1h (~3600s) and 4h (~14400s) windows
        cutoff_1h = ts - 3600.0
        while self.prices_1h and self.prices_1h[0][0] < cutoff_1h:
            self.prices_1h.popleft()
        cutoff_4h = ts - 4 * 3600.0
        while self.prices_4h and self.prices_4h[0][0] < cutoff_4h:
            self.prices_4h.popleft()

        cutoff_trades = ts - max(CVD_LOOKBACK_SEC, RET_LOOKBACK_SEC)
        while self.trades and self.trades[0][0] < cutoff_trades:
            self.trades.popleft()

        # update trends
        self.trend_1h = detect_trend_from_prices(self.prices_1h)
        self.trend_4h = detect_trend_from_prices(self.prices_4h)

    def on_depth(self, ts: float, bids: List[List[str]], asks: List[List[str]]):
        self.last_ts = ts
        pb: List[Tuple[float, float]] = []
        pa: List[Tuple[float, float]] = []
        for p, q in bids[:DEPTH_LEVEL]:
            pb.append((float(p), float(q)))
        for p, q in asks[:DEPTH_LEVEL]:
            pa.append((float(p), float(q)))
        self.bids = pb
        self.asks = pa

        bid_not = sum(p * q for p, q in self.bids)
        ask_not = sum(p * q for p, q in self.asks)
        total = bid_not + ask_not
        self.ob_imbalance = (bid_not - ask_not) / total if total > 0 else 0.0

    def on_oi_snapshot(self, ts: float, oi: float):
        self.oi_history.append((ts, oi))
        cutoff = ts - OI_LOOKBACK_SEC
        while self.oi_history and self.oi_history[0][0] < cutoff:
            self.oi_history.popleft()
        self.oi_ema = ema(self.oi_ema, oi, alpha=0.3)

    # ---- Features ----

    def compute_features(self, now_ts: float) -> Dict[str, float]:
        feats: Dict[str, float] = {}

        # 5m volume
        if self.volume_history:
            vol_vals = [v for _, v in self.volume_history]
            feats["vol_sum_5m"] = sum(vol_vals)
            feats["vol_mean_5m"] = safe_div(feats["vol_sum_5m"], len(vol_vals))
        else:
            feats["vol_sum_5m"] = 0.0
            feats["vol_mean_5m"] = 0.0

        # CVD slope
        if self.cvd_history:
            t0, cvd0 = self.cvd_history[0]
            t1, cvd1 = self.cvd_history[-1]
            dt = t1 - t0
            feats["cvd_delta_5m"] = cvd1 - cvd0
            feats["cvd_slope_5m"] = safe_div(cvd1 - cvd0, dt) if dt > 0 else 0.0
        else:
            feats["cvd_delta_5m"] = 0.0
            feats["cvd_slope_5m"] = 0.0

        # 15m return
        if len(self.prices) >= 2:
            _, p0 = self.prices[0]
            _, p1 = self.prices[-1]
            feats["ret_15m"] = safe_div(p1 - p0, p0) if p0 > 0 else 0.0
        else:
            feats["ret_15m"] = 0.0

        # realized volatility & range
        if len(self.prices) >= 5:
            ps = [p for _, p in self.prices]
            rets: List[float] = []
            for i in range(1, len(ps)):
                prev = ps[i - 1]
                cur = ps[i]
                if prev > 0:
                    rets.append((cur / prev) - 1.0)
            feats["rv_15m"] = float(np.std(rets)) if rets else 0.0
            hi = max(ps)
            lo = min(ps)
            last = ps[-1]
            feats["range_15m"] = safe_div(hi - lo, last) if last > 0 else 0.0
        else:
            feats["rv_15m"] = 0.0
            feats["range_15m"] = 0.0

        feats["ob_imb"] = self.ob_imbalance

        # OI
        if len(self.oi_history) >= 2:
            t0, oi0 = self.oi_history[0]
            t1, oi1 = self.oi_history[-1]
            dt = t1 - t0
            feats["oi_chg"] = oi1 - oi0
            feats["oi_chg_rate"] = safe_div(oi1 - oi0, dt) if dt > 0 else 0.0
        else:
            feats["oi_chg"] = 0.0
            feats["oi_chg_rate"] = 0.0

        feats["last_price"] = self.last_price or 0.0
        feats["vol_ok"] = 1.0 if feats["vol_sum_5m"] >= MIN_NOTION_USDT else 0.0
        feats["price_ok"] = 1.0 if feats["last_price"] >= MIN_PRICE else 0.0

        # ---- RVOL vs own history (per symbol) ----
        vol_5m = feats["vol_sum_5m"]
        if vol_5m > 0.0 and now_ts > 0:
            self.vol_5m_history.append((now_ts, vol_5m))
            cutoff = now_ts - RVOL_HISTORY_SEC
            while self.vol_5m_history and self.vol_5m_history[0][0] < cutoff:
                self.vol_5m_history.popleft()

            vols = [v for _, v in self.vol_5m_history]
            if len(vols) >= RVOL_MIN_OBS:
                med_v, iqr_v = compute_robust_scaler(vols)
                if med_v > 0:
                    feats["vol_rvol_5m"] = vol_5m / med_v
                else:
                    feats["vol_rvol_5m"] = 1.0
            else:
                feats["vol_rvol_5m"] = 1.0
        else:
            feats["vol_rvol_5m"] = 1.0

        return feats

    # ---- Composite alpha ----

    def compute_composite_alpha(
        self,
        feats: Dict[str, float],
        scalers: Dict[str, Tuple[float, float]],
    ) -> Tuple[float, float]:
        keys = [
            "vol_sum_5m",
            "ret_15m",
            "cvd_slope_5m",
            "ob_imb",
            "oi_chg",
            "rv_15m",
            "range_15m",
        ]
        z: Dict[str, float] = {}
        for key in keys:
            x = feats.get(key, 0.0)
            med, iqr = scalers.get(key, (0.0, 1.0))
            z[key] = safe_div(x - med, iqr)
        self.feature_z = z

        raw_score = 0.0
        for key in keys:
            w = COMPOSITE_WEIGHTS.get(key, 1.0)
            raw_score += w * z[key]

        instant_alpha = math.tanh(raw_score / 4.0)

        if not isfinite(self.composite_alpha):
            self.composite_alpha = 0.0

        if self.last_ts == 0 or self.composite_alpha == 0.0:
            alpha_ewma = instant_alpha
        else:
            alpha_ewma = (
                (1.0 - SCORE_DECAY_ALPHA) * self.composite_alpha
                + SCORE_DECAY_ALPHA * instant_alpha
            )

        self.composite_alpha = alpha_ewma
        self.composite_raw = raw_score

        # classify vol regime roughly from range_15m
        rng = feats.get("range_15m", 0.0)
        if rng < 0.01:
            self.vol_regime = "calm"
        elif rng < 0.03:
            self.vol_regime = "normal"
        else:
            self.vol_regime = "spicy"

        return alpha_ewma, raw_score

    # ---- Archetypes (normalized, RVOL + regimes) ----

    def compute_archetypes(self, feats: Dict[str, float]):
        scores: Dict[str, float] = {}
        reasons: Dict[str, str] = {}

        if feats["vol_ok"] < 0.5 or feats["price_ok"] < 0.5:
            self.archetype_scores = {}
            self.archetype_reason = {}
            return

        vs = feats["vol_sum_5m"]
        cvd_slope = feats["cvd_slope_5m"]
        ret_15m = feats["ret_15m"]
        obi = feats["ob_imb"]
        oi_chg = feats["oi_chg"]
        rv_15m = feats["rv_15m"]
        range_15m = feats["range_15m"]
        rvol = feats.get("vol_rvol_5m", 1.0)

        z = self.feature_z or {}
        z_vol   = z.get("vol_sum_5m", 0.0)
        z_ret   = z.get("ret_15m", 0.0)
        z_cvd   = z.get("cvd_slope_5m", 0.0)
        z_obi   = z.get("ob_imb", 0.0)
        z_oi    = z.get("oi_chg", 0.0)
        z_rv    = z.get("rv_15m", 0.0)
        z_range = z.get("range_15m", 0.0)

        regime = self.vol_regime

        hot_rvol = rvol >= RVOL_HIGH
        extreme_rvol = rvol >= RVOL_EXTREME
        mid_rvol = 1.2 <= rvol <= RVOL_HIGH
        low_rvol = rvol <= 1.3

        # 1) zec: Breakout
        score_zec = 0.0
        r_zec: List[str] = []
        if regime in ("normal", "spicy") and hot_rvol:
            if z_ret > 1.0:
                score_zec += 1.0; r_zec.append("15m trend > peers")
            if z_cvd > 0.8:
                score_zec += 0.7; r_zec.append("buy CVD supports move")
            if z_vol > 0.8:
                score_zec += 0.7; r_zec.append("elevated 5m volume")
            if z_obi > 0.3:
                score_zec += 0.4; r_zec.append("bid-leaning orderbook")
        if score_zec > 0:
            scores["zec"] = score_zec
            reasons["zec"] = ", ".join(r_zec)

        # 2) mel: Squeeze
        score_mel = 0.0
        r_mel: List[str] = []
        if regime in ("calm", "normal") and mid_rvol:
            if abs(z_ret) < 0.3 and z_range < 0.0:
                score_mel += 1.0; r_mel.append("flat price / tight range")
            if abs(z_obi) < 0.3:
                score_mel += 0.5; r_mel.append("balanced orderbook")
            if z_oi > 0.8:
                score_mel += 0.5; r_mel.append("OI building")
        if score_mel > 0:
            scores["mel"] = score_mel
            reasons["mel"] = ", ".join(r_mel)

        # 3) fil: Trend ride
        score_fil = 0.0
        r_fil: List[str] = []
        if regime in ("normal", "spicy") and rvol >= 1.0:
            if z_ret > 0.8:
                score_fil += 1.0; r_fil.append("strong 15m uptrend")
            if z_cvd > 0.4:
                score_fil += 0.5; r_fil.append("CVD supports move")
            if z_oi > 0.5:
                score_fil += 0.5; r_fil.append("OI expanding")
        if score_fil > 0:
            scores["fil"] = score_fil
            reasons["fil"] = ", ".join(r_fil)

        # 4) moca: Bounce
        score_moca = 0.0
        r_moca: List[str] = []
        if rvol >= 1.0:
            if z_ret < -0.8 and (z_obi > 0.3 or z_cvd > 0.4):
                score_moca += 1.0; r_moca.append("sharp dip with support")
            if z_cvd > 0.6:
                score_moca += 0.5; r_moca.append("dip bought in CVD")
        if score_moca > 0:
            scores["moca"] = score_moca
            reasons["moca"] = ", ".join(r_moca)

        # 5) cookie: Fast scalp
        score_cookie = 0.0
        r_cookie: List[str] = []
        if regime in ("normal", "spicy") and hot_rvol:
            if z_ret > 1.1:
                score_cookie += 1.0; r_cookie.append("explosive 15m move")
            if abs(z_obi) > 0.6:
                score_cookie += 0.5; r_cookie.append("heavy OB skew")
            if abs(z_cvd) > 1.0:
                score_cookie += 0.5; r_cookie.append("extreme CVD imbalance")
        if score_cookie > 0:
            scores["cookie"] = score_cookie
            reasons["cookie"] = ", ".join(r_cookie)

        # 6) anime: Hype/pump
        score_anime = 0.0
        r_anime: List[str] = []
        if regime in ("normal", "spicy") and hot_rvol:
            if abs(z_ret) > 0.9:
                score_anime += 1.0; r_anime.append("strong directional move")
            if z_oi > 1.0:
                score_anime += 0.7; r_anime.append("fresh leverage build")
            if abs(z_obi) > 0.3:
                score_anime += 0.3; r_anime.append("orderbook leaning")
        if score_anime > 0:
            scores["anime"] = score_anime
            reasons["anime"] = ", ".join(r_anime)

        # 7) turbo: High-beta
        score_turbo = 0.0
        r_turbo: List[str] = []
        if regime == "spicy" and extreme_rvol:
            if z_ret > 1.2:
                score_turbo += 1.0; r_turbo.append("very strong 15m up-move")
            if z_cvd > 0.9:
                score_turbo += 0.5; r_turbo.append("aggressive taker buying")
            if z_oi > 0.6:
                score_turbo += 0.5; r_turbo.append("leveraged momentum")
        if score_turbo > 0:
            scores["turbo"] = score_turbo
            reasons["turbo"] = ", ".join(r_turbo)

        # 8) broccoli: Range coil
        score_broc = 0.0
        r_broc: List[str] = []
        if regime in ("calm", "normal") and rvol >= 1.2:
            if abs(z_ret) < 0.3 and z_range < 0.0:
                score_broc += 1.0; r_broc.append("rangey price / tight range")
            if abs(z_obi) < 0.3:
                score_broc += 0.4; r_broc.append("balanced book")
            if abs(z_oi) < 0.4:
                score_broc += 0.4; r_broc.append("stable OI (churn)")
        if score_broc > 0:
            scores["broccoli"] = score_broc
            reasons["broccoli"] = ", ".join(r_broc)

        # 9) Breakdown short
        score_br_short = 0.0
        r_br_short: List[str] = []
        if regime in ("normal", "spicy") and hot_rvol:
            if z_ret < -1.0:
                score_br_short += 1.0; r_br_short.append("strong 15m sell move vs peers")
            if z_cvd < -0.8:
                score_br_short += 0.7; r_br_short.append("strong sell CVD")
            if z_obi < -0.3:
                score_br_short += 0.4; r_br_short.append("ask-dominant book")
        if score_br_short > 0:
            scores["breakout_short"] = score_br_short
            reasons["breakout_short"] = ", ".join(r_br_short)

        # 10) Slow grind up
        score_sg_up = 0.0
        r_sg_up: List[str] = []
        if regime in ("calm", "normal") and low_rvol:
            if 0.2 < z_ret < 0.8:
                score_sg_up += 1.0; r_sg_up.append("small positive 15m return")
            if z_rv < 0.0:
                score_sg_up += 0.5; r_sg_up.append("low realized volatility")
            if z_obi > 0.0:
                score_sg_up += 0.3; r_sg_up.append("mild bid imbalance")
        if score_sg_up > 0:
            scores["slow_grind_up"] = score_sg_up
            reasons["slow_grind_up"] = ", ".join(r_sg_up)

        # 11) Slow grind down
        score_sg_dn = 0.0
        r_sg_dn: List[str] = []
        if regime in ("calm", "normal") and low_rvol:
            if -0.8 < z_ret < -0.2:
                score_sg_dn += 1.0; r_sg_dn.append("small negative 15m return")
            if z_rv < 0.0:
                score_sg_dn += 0.5; r_sg_dn.append("low realized volatility")
            if z_obi < 0.0:
                score_sg_dn += 0.3; r_sg_dn.append("mild ask imbalance")
        if score_sg_dn > 0:
            scores["slow_grind_down"] = score_sg_dn
            reasons["slow_grind_down"] = ", ".join(r_sg_dn)

        # 12) Capitulation dump
        score_cap = 0.0
        r_cap: List[str] = []
        if regime == "spicy" and extreme_rvol:
            if z_ret < -1.3:
                score_cap += 1.2; r_cap.append("huge 15m down move vs peers")
            if z_cvd < -1.0:
                score_cap += 0.7; r_cap.append("aggressive sell CVD")
            if z_oi < -1.0:
                score_cap += 0.7; r_cap.append("OI flushing")
        if score_cap > 0:
            scores["capitulation"] = score_cap
            reasons["capitulation"] = ", ".join(r_cap)

        # 13) Bid wall
        score_bw = 0.0
        r_bw: List[str] = []
        if abs(z_ret) < 0.3 and z_obi > 0.8 and rvol >= 1.0:
            score_bw += 1.0; r_bw.append("big bid imbalance holding price")
        if score_bw > 0:
            scores["bid_wall"] = score_bw
            reasons["bid_wall"] = ", ".join(r_bw)

        # 14) Ask wall
        score_aw = 0.0
        r_aw: List[str] = []
        if abs(z_ret) < 0.3 and z_obi < -0.8 and rvol >= 1.0:
            score_aw += 1.0; r_aw.append("big ask imbalance capping price")
        if score_aw > 0:
            scores["ask_wall"] = score_aw
            reasons["ask_wall"] = ", ".join(r_aw)

        # 15) OI build long
        score_oi_bl = 0.0
        r_oi_bl: List[str] = []
        if z_oi > 0.8 and z_ret >= -0.3:
            score_oi_bl += 1.0; r_oi_bl.append("ΔOI up with non-negative price")
            if z_cvd > 0.0:
                score_oi_bl += 0.5; r_oi_bl.append("CVD not fighting longs")
        if score_oi_bl > 0:
            scores["oi_build_long"] = score_oi_bl
            reasons["oi_build_long"] = ", ".join(r_oi_bl)

        # 16) OI build short
        score_oi_bs = 0.0
        r_oi_bs: List[str] = []
        if z_oi > 0.8 and z_ret <= 0.3:
            score_oi_bs += 1.0; r_oi_bs.append("ΔOI up with non-positive price")
            if z_cvd < 0.0:
                score_oi_bs += 0.5; r_oi_bs.append("CVD leaning to sells")
        if score_oi_bs > 0:
            scores["oi_build_short"] = score_oi_bs
            reasons["oi_build_short"] = ", ".join(r_oi_bs)

        # 17) OI flush
        score_oi_fl = 0.0
        r_oi_fl: List[str] = []
        if z_oi < -0.8 and rvol >= 1.0:
            score_oi_fl += 1.0; r_oi_fl.append("big negative ΔOI")
            if abs(z_ret) > 0.8 and z_vol > 0.0:
                score_oi_fl += 0.5; r_oi_fl.append("strong move with volume")
        if score_oi_fl > 0:
            scores["oi_flush"] = score_oi_fl
            reasons["oi_flush"] = ", ".join(r_oi_fl)

        # 18) CVD divergence up
        score_cdu = 0.0
        r_cdu: List[str] = []
        if z_cvd > 0.8 and z_ret <= 0.3:
            score_cdu += 1.0; r_cdu.append("CVD rising while price flat/down")
        if score_cdu > 0:
            scores["cvd_div_up"] = score_cdu
            reasons["cvd_div_up"] = ", ".join(r_cdu)

        # 19) CVD divergence down
        score_cdd = 0.0
        r_cdd: List[str] = []
        if z_cvd < -0.8 and z_ret >= -0.3:
            score_cdd += 1.0; r_cdd.append("CVD falling while price flat/up")
        if score_cdd > 0:
            scores["cvd_div_down"] = score_cdd
            reasons["cvd_div_down"] = ", ".join(r_cdd)

        # 20) Liquidity vacuum
        score_lv = 0.0
        r_lv: List[str] = []
        if abs(z_ret) > 0.9 and z_vol <= 0.0 and abs(z_obi) < 0.3:
            score_lv += 1.0; r_lv.append("big move on modest volume & thin book")
        if score_lv > 0:
            scores["liquidity_vacuum"] = score_lv
            reasons["liquidity_vacuum"] = ", ".join(r_lv)

        # 21) Mean reversion band
        score_mrb = 0.0
        r_mrb: List[str] = []
        if regime == "calm" and low_rvol:
            if abs(z_ret) < 0.2 and z_range < -0.3:
                score_mrb += 1.0; r_mrb.append("tiny move & tight range")
            if abs(z_cvd) < 0.2 and abs(z_obi) < 0.2:
                score_mrb += 0.5; r_mrb.append("balanced CVD & book")
        if score_mrb > 0:
            scores["mean_revert_band"] = score_mrb
            reasons["mean_revert_band"] = ", ".join(r_mrb)

        # 22) Micro chop
        score_mc = 0.0
        r_mc: List[str] = []
        if regime == "calm" and low_rvol and z_vol < 0.0 and z_rv < -0.3 and z_range < -0.3:
            score_mc += 1.0; r_mc.append("ultra low vol & volume")
        if score_mc > 0:
            scores["micro_chop"] = score_mc
            reasons["micro_chop"] = ", ".join(r_mc)

        # 23) Shock up
        score_su = 0.0
        r_su: List[str] = []
        if regime == "spicy" and extreme_rvol:
            if z_ret > 1.5:
                score_su += 1.2; r_su.append("huge positive 15m return vs peers")
            if z_cvd > 1.3:
                score_su += 0.8; r_su.append("very strong buy CVD")
        if score_su > 0:
            scores["shock_up"] = score_su
            reasons["shock_up"] = ", ".join(r_su)

        # 24) Shock down
        score_sd = 0.0
        r_sd: List[str] = []
        if regime == "spicy" and extreme_rvol:
            if z_ret < -1.5:
                score_sd += 1.2; r_sd.append("huge negative 15m return vs peers")
            if z_cvd < -1.3:
                score_sd += 0.8; r_sd.append("very strong sell CVD")
        if score_sd > 0:
            scores["shock_down"] = score_sd
            reasons["shock_down"] = ", ".join(r_sd)

        self.archetype_scores = scores
        self.archetype_reason = reasons

# ======================================================================
# Magnetic levels / zones
# ======================================================================

@dataclass
class Level:
    price: float
    weight: float
    kind: str  # e.g. "swing_high", "fib_1.272", "vwap", "hvn", ...


@dataclass
class Zone:
    center: float
    score: float
    low: float
    high: float
    kinds: List[str]

class TargetCalculator:
    """
    Builds real "magnetic zones" from:
    - Swing highs/lows
    - Fib extensions (1.272/1.414/1.618) of local swing range
    - Local swing pivots
    - Intraday VWAP + bands
    - Local volume-profile HVN/LVN
    Then clusters them into zones and picks SL / TP1/TP2/TP3 + RR.
    """

    def __init__(
        self,
        cluster_eps: float = 0.003,  # ~0.3% cluster width
        near_zone_eps: float = 0.01, # price must be within ~1% of strong zone
        min_zone_score: float = 1.5,
    ):
        self.cluster_eps = cluster_eps
        self.near_zone_eps = near_zone_eps
        self.min_zone_score = min_zone_score

    def _build_levels(self, st: SymbolState, feats: Dict[str, float]) -> List[Level]:
        levels: List[Level] = []
        price = feats.get("last_price", 0.0)
        if price <= 0:
            return levels

        # 1) Recent swing high/low from price window
        ps = [p for _, p in st.prices]
        if len(ps) >= 5:
            hi = max(ps)
            lo = min(ps)
            last = ps[-1]
            rng = hi - lo

            if hi > 0:
                levels.append(Level(hi, 1.5, "swing_high"))
            if lo > 0:
                levels.append(Level(lo, 1.5, "swing_low"))

            # 2) Fib extensions around that range
            if rng > 0 and isfinite(rng):
                for f in (1.272, 1.414, 1.618):
                    ext_up = hi + rng * (f - 1.0)
                    if ext_up > 0:
                        levels.append(Level(ext_up, 1.0, f"fib_{f:.3f}_up"))
                    ext_dn = lo - rng * (f - 1.0)
                    if ext_dn > 0:
                        levels.append(Level(ext_dn, 1.0, f"fib_{f:.3f}_dn"))

                # 3) Local swing pivot style (intraday)
                pivot = (hi + lo + last) / 3.0
                levels.append(Level(pivot, 1.2, "pivot_local"))
                r1 = 2 * pivot - lo
                s1 = 2 * pivot - hi
                if r1 > 0:
                    levels.append(Level(r1, 1.0, "pivot_R1_local"))
                if s1 > 0:
                    levels.append(Level(s1, 1.0, "pivot_S1_local"))

        # 4) Intraday VWAP + bands (from true trades)
        if st.trades:
            prices = np.array([p for _, p, _, _ in st.trades], dtype=float)
            qtys = np.array([q for _, _, q, _ in st.trades], dtype=float)
            tot_vol = float(np.sum(qtys))
            if tot_vol > 0:
                vwap = float(np.sum(prices * qtys) / tot_vol)
                levels.append(Level(vwap, 1.3, "vwap"))

                diff = prices - vwap
                band_std = float(np.std(diff))
                if band_std > 0 and isfinite(band_std):
                    for k, w in ((1.0, 0.9), (2.0, 0.7)):
                        up = vwap + k * band_std
                        dn = vwap - k * band_std
                        if up > 0:
                            levels.append(Level(up, w, f"vwap+{k}σ"))
                        if dn > 0:
                            levels.append(Level(dn, w, f"vwap-{k}σ"))

        # 5) Tiny local volume-profile (HVN/LVN) from trades
        if st.trades:
            prices = np.array([p for _, p, _, _ in st.trades], dtype=float)
            qtys = np.array([q for _, _, q, _ in st.trades], dtype=float)
            pmin, pmax = float(prices.min()), float(prices.max())
            if pmax > pmin and isfinite(pmin) and isfinite(pmax):
                bins = 20
                hist, edges = np.histogram(prices, bins=bins, weights=qtys)
                if np.any(hist > 0):
                    # HVN: top 2 bins
                    indices = np.argsort(hist)[::-1]
                    hvn_added = 0
                    lvn_added = 0
                    for idx in indices:
                        if hist[idx] <= 0:
                            continue
                        center = (edges[idx] + edges[idx + 1]) / 2.0
                        if center <= 0:
                            continue
                        if hvn_added < 2:
                            levels.append(Level(center, 1.4, "hvn_local"))
                            hvn_added += 1
                        else:
                            break
                    # LVN: smallest non-zero volumes
                    non_zero_idx = np.where(hist > 0)[0]
                    if len(non_zero_idx) > 0:
                        small_idx = non_zero_idx[np.argsort(hist[non_zero_idx])]
                        for idx in small_idx:
                            if lvn_added >= 2:
                                break
                            center = (edges[idx] + edges[idx + 1]) / 2.0
                            if center <= 0:
                                continue
                            levels.append(Level(center, 1.1, "lvn_local"))
                            lvn_added += 1

        return levels

    def _cluster_levels(self, levels: List[Level]) -> List[Zone]:
        if not levels:
            return []

        levels_sorted = sorted(levels, key=lambda l: l.price)
        zones: List[Zone] = []

        for lvl in levels_sorted:
            if lvl.price <= 0 or not isfinite(lvl.price):
                continue

            if not zones:
                zones.append(
                    Zone(
                        center=lvl.price,
                        score=lvl.weight,
                        low=lvl.price,
                        high=lvl.price,
                        kinds=[lvl.kind],
                    )
                )
                continue

            z = zones[-1]
            base = z.center if z.center > 0 else 1.0
            rel = abs(lvl.price - z.center) / base
            if rel <= self.cluster_eps:
                z.center = z.center + (lvl.price - z.center) * (lvl.weight / (z.score + lvl.weight))
                z.score += lvl.weight
                z.low = min(z.low, lvl.price)
                z.high = max(z.high, lvl.price)
                z.kinds.append(lvl.kind)
            else:
                zones.append(
                    Zone(
                        center=lvl.price,
                        score=lvl.weight,
                        low=lvl.price,
                        high=lvl.price,
                        kinds=[lvl.kind],
                    )
                )

        return zones

    def compute_targets(
        self,
        st: SymbolState,
        feats: Dict[str, float],
        direction: str,
    ) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], Optional[float], Optional[float], Optional[str]]:
        """
        Returns: entry, sl, tp1, tp2, tp3, rr, zone_debug
        If unable to build sensible zones or not near a strong zone, returns all None.
        """
        price = feats.get("last_price", 0.0)
        if price <= 0 or direction not in ("long", "short"):
            return None, None, None, None, None, None, None

        rng_15m = feats.get("range_15m", 0.0)
        rv_15m = feats.get("rv_15m", 0.0)

        # Approximate noise floor: half the 15m range or ~3x realized vol,
        # but never below MIN_SL_PCT.
        noise_floor_pct = max(
            MIN_SL_PCT,
            rng_15m * NOISE_RANGE_MULT if rng_15m > 0 else 0.0,
            rv_15m * NOISE_RV_MULT if rv_15m > 0 else 0.0,
        )

        def _apply_risk_guards(entry, sl, tp1, tp2, tp3, direction, noise_floor_pct_local):
            price_local = entry
            if direction == "long":
                risk = entry - sl
                reward = tp1 - entry
            else:
                risk = sl - entry
                reward = entry - tp1

            if risk <= 0 or reward <= 0:
                return None

            # Skip trades where SL is inside microscopic tick noise.
            if abs(risk) / max(price_local, 1e-8) < TOO_CLOSE_EPS:
                return None

            # Noise-aware minimum risk: max of % config and volatility-based noise floor.
            min_risk = max(
                price_local * MIN_SL_PCT,
                price_local * noise_floor_pct_local,
            )
            max_risk = price_local * MAX_SL_PCT

            # If risk is too tiny, push SL further away to at least min_risk.
            if risk < min_risk:
                if direction == "long":
                    sl = entry - min_risk
                else:
                    sl = entry + min_risk
                risk = min_risk

            # If risk is too huge, skip the trade altogether.
            if risk > max_risk:
                return None

            if direction == "long":
                reward = tp1 - entry
            else:
                reward = entry - tp1

            if reward <= 0:
                return None

            rr = safe_div(reward, risk) if risk > 0 else None
            if rr is None or rr < MIN_RR:
                return None

            # Cap fantasy RR by pulling TP1 towards entry if needed.
            if rr > MAX_RR_DEFAULT:
                if direction == "long":
                    tp1 = entry + MAX_RR_DEFAULT * risk
                    if tp2 < tp1:
                        tp2 = tp1
                    if tp3 < tp2:
                        tp3 = tp2
                else:
                    tp1 = entry - MAX_RR_DEFAULT * risk
                    if tp2 > tp1:
                        tp2 = tp1
                    if tp3 > tp2:
                        tp3 = tp2

                if direction == "long":
                    reward = tp1 - entry
                else:
                    reward = entry - tp1
                rr = safe_div(reward, risk) if risk > 0 else None

            if rr is None or rr < MIN_RR:
                return None

            return sl, tp1, tp2, tp3, rr

        levels = self._build_levels(st, feats)
        if not levels:
            # last-resort fallback: simple % levels
            entry = price
            if direction == "long":
                sl = price * (1.0 - SL_PCT_FALLBACK)
                tp1 = price * (1.0 + TP_PCT_FALLBACK)
                tp2 = price * (1.0 + 2 * TP_PCT_FALLBACK)
                tp3 = price * (1.0 + 3 * TP_PCT_FALLBACK)
            else:
                sl = price * (1.0 + SL_PCT_FALLBACK)
                tp1 = price * (1.0 - TP_PCT_FALLBACK)
                tp2 = price * (1.0 - 2 * TP_PCT_FALLBACK)
                tp3 = price * (1.0 - 3 * TP_PCT_FALLBACK)

            guarded = _apply_risk_guards(entry, sl, tp1, tp2, tp3, direction, noise_floor_pct)
            if guarded is None:
                return None, None, None, None, None, None, None
            sl, tp1, tp2, tp3, rr = guarded
            return entry, sl, tp1, tp2, tp3, rr, "fallback_%_levels"

        zones = self._cluster_levels(levels)
        if not zones:
            return None, None, None, None, None, None, None

        zones_by_score = sorted(zones, key=lambda z: z.score, reverse=True)
        near_zone: Optional[Zone] = None
        for z in zones_by_score:
            base = price if price > 0 else 1.0
            rel = abs(z.center - price) / base
            if rel <= self.near_zone_eps and z.score >= self.min_zone_score:
                near_zone = z
                break

        if near_zone is None:
            return None, None, None, None, None, None, None

        zones_above = [z for z in zones if z.center > price]
        zones_below = [z for z in zones if z.center < price]

        zones_above = sorted(zones_above, key=lambda z: z.score, reverse=True)
        zones_below = sorted(zones_below, key=lambda z: z.score, reverse=True)

        entry = price

        if direction == "long":
            if not zones_above or not zones_below:
                return None, None, None, None, None, None, None
            sl_zone = zones_below[0]
            sl = sl_zone.center
            tps = zones_above[:3]
            tp_prices = [z.center for z in tps]
            if len(tp_prices) < 1:
                return None, None, None, None, None, None, None
            tp1 = tp_prices[0]
            tp2 = tp_prices[1] if len(tp_prices) > 1 else tp1
            tp3 = tp_prices[2] if len(tp_prices) > 2 else tp2
        else:
            if not zones_above or not zones_below:
                return None, None, None, None, None, None, None
            sl_zone = zones_above[0]
            sl = sl_zone.center
            tps = zones_below[:3]
            tp_prices = [z.center for z in tps]
            if len(tp_prices) < 1:
                return None, None, None, None, None, None, None
            tp1 = tp_prices[0]
            tp2 = tp_prices[1] if len(tp_prices) > 1 else tp1
            tp3 = tp_prices[2] if len(tp_prices) > 2 else tp2

        guarded = _apply_risk_guards(entry, sl, tp1, tp2, tp3, direction, noise_floor_pct)
        if guarded is None:
            return None, None, None, None, None, None, None
        sl, tp1, tp2, tp3, rr = guarded

        zone_debug = f"zone@{near_zone.center:.6g} ({'+'.join(sorted(set(near_zone.kinds)))})"
        return entry, sl, tp1, tp2, tp3, rr, zone_debug

# ======================================================================
# Trade model + SQLite
# ======================================================================

@dataclass
class Trade:
    symbol: str
    side: str
    entry: float
    sl: float
    tp1: float
    tp2: float
    tp3: float
    rr: float
    primary: str
    setups: str
    reason: str
    leverage: float = 1.0
    status: str = "OPEN"   # OPEN / CLOSED
    verdict: str = "-"     # TP1/TP2/TP3/SL
    ts_open: float = field(default_factory=now_s)
    ts_close: float = 0.0
    exit_price: float = 0.0
    db_id: Optional[int] = None

# ======================================================================
# Binance Futures Connector (async I/O + safe REST)
# ======================================================================

class BinanceFuturesConnector:
    FUTURES_REST = BINANCE_FUTURES_REST

    def __init__(
        self,
        session: aiohttp.ClientSession,
        states: Dict[str, SymbolState],
        limiter: RateLimiter,
    ):
        self.session = session
        self.states = states
        self.limiter = limiter
        self.logger = logging.getLogger("binance_connector")
        self.trade_ws_tasks: List[asyncio.Task] = []
        self.depth_ws_tasks: List[asyncio.Task] = []
        self.oi_task: Optional[asyncio.Task] = None
        self.ws_trade_shards = TRADE_WS_SHARDS
        self.ws_depth_shards = DEPTH_WS_SHARDS

    # ---- Safe REST wrapper -------------------------------------------------

    async def safe_request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, str]] = None,
        max_retries: int = 5,
    ):
        """
        Token-bucket limited, 418/429-aware REST wrapper.

        - Honors Retry-After when present.
        - Otherwise uses exponential backoff with jitter.
        - Logs X-MBX-USED-WEIGHT-* for visibility.
        """
        url = f"{self.FUTURES_REST}{path}"
        base_delay = 1.0

        for attempt in range(max_retries):
            await self.limiter.acquire()
            try:
                async with self.session.request(method, url, params=params, timeout=10) as resp:
                    status = resp.status
                    headers = resp.headers

                    # Log weight usage if exposed
                    used_weight = (
                        headers.get("X-MBX-USED-WEIGHT-1M")
                        or headers.get("X-MBX-USED-WEIGHT")
                    )
                    if used_weight:
                        self.logger.debug("Binance weight header: %s", used_weight)

                    if status in (418, 429):
                        retry_after = headers.get("Retry-After")
                        if retry_after is not None:
                            try:
                                delay = float(retry_after)
                            except ValueError:
                                delay = base_delay * (2 ** attempt)
                        else:
                            delay = base_delay * (2 ** attempt)
                        delay += random.uniform(0.0, 1.0)
                        self.logger.warning(
                            "Rate limit (%s) on %s %s – sleeping %.2fs",
                            status, method, path, delay,
                        )
                        await asyncio.sleep(delay)
                        continue

                    if status >= 500:
                        delay = base_delay * (2 ** attempt) + random.uniform(0.0, 1.0)
                        self.logger.warning(
                            "Server error %s on %s %s – retry in %.2fs",
                            status, method, path, delay,
                        )
                        await asyncio.sleep(delay)
                        continue

                    resp.raise_for_status()
                    return await resp.json()

            except asyncio.CancelledError:
                raise
            except Exception as e:
                delay = base_delay * (2 ** attempt) + random.uniform(0.0, 1.0)
                self.logger.warning(
                    "Error on %s %s: %s – retry in %.2fs",
                    method, path, e, delay,
                )
                await asyncio.sleep(delay)

        self.logger.error(
            "safe_request: giving up on %s %s after %d attempts",
            method, path, max_retries,
        )
        raise RuntimeError(f"safe_request failed for {method} {path}")

    # ---- Universe / WS / OI -----------------------------------------------

    async def fetch_futures_universe(self) -> List[str]:
        data = await self.safe_request("GET", "/fapi/v1/exchangeInfo")
        symbols: List[str] = []
        for sym in data.get("symbols", []):
            if sym.get("contractType") == "PERPETUAL" and sym.get("status") == "TRADING":
                if sym.get("quoteAsset") == "USDT":
                    symbols.append(sym["symbol"])
        symbols = symbols[:UNIVERSE_LIMIT]
        self.logger.info("Fetched %d futures symbols", len(symbols))
        return symbols

    async def start(self, symbols: List[str]):
        self.logger.info("BinanceFuturesConnector starting")

        chunk_size_trade = math.ceil(len(symbols) / self.ws_trade_shards)
        chunk_size_depth = math.ceil(len(symbols) / self.ws_depth_shards)

        for i in range(self.ws_trade_shards):
            chunk = symbols[i * chunk_size_trade:(i + 1) * chunk_size_trade]
            if not chunk:
                continue
            t = asyncio.create_task(self._trade_ws_loop(chunk), name=f"trade_ws_{i}")
            self.trade_ws_tasks.append(t)

        for i in range(self.ws_depth_shards):
            chunk = symbols[i * chunk_size_depth:(i + 1) * chunk_size_depth]
            if not chunk:
                continue
            t = asyncio.create_task(self._depth_ws_loop(chunk), name=f"depth_ws_{i}")
            self.depth_ws_tasks.append(t)

        self.oi_task = asyncio.create_task(self._oi_poller(), name="oi_poller")

    async def stop(self):
        for t in self.trade_ws_tasks + self.depth_ws_tasks:
            t.cancel()
        if self.oi_task:
            self.oi_task.cancel()

    async def _trade_ws_loop(self, symbols: List[str]):
        streams = "/".join(f"{s.lower()}@trade" for s in symbols)
        url = f"{BINANCE_FUTURES_WS}?streams={streams}"
        while True:
            try:
                self.logger.info("Opening trade WS ...")
                async with self.session.ws_connect(url, heartbeat=20) as ws:
                    await self._consume_trade_ws(ws)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger.warning("Trade WS error: %s -- reconnecting after jitter", e)
                await asyncio.sleep(1.0 + random.random() * 2.0)

    async def _consume_trade_ws(self, ws: aiohttp.ClientWebSocketResponse):
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                data = json.loads(msg.data)
                payload = data.get("data")
                if not payload:
                    continue
                symbol = payload["s"]
                price = float(payload["p"])
                qty = float(payload["q"])
                is_buyer_maker = payload["m"]
                ts = payload.get("T", now_ms()) / 1000.0
                side = "sell" if is_buyer_maker else "buy"
                state = self.states.get(symbol)
                if state:
                    state.on_trade(ts, price, qty, side)
            elif msg.type == aiohttp.WSMsgType.ERROR:
                break

    async def _depth_ws_loop(self, symbols: List[str]):
        streams = "/".join(f"{s.lower()}@depth{DEPTH_LEVEL}@100ms" for s in symbols)
        url = f"{BINANCE_FUTURES_WS}?streams={streams}"
        while True:
            try:
                self.logger.info("Opening depth WS ...")
                async with self.session.ws_connect(url, heartbeat=20) as ws:
                    await self._consume_depth_ws(ws)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger.warning("Depth WS error: %s -- reconnecting after jitter", e)
                await asyncio.sleep(1.0 + random.random() * 2.0)

    async def _consume_depth_ws(self, ws: aiohttp.ClientWebSocketResponse):
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                data = json.loads(msg.data)
                payload = data.get("data")
                if not payload:
                    continue
                symbol = payload["s"]
                bids = payload.get("b", [])
                asks = payload.get("a", [])
                ts = payload.get("E", now_ms()) / 1000.0
                state = self.states.get(symbol)
                if state:
                    state.on_depth(ts, bids, asks)
            elif msg.type == aiohttp.WSMsgType.ERROR:
                break

    async def _oi_poller(self):
        while True:
            try:
                await asyncio.sleep(OI_POLL_INTERVAL_SEC)
                await self._poll_oi()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger.error("OI poll outer loop error: %s", e)

    async def _poll_oi(self):
        for symbol, state in self.states.items():
            try:
                data = await self.safe_request(
                    "GET", "/fapi/v1/openInterest", params={"symbol": symbol}
                )
            except Exception as e:
                self.logger.warning("openInterest request failed for %s: %s", symbol, e)
                continue
            oi = float(data.get("openInterest", 0.0))
            ts = now_s()
            state.on_oi_snapshot(ts, oi)

# ======================================================================
# App / TUI / Trades + Metrics
# ======================================================================

class BreakoutArchetypeApp:
    def __init__(self):
        self.console = Console()
        self.states: Dict[str, SymbolState] = {}
        self.connector: Optional[BinanceFuturesConnector] = None
        self.session: Optional[aiohttp.ClientSession] = None
        self.limiter: Optional[RateLimiter] = None

        # trades
        self.conn: Optional[sqlite3.Connection] = None
        self.open_trades: Dict[str, Trade] = {}
        self.closed_trades: Deque[Trade] = deque(maxlen=MAX_CLOSED_SHOWN)

        # magnetic target engine
        self.target_calc = TargetCalculator()

        # metrics / HTTP
        self.metrics_app: Optional[web.Application] = None
        self.metrics_runner: Optional[web.AppRunner] = None
        self.metrics_site: Optional[web.TCPSite] = None

    async def start(self):
        logger.info("Application starting")
        self.session = aiohttp.ClientSession()
        self.limiter = RateLimiter(REST_MAX_REQ_PER_SEC, REST_BUCKET_CAPACITY)
        self.connector = BinanceFuturesConnector(self.session, self.states, self.limiter)

        self._init_db()

        symbols = await self.connector.fetch_futures_universe()
        for s in symbols:
            self.states[s] = SymbolState(symbol=s)
        logger.info("Prepared %d futures symbols for tracking", len(symbols))

        await self.connector.start(symbols)

        # metrics HTTP server
        await self._start_metrics_server()

        with Live(self._build_view(), console=self.console, refresh_per_second=4) as live:
            while True:
                await asyncio.sleep(TABLE_REFRESH_SEC)
                live.update(self._build_view())

    async def stop(self):
        if self.connector:
            await self.connector.stop()
        if self.session:
            await self.session.close()
        if self.conn:
            self.conn.close()
        if self.metrics_runner:
            await self.metrics_runner.cleanup()

    # ---- metrics HTTP server ----

    async def _start_metrics_server(self):
        self.metrics_app = web.Application()
        self.metrics_app.router.add_get("/metrics", self.handle_metrics)
        self.metrics_runner = web.AppRunner(self.metrics_app)
        await self.metrics_runner.setup()
        self.metrics_site = web.TCPSite(self.metrics_runner, "127.0.0.1", 8001)
        await self.metrics_site.start()
        logger.info("Metrics server started at http://127.0.0.1:8001/metrics")

    async def handle_metrics(self, request: web.Request) -> web.Response:
        open_count = len(self.open_trades)
        closed_count = len(self.closed_trades)
        txt = (
            "# Breakout Archetype metrics\n"
            f"breakout_open_trades {open_count}\n"
            f"breakout_closed_trades {closed_count}\n"
        )
        return web.Response(text=txt, content_type="text/plain")

    # ---- SQLite ----

    def _init_db(self):
        self.conn = sqlite3.connect(TRADES_DB_PATH)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_open REAL,
                ts_close REAL,
                symbol TEXT,
                side TEXT,
                entry REAL,
                sl REAL,
                tp1 REAL,
                tp2 REAL,
                tp3 REAL,
                exit_price REAL,
                rr REAL,
                status TEXT,
                verdict TEXT,
                primary_arch TEXT,
                setups TEXT,
                reason TEXT
            )
            """
        )
        self.conn.commit()

    def _insert_trade(self, trade: Trade) -> int:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO trades (
                ts_open, ts_close, symbol, side, entry, sl, tp1, tp2, tp3,
                exit_price, rr, status, verdict, primary_arch, setups, reason
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                trade.ts_open,
                trade.ts_close,
                trade.symbol,
                trade.side,
                trade.entry,
                trade.sl,
                trade.tp1,
                trade.tp2,
                trade.tp3,
                trade.exit_price,
                trade.rr,
                trade.status,
                trade.verdict,
                trade.primary,
                trade.setups,
                trade.reason,
            ),
        )
        self.conn.commit()
        return cur.lastrowid

    def _update_trade_db(self, trade: Trade):
        if trade.db_id is None:
            return
        self.conn.execute(
            """
            UPDATE trades
            SET ts_close = ?, exit_price = ?, rr = ?, status = ?, verdict = ?
            WHERE id = ?
            """,
            (
                trade.ts_close,
                trade.exit_price,
                trade.rr,
                trade.status,
                trade.verdict,
                trade.db_id,
            ),
        )
        self.conn.commit()

    # ---- Trade lifecycle ----

    def _maybe_open_trade(
        self,
        sym: str,
        side: str,
        entry: Optional[float],
        sl: Optional[float],
        tp1: Optional[float],
        tp2: Optional[float],
        tp3: Optional[float],
        rr: Optional[float],
        primary_label: str,
        setups_label: str,
        reason: str,
        leverage: float,
    ):
        if (
            sym in self.open_trades or
            side not in ("long", "short") or
            entry is None or sl is None or tp1 is None or
            rr is None or rr < MIN_RR
        ):
            return

        trade = Trade(
            symbol=sym,
            side=side,
            entry=entry,
            sl=sl,
            tp1=tp1,
            tp2=tp2 if tp2 is not None else tp1,
            tp3=tp3 if tp3 is not None else tp1,
            rr=rr,
            primary=primary_label,
            setups=setups_label,
            reason=reason,
            leverage=leverage,
        )
        trade_id = self._insert_trade(trade)
        trade.db_id = trade_id
        self.open_trades[sym] = trade

        logger.info(json.dumps({
            "event": "trade_open",
            "symbol": sym,
            "side": side,
            "entry": entry,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
            "rr": rr,
            "primary": primary_label,
            "setups": setups_label,
            "reason": reason,
            "leverage": leverage,
            "ts_open": trade.ts_open,
        }))

    def _update_trade_with_price(self, sym: str, price: float):
        trade = self.open_trades.get(sym)
        if not trade or price <= 0:
            return

        side = trade.side
        sl = trade.sl
        tp1 = trade.tp1
        tp2 = trade.tp2
        tp3 = trade.tp3

        verdict = None

        if side == "long":
            if price <= sl:
                verdict = "SL"
            elif price >= tp3:
                verdict = "TP3"
            elif price >= tp2:
                verdict = "TP2"
            elif price >= tp1:
                verdict = "TP1"
        else:  # short
            if price >= sl:
                verdict = "SL"
            elif price <= tp3:
                verdict = "TP3"
            elif price <= tp2:
                verdict = "TP2"
            elif price <= tp1:
                verdict = "TP1"

        if verdict is None:
            return

        trade.status = "CLOSED"
        trade.verdict = verdict
        trade.ts_close = now_s()
        trade.exit_price = price
        self._update_trade_db(trade)

        self.closed_trades.appendleft(trade)
        self.open_trades.pop(sym, None)

        logger.info(json.dumps({
            "event": "trade_close",
            "symbol": trade.symbol,
            "side": trade.side,
            "entry": trade.entry,
            "exit": trade.exit_price,
            "sl": trade.sl,
            "tp1": trade.tp1,
            "tp2": trade.tp2,
            "tp3": trade.tp3,
            "rr": trade.rr,
            "verdict": verdict,
            "leverage": trade.leverage,
            "ts_open": trade.ts_open,
            "ts_close": trade.ts_close,
        }))

    # ---- View ----

    def _build_view(self):
        setups_panel, _ = self._build_setups_table()
        trades_panel = self._build_trades_panel()
        legend_panel = self._build_legend_panel()  # NEW

        layout = Table.grid(padding=(0, 1), expand=True)
        layout.add_row(setups_panel)
        layout.add_row(trades_panel)
        layout.add_row(legend_panel)  # NEW
        return layout

    def _build_setups_table(self) -> Tuple[Panel, List[str]]:
        now_ts = now_s()

        feats_by_symbol: Dict[str, Dict[str, float]] = {}
        vol_vals: List[float] = []
        ret_vals: List[float] = []
        cvd_vals: List[float] = []
        obi_vals: List[float] = []
        oi_vals: List[float] = []
        rv_vals: List[float] = []
        range_vals: List[float] = []

        for sym, st in self.states.items():
            feats = st.compute_features(now_ts)
            feats_by_symbol[sym] = feats
            vol_vals.append(feats.get("vol_sum_5m", 0.0))
            ret_vals.append(feats.get("ret_15m", 0.0))
            cvd_vals.append(feats.get("cvd_slope_5m", 0.0))
            obi_vals.append(feats.get("ob_imb", 0.0))
            oi_vals.append(feats.get("oi_chg", 0.0))
            rv_vals.append(feats.get("rv_15m", 0.0))
            range_vals.append(feats.get("range_15m", 0.0))

        scalers = {
            "vol_sum_5m": compute_robust_scaler(vol_vals),
            "ret_15m": compute_robust_scaler(ret_vals),
            "cvd_slope_5m": compute_robust_scaler(cvd_vals),
            "ob_imb": compute_robust_scaler(obi_vals),
            "oi_chg": compute_robust_scaler(oi_vals),
            "rv_15m": compute_robust_scaler(rv_vals),
            "range_15m": compute_robust_scaler(range_vals),
        }

        rows: List[Tuple] = []
        active_syms: List[str] = []

        for sym, st in self.states.items():
            feats = feats_by_symbol[sym]
            price = feats.get("last_price", 0.0)

            if price > 0:
                self._update_trade_with_price(sym, price)

            # IMPORTANT: compute composite (and feature_z) BEFORE archetypes
            alpha_ewma, raw_score = st.compute_composite_alpha(feats, scalers)
            st.compute_archetypes(feats)

            if raw_score < GLOBAL_RAW_SCORE_GATE:
                continue
            if alpha_ewma < ALPHA_THRESHOLD:
                continue
            if not st.archetype_scores:
                continue

            sorted_arch = sorted(
                st.archetype_scores.items(),
                key=lambda kv: kv[1],
                reverse=True,
            )
            primary_key, primary_score = sorted_arch[0]
            primary_label, _ = ARCHETYPE_INFO.get(primary_key, (primary_key, primary_key))
            direction = ARCHETYPE_DIRECTION.get(primary_key, "neutral")
            if direction == "neutral":
                continue

            # MTF filter
            if not passes_mtf_filters(st, direction):
                continue

            active_labels: List[str] = []
            for key, score in sorted_arch:
                if score <= 0:
                    continue
                short_label, _ = ARCHETYPE_INFO.get(key, (key, key))
                active_labels.append(short_label)
                if len(active_labels) >= 4:
                    break
            setups_label = ", ".join(active_labels)

            vol5m = feats.get("vol_sum_5m", 0.0)
            ret15 = feats.get("ret_15m", 0.0)
            obi = feats.get("ob_imb", 0.0)
            oi_chg = feats.get("oi_chg", 0.0)

            price_spark = ""
            if st.prices:
                closes = [p for _, p in st.prices]
                price_spark = sparkline(closes, length=8) if closes else ""

            reason_primary = st.archetype_reason.get(primary_key, "")

            # --- Magnetic target engine ---
            entry, sl, tp1, tp2, tp3, rr, zone_debug = self.target_calc.compute_targets(
                st, feats, direction
            )
            if entry is None or rr is None or rr < MIN_RR:
                continue

            if zone_debug:
                if reason_primary:
                    reason_primary = reason_primary + " | " + zone_debug
                else:
                    reason_primary = zone_debug

            # Leverage suggestion
            lev = suggest_leverage_for_symbol(feats, rr, direction)

            # ---- DEBUG COLUMNS: RVOL, regime, noise floor ----
            vol_rvol = feats.get("vol_rvol_5m", 1.0)
            regime = st.vol_regime
            rng_15m = feats.get("range_15m", 0.0)
            rv_15m = feats.get("rv_15m", 0.0)
            noise_floor_pct = max(
                MIN_SL_PCT,
                rng_15m * NOISE_RANGE_MULT if rng_15m > 0 else 0.0,
                rv_15m * NOISE_RV_MULT if rv_15m > 0 else 0.0,
            )

            # Trade trigger
            self._maybe_open_trade(
                sym=sym,
                side=direction,
                entry=entry,
                sl=sl,
                tp1=tp1,
                tp2=tp2,
                tp3=tp3,
                rr=rr,
                primary_label=primary_label,
                setups_label=setups_label,
                reason=reason_primary,
                leverage=lev,
            )

            rows.append(
                (
                    alpha_ewma,      # 0
                    sym,             # 1
                    f"{price:.6g}",  # 2
                    setups_label,    # 3
                    f"{alpha_ewma:.2f}",      # 4
                    f"{primary_score:.2f}",   # 5
                    primary_label,            # 6
                    direction,                # 7
                    f"{vol5m / 1_000_000:.2f}M",   # 8
                    f"{vol_rvol:.2f}",             # 9 RVOL
                    regime,                       # 10 Regime
                    f"{ret15 * 100:.2f}%",        # 11
                    f"{obi * 100:.1f}%",          # 12
                    f"{oi_chg:.0f}",              # 13
                    price_spark,                  # 14
                    reason_primary,               # 15
                    entry,                        # 16
                    sl,                           # 17
                    tp1,                          # 18
                    tp2,                          # 19
                    tp3,                          # 20
                    rr,                           # 21
                    lev,                          # 22
                    noise_floor_pct,              # 23 Noise floor (raw fraction)
                )
            )
            active_syms.append(sym)

        rows.sort(key=lambda r: r[0], reverse=True)
        rows = rows[:MAX_ROWS]

        # pad with blank rows so height is constant
        while len(rows) < MAX_ROWS:
            rows.append(
                (
                    -999.0,  # alpha
                    "-",     # sym
                    "-",     # price
                    "-",     # setups_label
                    "-",     # alpha_str
                    "-",     # setup_score_str
                    "-",     # primary_label
                    "-",     # direction
                    "-",     # vol_str
                    "-",     # rvol_str
                    "-",     # regime
                    "-",     # ret_str
                    "-",     # obi_str
                    "-",     # oi_str
                    "",      # spark_str
                    "",      # reason
                    None,    # entry
                    None,    # sl
                    None,    # tp1
                    None,    # tp2
                    None,    # tp3
                    None,    # rr
                    None,    # lev
                    None,    # noise_floor_pct
                )
            )

        table = Table(
            title="Breakout Setups – Binance Futures (USDT Perps)",
            box=box.SIMPLE_HEAVY,
            expand=True,
        )

        table.add_column("Rank", justify="right", style="bold")
        table.add_column("Symbol", style="cyan", no_wrap=True)
        table.add_column("Price", justify="right")
        table.add_column("Setups", style="magenta", no_wrap=True)
        table.add_column("Alpha", justify="right", style="bold green")
        table.add_column("SetupScore", justify="right")
        table.add_column("Primary", no_wrap=True)
        table.add_column("Side", no_wrap=True)
        table.add_column("5m Vol (USDT)", justify="right")
        table.add_column("RVOL(5m)", justify="right")          # NEW
        table.add_column("Regime", justify="right")            # NEW
        table.add_column("15m Ret", justify="right")
        table.add_column("OB Imb", justify="right")
        table.add_column("ΔOI", justify="right")
        table.add_column("Spark", justify="left")
        table.add_column("Why (primary)", style="dim")
        table.add_column("Entry", justify="right")
        table.add_column("SL", justify="right")
        table.add_column("TP1", justify="right")
        table.add_column("TP2", justify="right")
        table.add_column("TP3", justify="right")
        table.add_column("Noise SL%", justify="right")         # NEW (percent)
        table.add_column("RR", justify="right")
        table.add_column("Lev", justify="right")

        for idx, row in enumerate(rows, start=1):
            (
                alpha,
                sym,
                price,
                setups_label,
                alpha_str,
                setup_score_str,
                primary_label,
                direction,
                vol_str,
                rvol_str,
                regime,
                ret_str,
                obi_str,
                oi_str,
                spark_str,
                reason,
                entry,
                sl,
                tp1,
                tp2,
                tp3,
                rr,
                lev,
                noise_floor_pct,
            ) = row

            if sym == "-":
                # blank filler row
                table.add_row(
                    str(idx),
                    "-",
                    "-",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "-",
                    "-",
                    "-",
                    "-",
                    "-",
                    "-",
                    "-",
                    "-",
                )
                continue

            noise_str = (
                f"{noise_floor_pct * 100:.2f}%"
                if noise_floor_pct is not None
                else "-"
            )

            table.add_row(
                str(idx),
                sym,
                price,
                setups_label,
                alpha_str,
                setup_score_str,
                primary_label,
                direction,
                vol_str,
                rvol_str,
                regime,
                ret_str,
                obi_str,
                oi_str,
                spark_str,
                reason,
                f"{entry:.6g}" if entry else "-",
                f"{sl:.6g}" if sl else "-",
                f"{tp1:.6g}" if tp1 else "-",
                f"{tp2:.6g}" if tp2 else "-",
                f"{tp3:.6g}" if tp3 else "-",
                noise_str,
                f"{rr:.2f}" if rr is not None else "-",
                f"{lev:.1f}x" if lev is not None else "-",
            )

        subtitle_text = (
            f"Universe: Binance USDT Perps • Raw score ≥ {GLOBAL_RAW_SCORE_GATE:.2f} "
            f"| RR ≥ {MIN_RR:.1f} | 24 archetypes + Magnetic Zones + Lev Governor"
        )
        subtitle = Text(subtitle_text)

        panel = Panel(
            table,
            title="[bold]Parallel Breakout Setup Monitor (24 Archetypes)[/bold]",
            subtitle=subtitle,
            border_style="bright_blue",
        )
        return panel, active_syms

    def _build_trades_panel(self) -> Panel:
        # Open trades table
        t_open = Table(
            title="Open Trades",
            box=box.MINIMAL_HEAVY_HEAD,
            expand=True,
        )
        t_open.add_column("Symbol", style="cyan", no_wrap=True)
        t_open.add_column("Side", no_wrap=True)
        t_open.add_column("Entry", justify="right")
        t_open.add_column("SL", justify="right")
        t_open.add_column("TP1", justify="right")
        t_open.add_column("TP2", justify="right")
        t_open.add_column("TP3", justify="right")
        t_open.add_column("RR", justify="right")
        t_open.add_column("Lev", justify="right")
        t_open.add_column("Primary", no_wrap=True)
        t_open.add_column("Why", style="dim")

        open_rows_used = 0
        if not self.open_trades:
            t_open.add_row("-", "-", "-", "-", "-", "-", "-", "-", "-", "No active trades yet")
            open_rows_used = 1
        else:
            for trade in self.open_trades.values():
                t_open.add_row(
                    trade.symbol,
                    trade.side,
                    f"{trade.entry:.6g}",
                    f"{trade.sl:.6g}",
                    f"{trade.tp1:.6g}",
                    f"{trade.tp2:.6g}",
                    f"{trade.tp3:.6g}",
                    f"{trade.rr:.2f}",
                    f"{trade.leverage:.1f}x",
                    trade.primary,
                    trade.reason,
                )
                open_rows_used += 1

        while open_rows_used < OPEN_ROWS:
            t_open.add_row("-", "-", "-", "-", "-", "-", "-", "-", "-", "")
            open_rows_used += 1

        # Closed trades table
        t_closed = Table(
            title="Closed Trades (recent)",
            box=box.MINIMAL_HEAVY_HEAD,
            expand=True,
        )
        t_closed.add_column("Time", justify="right")
        t_closed.add_column("Symbol", style="cyan", no_wrap=True)
        t_closed.add_column("Side", no_wrap=True)
        t_closed.add_column("Entry", justify="right")
        t_closed.add_column("Exit", justify="right")
        t_closed.add_column("Verdict", justify="right")
        t_closed.add_column("RR", justify="right")

        closed_rows_used = 0
        if not self.closed_trades:
            t_closed.add_row("-", "-", "-", "-", "-", "-", "-")
            closed_rows_used = 1
        else:
            for trade in list(self.closed_trades)[:CLOSED_ROWS]:
                time_str = (
                    time.strftime("%H:%M:%S", time.localtime(trade.ts_close))
                    if trade.ts_close
                    else "-"
                )
                t_closed.add_row(
                    time_str,
                    trade.symbol,
                    trade.side,
                    f"{trade.entry:.6g}",
                    f"{trade.exit_price:.6g}",
                    trade.verdict,
                    f"{trade.rr:.2f}",
                )
                closed_rows_used += 1

        while closed_rows_used < CLOSED_ROWS:
            t_closed.add_row("-", "-", "-", "-", "-", "-", "-")
            closed_rows_used += 1

        grid = Table.grid(padding=(0, 2), expand=True)
        grid.add_row(t_open)
        grid.add_row(t_closed)
        panel = Panel(grid, title="Trades Dashboard", border_style="green")
        return panel

    # ---- Legend panel ----
    def _build_legend_panel(self) -> Panel:
        """
        Baby-language legend for the debug columns:
        - RVOL(5m)
        - Regime
        - Noise SL%
        """
        txt = Text()
        txt.append("Legend – debug columns\n", style="bold underline")

        txt.append("\nRVOL(5m):  ", style="bold")
        txt.append("How hot the last 5 minutes of volume are compared to this coin’s own normal.\n")
        txt.append(" • ≈1.0   → normal volume.\n")
        txt.append(" • 1.5–2.0 → getting active.\n")
        txt.append(" • >2.0   → 🔥 something is waking up.\n")
        txt.append(" • >3.0   → 🚀 extreme, real action.\n")

        txt.append("\nRegime:  ", style="bold")
        txt.append("How wild the last ~15 minutes of price movement are.\n")
        txt.append(" • calm   → super quiet, small candles, low range.\n")
        txt.append(" • normal → healthy movement, not dead, not crazy.\n")
        txt.append(" • spicy  → big candles, wide range, trend moves.\n")

        txt.append("\nNoise SL%:  ", style="bold")
        txt.append("Minimum stop-loss distance that keeps you out of useless wiggles.\n")
        txt.append(" • Comes from 15m range + realized volatility.\n")
        txt.append(" • SL will NOT be tighter than this %, so tiny micro-noise doesn’t hit your stop.\n")
        txt.append(" • Bigger Noise SL% = market is wild → script automatically gives price more breathing room.\n")

        txt.append("\nHow to use:\n", style="bold")
        txt.append(" • Look for setups where: RVOL(5m) is high, Regime is normal/spicy, and Noise SL% is sane (not micro, not insane).\n")
        txt.append(" • If RVOL is dead and Regime = calm, but the script still wants to trade → be suspicious.\n")

        return Panel(
            txt,
            title="Legend – RVOL, Regime, Noise SL%",
            border_style="yellow",
        )

# ======================================================================
# Main
# ======================================================================

async def run_main():
    app = BreakoutArchetypeApp()
    try:
        await app.start()
    finally:
        await app.stop()

if __name__ == "__main__":
    try:
        asyncio.run(run_main())
    except KeyboardInterrupt:
        logger.info("Shutting down cleanly (Ctrl+C)…")
