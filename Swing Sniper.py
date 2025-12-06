#!/usr/bin/env python3
"""
Swing Sniper v3.1 – Breakout Potential Screener
-----------------------------------------------

Ferrari-style live HUD + honest blueprint-aligned feature engine.

LIVE MODE:
    python "Swing Sniper.py"

REPLAY MODE:
    python "Swing Sniper.py" --replay path/to/history.csv

Replay CSV columns:
    timestamp,exchange,symbol,price,quote_volume_24h,oi

Blueprint honesty markers:
    [BLUEPRINT]  = what spec demands
    [CURRENT]    = what this script actually does
    [GAP]        = missing elements (no fake naming)
"""

import argparse
import asyncio
import csv
import json
import math
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional, Set

import aiohttp
import aiosqlite
import websockets
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.text import Text

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BINANCE_FAPI_REST = "https://fapi.binance.com"
BINANCE_FAPI_WS_TICK = "wss://fstream.binance.com/stream?streams=!ticker@arr"
BINANCE_FAPI_WS_TRADES = "wss://fstream.binance.com/stream?streams=!aggTrade@arr"

BYBIT_REST = "https://api.bybit.com"
BYBIT_WS_LINEAR = "wss://stream.bybit.com/v5/public/linear"

MAX_BINANCE_SYMBOLS = 150
MAX_BYBIT_SYMBOLS = 100

OI_REFRESH_SECONDS = 60
SCREEN_REFRESH_SECONDS = 0.5

RET_WINDOW = 200
VOL_WINDOW = 200
OI_WINDOW = 200

FAST_SLICE = 8
SLOW_SLICE = 32

MIN_NOTIONAL_24H = 20_000

DB_PATH = "swing_sniper_trades.sqlite"

IGNITION_ENTRY_THRESHOLD = 75.0   # to be tuned via replay
SL_PCT = 0.03                     # 3% stop
TP_FACTOR_INDEX = 0               # use TP1 as exit

# ---------------------------------------------------------------------------
# Data Structures
# ---------------------------------------------------------------------------


@dataclass
class SymbolState:
    exchange: str
    symbol: str

    last_price: float = math.nan
    last_ts: float = 0.0
    last_quote_vol24h: float = 0.0
    notional_24h: float = 0.0

    returns: deque = field(default_factory=lambda: deque(maxlen=RET_WINDOW))
    delta_q: deque = field(default_factory=lambda: deque(maxlen=VOL_WINDOW))
    oi_samples: deque = field(default_factory=lambda: deque(maxlen=OI_WINDOW))

    last_oi: float = math.nan
    last_oi_ts: float = 0.0

    cvd: float = 0.0  # futures CVD (notional)

    coil_score: float = 0.0
    ignition_score: float = 0.0
    tp_levels: List[float] = field(default_factory=list)

    # ------------------------------------------------------------------ #
    # Ticker / OI / CVD updates
    # ------------------------------------------------------------------ #

    def update_from_ticker(
        self,
        ts_ms: int,
        price: float,
        quote_vol24h: Optional[float] = None,
        notional_24h: Optional[float] = None,
    ) -> None:
        ts = ts_ms / 1000.0
        if not math.isfinite(price) or price <= 0:
            return

        if self.last_ts > 0 and self.last_price > 0:
            dt = ts - self.last_ts
            if dt <= 0:
                dt = 1e-3
            ret = math.log(price / self.last_price)
            self.returns.append(ret)

            if quote_vol24h is not None:
                if self.last_quote_vol24h > 0 and quote_vol24h >= self.last_quote_vol24h:
                    dq = quote_vol24h - self.last_quote_vol24h
                    self.delta_q.append(dq / dt)
                self.last_quote_vol24h = quote_vol24h

        self.last_price = price
        self.last_ts = ts

        if notional_24h is not None and notional_24h > 0:
            self.notional_24h = notional_24h

    def update_oi(self, oi: float, ts: float) -> None:
        if oi > 0:
            self.last_oi = oi
            self.last_oi_ts = ts
            self.oi_samples.append(oi)

    def apply_trade_cvd(self, qty: float, price: float, is_buy_taker: bool) -> None:
        """
        [BLUEPRINT] True futures CVD with decay, multi-TF slope modelling.
        [CURRENT]  Raw cumulative futures CVD (notional) from aggTrade.
        [GAP]      No decay, no MT slope decomposition.
        """
        notional = qty * price
        if notional <= 0:
            return
        if is_buy_taker:
            self.cvd += notional
        else:
            self.cvd -= notional

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #

    def _safe_mean(self, arr: List[float]) -> float:
        if not arr:
            return math.nan
        return sum(arr) / len(arr)

    def _safe_std(self, arr: List[float]) -> float:
        n = len(arr)
        if n < 2:
            return math.nan
        mu = self._safe_mean(arr)
        var = sum((x - mu) ** 2 for x in arr) / (n - 1)
        return math.sqrt(var)

    # ------------------------------------------------------------------ #
    # RVOL
    # ------------------------------------------------------------------ #

    def compute_rvol(self) -> float:
        """
        [BLUEPRINT] RVOL anomalies with multiple baselines + anomaly flags.
        [CURRENT]  Fast mean taker notional speed vs whole-history mean.
        [GAP]      Only one baseline, no full anomaly model.

        Loosened: wakes up as soon as there's ANY history.
        """
        dq_list = list(self.delta_q)
        if len(dq_list) == 0:
            return math.nan
        fast_len = min(FAST_SLICE, len(dq_list))
        fast = dq_list[-fast_len:]
        slow = dq_list
        fast_mean = self._safe_mean(fast)
        slow_mean = self._safe_mean(slow)
        if not math.isfinite(fast_mean) or not math.isfinite(slow_mean) or slow_mean <= 0:
            return 1.0  # neutral RVOL instead of NaN
        return fast_mean / slow_mean

    # ------------------------------------------------------------------ #
    # Entropy squeeze proxy (BB-in-KC)
    # ------------------------------------------------------------------ #

    def compute_vol_squeeze(self) -> float:
        """
        [BLUEPRINT] Entropy Collapse via Shannon entropy + MEI + BB-in-KC.
        [CURRENT]  BB-in-KC proxy on returns:
            - BB width ~ std(rets)
            - KC width ~ mean(|rets|) * factor
            - Squeeze = (KC - BB) / KC, clipped to 0..100.
        [GAP]      No entropy/MEI; pure variance & range proxy.

        Loosened: works with >=2 returns.
        """
        rets = list(self.returns)
        if len(rets) < 2:
            return 0.0

        fast_len = min(FAST_SLICE, len(rets))
        fast = rets[-fast_len:]
        sigma = self._safe_std(fast)
        if not math.isfinite(sigma):
            return 0.0

        atr_proxy = self._safe_mean([abs(x) for x in fast])
        if not math.isfinite(atr_proxy) or atr_proxy <= 0:
            return 0.0

        bb_width = 2.0 * sigma
        kc_width = 2.0 * atr_proxy * 1.5

        if kc_width <= 0 or bb_width >= kc_width:
            return 0.0

        squeeze = (kc_width - bb_width) / kc_width
        return max(0.0, min(1.0, squeeze)) * 100.0

    # ------------------------------------------------------------------ #
    # OI Z-score
    # ------------------------------------------------------------------ #

    def compute_oi_zscore(self) -> float:
        """
        [BLUEPRINT] OI Z-score with multi half-life decay, quiet build vs unwind.
        [CURRENT]  Z-score of last OI vs rolling mean/std.
        [GAP]      No half-life decay; no OI/vol ROC comparison.

        Loosened: wakes up after a few samples, not 10+.
        """
        oi_list = list(self.oi_samples)
        if len(oi_list) < 3 or not math.isfinite(self.last_oi):
            return 0.0

        mu = self._safe_mean(oi_list)
        sd = self._safe_std(oi_list)
        if not math.isfinite(mu) or not math.isfinite(sd) or sd <= 0:
            return 0.0

        z = (self.last_oi - mu) / sd
        if z <= 0:
            return 0.0
        z_clamped = min(z, 3.0)
        return z_clamped / 3.0 * 100.0

    # ------------------------------------------------------------------ #
    # Momentum burst
    # ------------------------------------------------------------------ #

    def compute_momentum_burst(self) -> float:
        """
        [BLUEPRINT] Acceleration signatures via ROC + higher-order trends.
        [CURRENT]  Simple z-score: recent-mean vs base-mean of returns.
        [GAP]      No higher derivatives, no explicit regime model.

        Loosened: starts with 3+ returns.
        """
        rets = list(self.returns)
        if len(rets) < 3:
            return 0.0
        recent_len = min(3, len(rets))
        base_len = min(max(FAST_SLICE, 4), len(rets))
        recent = rets[-recent_len:]
        base = rets[-base_len:]
        mu_recent = self._safe_mean(recent)
        mu_base = self._safe_mean(base)
        sd_base = self._safe_std(base)
        if sd_base <= 0 or not math.isfinite(sd_base):
            return 0.0
        z = (mu_recent - mu_base) / sd_base
        if z <= 0:
            return 0.0
        return max(0.0, min(5.0, z)) / 5.0 * 100.0

    # ------------------------------------------------------------------ #
    # ATR proxy + TP ladder
    # ------------------------------------------------------------------ #

    def compute_atr_proxy(self) -> float:
        """
        [BLUEPRINT] Volatility modelling per regime, TP based on VPVR/VWAP.
        [CURRENT]  ATR-like proxy from return std × last price.
        [GAP]      No volume profile; no BOCPD.
        """
        rets = list(self.returns)
        if len(rets) < 2:
            return 0.0
        fast_len = min(FAST_SLICE, len(rets))
        fast = rets[-fast_len:]
        fast_std = self._safe_std(fast)
        if not math.isfinite(fast_std) or self.last_price <= 0:
            return 0.0
        return fast_std * self.last_price

    def compute_tp_ladder(self) -> List[float]:
        """
        [BLUEPRINT] Dynamic magnetic ladder from volume nodes + BOCPD.
        [CURRENT]  Static ATR multiples (1.5, 2.5, 3.5).
        [GAP]      No node targeting; no run-length model.
        """
        atr = self.compute_atr_proxy()
        if atr <= 0 or self.last_price <= 0:
            return []
        factors = [1.5, 2.5, 3.5]
        return [self.last_price + f * atr for f in factors]

    # ------------------------------------------------------------------ #
    # CVD score
    # ------------------------------------------------------------------ #

    def compute_cvd_score(self) -> float:
        """
        [BLUEPRINT] Multi-TF CVD slope differential & spot vs perp divergence.
        [CURRENT]  Magnitude-only log-scaled CVD strength 0..100.
        [GAP]      No slope, no spot comparison.
        """
        if self.cvd == 0:
            return 0.0
        mag = math.log10(abs(self.cvd) + 1.0)
        mag = min(mag, 6.0) / 6.0 * 100.0
        return mag

    # ------------------------------------------------------------------ #
    # Score fusion
    # ------------------------------------------------------------------ #

    def update_scores(self) -> None:
        rvol = self.compute_rvol()
        squeeze = self.compute_vol_squeeze()
        mom = self.compute_momentum_burst()
        oi_z = self.compute_oi_zscore()
        cvd_score = self.compute_cvd_score()

        # RVOL mid-term "comfort zone" around 1.0
        rvol_for_mid = rvol if math.isfinite(rvol) else 1.0
        if 0.6 <= rvol_for_mid <= 1.6:
            rvol_mid = 100.0
        else:
            dist = min(abs(rvol_for_mid - 1.1), 3.0)
            rvol_mid = max(0.0, 100.0 * (1.0 - dist / 3.0))

        # Coil: compressed, quietly building
        self.coil_score = max(
            0.0,
            min(
                100.0,
                0.55 * squeeze + 0.20 * rvol_mid + 0.15 * oi_z + 0.10 * cvd_score,
            ),
        )

        # RVOL burst component
        rvol_burst = 0.0
        if math.isfinite(rvol) and rvol > 1.0:
            r_clamped = min(rvol, 6.0)
            rvol_burst = (r_clamped - 1.0) / 5.0 * 100.0

        # Ignition: burst + RVOL spike + OI up + CVD pressure
        self.ignition_score = max(
            0.0,
            min(
                100.0,
                0.45 * mom + 0.30 * rvol_burst + 0.15 * oi_z + 0.10 * cvd_score,
            ),
        )

        self.tp_levels = self.compute_tp_ladder()


# ---------------------------------------------------------------------------
# Global State
# ---------------------------------------------------------------------------

symbol_states: Dict[Tuple[str, str], SymbolState] = {}
binance_symbols: Set[str] = set()
bybit_symbols: Set[str] = set()
state_lock = asyncio.Lock()

open_trades: Dict[Tuple[str, str], dict] = {}
db_conn: Optional[aiosqlite.Connection] = None
db_lock = asyncio.Lock()

# ---------------------------------------------------------------------------
# DB Helpers
# ---------------------------------------------------------------------------


async def init_db() -> None:
    global db_conn
    db_conn = await aiosqlite.connect(DB_PATH)
    await db_conn.execute(
        """
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            entry_ts REAL NOT NULL,
            entry_price REAL NOT NULL,
            size REAL NOT NULL,
            exit_ts REAL,
            exit_price REAL,
            pnl REAL,
            status TEXT NOT NULL
        )
        """
    )
    await db_conn.commit()


async def log_trade_open(exchange: str, symbol: str, entry_ts: float, entry_price: float, size: float) -> int:
    async with db_lock:
        cur = await db_conn.execute(
            """
            INSERT INTO trades (exchange, symbol, side, entry_ts, entry_price, size, status)
            VALUES (?, ?, 'LONG', ?, ?, ?, 'OPEN')
            """,
            (exchange, symbol, entry_ts, entry_price, size),
        )
        await db_conn.commit()
        return cur.lastrowid


async def log_trade_close(trade_id: int, exit_ts: float, exit_price: float) -> None:
    async with db_lock:
        cur = await db_conn.execute(
            "SELECT entry_price, size FROM trades WHERE id = ?",
            (trade_id,),
        )
        row = await cur.fetchone()
        if not row:
            return
        entry_price, size = row
        pnl = (exit_price - entry_price) * size
        await db_conn.execute(
            """
            UPDATE trades
            SET exit_ts = ?, exit_price = ?, pnl = ?, status = 'CLOSED'
            WHERE id = ?
            """,
            (exit_ts, exit_price, pnl, trade_id),
        )
        await db_conn.commit()


async def compute_expectancy_stats() -> dict:
    async with db_lock:
        cur = await db_conn.execute(
            "SELECT COUNT(*), SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END), "
            "AVG(pnl), SUM(pnl) FROM trades WHERE status='CLOSED'"
        )
        row = await cur.fetchone()
    if not row:
        return {"n": 0, "wins": 0, "winrate": 0.0, "avg_pnl": 0.0, "total_pnl": 0.0}
    n_closed, n_win, avg_pnl, total_pnl = row
    n_closed = n_closed or 0
    n_win = n_win or 0
    winrate = (n_win / n_closed * 100.0) if n_closed > 0 else 0.0
    avg_pnl = avg_pnl or 0.0
    total_pnl = total_pnl or 0.0
    return {
        "n": n_closed,
        "wins": n_win,
        "winrate": winrate,
        "avg_pnl": avg_pnl,
        "total_pnl": total_pnl,
    }


# ---------------------------------------------------------------------------
# Universe discovery & bootstrap
# ---------------------------------------------------------------------------


async def discover_binance_universe(session: aiohttp.ClientSession):
    url = f"{BINANCE_FAPI_REST}/fapi/v1/exchangeInfo"
    async with session.get(url, timeout=15) as resp:
        data = await resp.json()
    symbols_info = [
        s for s in data["symbols"]
        if s.get("contractType") == "PERPETUAL"
        and s.get("quoteAsset") == "USDT"
        and s.get("status") == "TRADING"
    ]

    tick_url = f"{BINANCE_FAPI_REST}/fapi/v1/ticker/24hr"
    async with session.get(tick_url, timeout=15) as resp:
        tdata = await resp.json()
    vol_map = {t["symbol"]: float(t["quoteVolume"]) for t in tdata}
    tick_map = {t["symbol"]: t for t in tdata}

    ranked = sorted(
        [s["symbol"] for s in symbols_info if s["symbol"] in vol_map],
        key=lambda sym: vol_map.get(sym, 0.0),
        reverse=True,
    )
    return ranked[:MAX_BINANCE_SYMBOLS], tick_map


async def discover_bybit_universe(session: aiohttp.ClientSession):
    url = f"{BYBIT_REST}/v5/market/instruments-info?category=linear"
    async with session.get(url, timeout=15) as resp:
        data = await resp.json()
    if data.get("retCode") != 0:
        return [], {}

    rows = data["result"]["list"]
    usdt = [
        r for r in rows
        if r.get("quoteCoin") == "USDT" and r.get("status") == "Trading"
    ]

    tick_url = f"{BYBIT_REST}/v5/market/tickers?category=linear"
    async with session.get(tick_url, timeout=15) as resp:
        tdata = await resp.json()
    if tdata.get("retCode") != 0:
        return [], {}

    tick_rows = tdata["result"]["list"]
    tick_map = {row["symbol"]: row for row in tick_rows}
    vol_map = {row["symbol"]: float(row["turnover24h"]) for row in tick_rows}

    ranked = sorted(
        [r["symbol"] for r in usdt if r["symbol"] in vol_map],
        key=lambda s: vol_map.get(s, 0.0),
        reverse=True,
    )
    return ranked[:MAX_BYBIT_SYMBOLS], tick_map


async def bootstrap_snapshots(binance_tick_map: Dict[str, dict], bybit_tick_map: Dict[str, dict]) -> None:
    now_ms = int(time.time() * 1000)
    async with state_lock:
        # Binance
        for sym in binance_symbols:
            t = binance_tick_map.get(sym)
            if not t:
                continue
            price = float(t.get("lastPrice", t.get("closePrice", t.get("c", "0"))))
            quote_vol = float(t.get("quoteVolume", "0") or 0)
            key = ("BINANCE", sym)
            st = symbol_states.get(key)
            if not st:
                st = SymbolState(exchange="BINANCE", symbol=sym)
                symbol_states[key] = st
            st.update_from_ticker(now_ms, price, quote_vol, quote_vol)
            st.update_scores()

        # Bybit
        for sym in bybit_symbols:
            row = bybit_tick_map.get(sym)
            if not row:
                continue
            price = float(row.get("lastPrice", "0") or 0)
            turnover = float(row.get("turnover24h", "0") or 0)
            oi_val = float(row.get("openInterestValue", "0") or 0)
            key = ("BYBIT", sym)
            st = symbol_states.get(key)
            if not st:
                st = SymbolState(exchange="BYBIT", symbol=sym)
                symbol_states[key] = st
            st.update_from_ticker(now_ms, price, turnover, turnover)
            if oi_val > 0:
                st.update_oi(oi_val, now_ms / 1000.0)
            st.update_scores()


# ---------------------------------------------------------------------------
# OI updaters
# ---------------------------------------------------------------------------


async def refresh_binance_oi(session: aiohttp.ClientSession) -> None:
    while True:
        start = time.time()
        symbols_snapshot = list(binance_symbols)
        ts = time.time()
        for sym in symbols_snapshot:
            url = f"{BINANCE_FAPI_REST}/fapi/v1/openInterest?symbol={sym}"
            try:
                async with session.get(url, timeout=10) as resp:
                    data = await resp.json()
                oi = float(data.get("openInterest", "0") or 0)
            except Exception:
                continue

            async with state_lock:
                key = ("BINANCE", sym)
                st = symbol_states.get(key)
                if st:
                    st.update_oi(oi, ts)
                    st.update_scores()

        elapsed = time.time() - start
        await asyncio.sleep(max(5.0, OI_REFRESH_SECONDS - elapsed))


async def refresh_bybit_oi(session: aiohttp.ClientSession) -> None:
    while True:
        start = time.time()
        try:
            url = f"{BYBIT_REST}/v5/market/tickers?category=linear"
            async with session.get(url, timeout=15) as resp:
                data = await resp.json()
            if data.get("retCode") != 0:
                elapsed = time.time() - start
                await asyncio.sleep(max(5.0, OI_REFRESH_SECONDS - elapsed))
                continue

            rows = data["result"]["list"]
            ts = time.time()
            async with state_lock:
                for row in rows:
                    sym = row["symbol"]
                    if sym not in bybit_symbols:
                        continue
                    oi = float(row.get("openInterestValue", "0") or 0)
                    key = ("BYBIT", sym)
                    st = symbol_states.get(key)
                    if st:
                        st.update_oi(oi, ts)
                        st.update_scores()
        except Exception:
            pass

        elapsed = time.time() - start
        await asyncio.sleep(max(5.0, OI_REFRESH_SECONDS - elapsed))


# ---------------------------------------------------------------------------
# WebSocket handlers (live)
# ---------------------------------------------------------------------------


async def binance_ws_ticker_loop() -> None:
    while True:
        try:
            async with websockets.connect(
                BINANCE_FAPI_WS_TICK, ping_interval=20, ping_timeout=20
            ) as ws:
                async for msg in ws:
                    data = json.loads(msg)
                    payload = data.get("data")
                    if not payload:
                        continue
                    items = payload if isinstance(payload, list) else [payload]
                    ts_ms = int(time.time() * 1000)
                    async with state_lock:
                        for t in items:
                            sym = t.get("s")
                            if sym not in binance_symbols:
                                continue
                            price = float(t["c"])
                            quote_vol24h = float(t["quoteVolume"])
                            notional = quote_vol24h
                            key = ("BINANCE", sym)
                            st = symbol_states.get(key)
                            if not st:
                                st = SymbolState(exchange="BINANCE", symbol=sym)
                                symbol_states[key] = st
                            st.update_from_ticker(ts_ms, price, quote_vol24h, notional)
                            st.update_scores()
        except Exception:
            await asyncio.sleep(3.0)


async def binance_ws_trade_loop() -> None:
    """
    Binance aggTrade stream for real CVD.
    """
    while True:
        try:
            async with websockets.connect(
                BINANCE_FAPI_WS_TRADES, ping_interval=20, ping_timeout=20
            ) as ws:
                async for msg in ws:
                    data = json.loads(msg)
                    payload = data.get("data")
                    if not payload:
                        continue
                    items = payload if isinstance(payload, list) else [payload]
                    async with state_lock:
                        for t in items:
                            sym = t.get("s")
                            if sym not in binance_symbols:
                                continue
                            price = float(t["p"])
                            qty = float(t["q"])
                            is_buy_taker = not t.get("m", False)
                            key = ("BINANCE", sym)
                            st = symbol_states.get(key)
                            if not st:
                                st = SymbolState(exchange="BINANCE", symbol=sym)
                                symbol_states[key] = st
                            st.apply_trade_cvd(qty, price, is_buy_taker)
                            st.update_scores()
        except Exception:
            await asyncio.sleep(3.0)


async def bybit_ws_loop() -> None:
    if not bybit_symbols:
        return
    args = [f"tickers.{sym}" for sym in bybit_symbols]
    sub_msg = {"op": "subscribe", "args": args}

    while True:
        try:
            async with websockets.connect(
                BYBIT_WS_LINEAR, ping_interval=20, ping_timeout=20
            ) as ws:
                await ws.send(json.dumps(sub_msg))
                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("topic", "").startswith("tickers.") and "data" in msg:
                        data_list = msg["data"]
                        ts_ms = int(time.time() * 1000)
                        async with state_lock:
                            for row in data_list:
                                sym = row["symbol"]
                                if sym not in bybit_symbols:
                                    continue
                                price = float(row["lastPrice"])
                                quote_vol24h = float(row.get("turnover24h", "0") or 0)
                                notional = quote_vol24h
                                key = ("BYBIT", sym)
                                st = symbol_states.get(key)
                                if not st:
                                    st = SymbolState(exchange="BYBIT", symbol=sym)
                                    symbol_states[key] = st
                                st.update_from_ticker(ts_ms, price, quote_vol24h, notional)
                                st.update_scores()
        except Exception:
            await asyncio.sleep(3.0)


# ---------------------------------------------------------------------------
# Trade engine (shared logic)
# ---------------------------------------------------------------------------


async def process_trades_for_states(states: List[SymbolState], now: float) -> None:
    """
    Shared trade logic for live + replay.
    """
    for st in states:
        if not math.isfinite(st.last_price):
            continue
        if st.notional_24h < MIN_NOTIONAL_24H:
            continue

        key = (st.exchange, st.symbol)
        trade = open_trades.get(key)

        if trade is None:
            if st.ignition_score >= IGNITION_ENTRY_THRESHOLD:
                entry_price = st.last_price
                size = 1.0  # risk sizing TODO
                trade_id = await log_trade_open(st.exchange, st.symbol, now, entry_price, size)
                open_trades[key] = {
                    "id": trade_id,
                    "entry_price": entry_price,
                    "size": size,
                }
        else:
            entry_price = trade["entry_price"]
            price = st.last_price
            tps = st.tp_levels
            tp_hit = False
            if tps and len(tps) > TP_FACTOR_INDEX:
                tp_price = tps[TP_FACTOR_INDEX]
                if price >= tp_price:
                    tp_hit = True
            sl_price = entry_price * (1.0 - SL_PCT)
            sl_hit = price <= sl_price

            if tp_hit or sl_hit:
                trade_id = trade["id"]
                await log_trade_close(trade_id, now, price)
                del open_trades[key]


async def trade_engine_loop() -> None:
    while True:
        now = time.time()
        async with state_lock:
            states = list(symbol_states.values())
        await process_trades_for_states(states, now)
        await asyncio.sleep(1.0)


# ---------------------------------------------------------------------------
# UI – Ferrari HUD
# ---------------------------------------------------------------------------


def heat_bar(score: float, length: int = 5, frame: int = 0, mode: str = "coil") -> str:
    score = max(0.0, min(100.0, score))
    filled = int(round(score / 100.0 * length))
    empty = length - filled

    if score >= 80:
        if frame % 2 == 0:
            block = "🟩" if mode != "ignition" else "🟥"
        else:
            block = "🟢" if mode != "ignition" else "🔴"
    elif score >= 40:
        block = "🟨"
    else:
        block = "🟦"

    bar = block * filled + "⬛" * empty
    return bar


def cvd_text(st: SymbolState, frame: int) -> Text:
    if st.cvd == 0:
        return Text("0", style="dim")
    style = "bright_green" if st.cvd > 0 else "bright_red"
    mag = abs(st.cvd)
    if mag > 1e6 and frame % 2 == 0:
        style += " bold"
    short = f"{st.cvd/1e6:.1f}M"
    return Text(short, style=style)


def build_phase_table(title: str, rows: List[SymbolState], mode: str, frame: int) -> Table:
    table = Table(
        title=title,
        expand=True,
        show_header=True,
        show_lines=True,
    )
    table.add_column("Ex", justify="left", no_wrap=True)
    table.add_column("Symbol", justify="left", no_wrap=True)
    table.add_column("Price", justify="right")
    table.add_column("Heat", justify="left")
    table.add_column("RVOL", justify="right")
    table.add_column("Sqz", justify="right")
    table.add_column("CVD", justify="right")

    if mode in ("coil", "ignition"):
        table.add_column("OI Z", justify="right")
        table.add_column("Score", justify="right")
    else:
        table.add_column("ATR", justify="right")
        table.add_column("TP1/2/3", justify="left")
        table.add_column("Ign", justify="right")

    for st in rows:
        if not math.isfinite(st.last_price):
            continue
        rvol = st.compute_rvol()
        rvol_str = "—" if not math.isfinite(rvol) else f"{rvol:4.2f}"
        squeeze = st.compute_vol_squeeze()
        squeeze_str = f"{squeeze:4.0f}"
        oi_z = st.compute_oi_zscore()
        oi_str = f"{oi_z:4.0f}"
        cvd_txt = cvd_text(st, frame)

        score = st.coil_score if mode == "coil" else st.ignition_score
        bar = heat_bar(score, length=5, frame=frame, mode=mode)
        ex = "B" if st.exchange == "BINANCE" else "Y"

        if mode in ("coil", "ignition"):
            table.add_row(
                ex,
                st.symbol,
                f"{st.last_price:.6g}",
                bar,
                rvol_str,
                squeeze_str,
                cvd_txt,
                oi_str,
                f"{score:4.0f}",
            )
        else:
            atr = st.compute_atr_proxy()
            atr_str = f"{atr:.4g}" if atr > 0 else "—"
            tps = st.tp_levels
            if len(tps) == 3:
                tp_str = f"{tps[0]:.4g}/{tps[1]:.4g}/{tps[2]:.4g}"
            else:
                tp_str = "—"
            ign_str = f"{st.ignition_score:4.0f}"
            table.add_row(
                ex,
                st.symbol,
                f"{st.last_price:.6g}",
                bar,
                rvol_str,
                squeeze_str,
                cvd_txt,
                atr_str,
                tp_str,
                ign_str,
            )

    return table


def build_layout() -> Layout:
    layout = Layout()
    layout.split(
        Layout(name="header", size=3),
        Layout(name="stats", size=5),
        Layout(name="body", ratio=1),
    )
    layout["body"].split_row(
        Layout(name="coil"),
        Layout(name="ignite"),
        Layout(name="tp"),
    )
    return layout


async def ui_loop() -> None:
    layout = build_layout()
    frame = 0
    with Live(layout, refresh_per_second=10, screen=True):
        while True:
            async with state_lock:
                states = list(symbol_states.values())

            filtered = [
                s for s in states
                if s.notional_24h > MIN_NOTIONAL_24H and math.isfinite(s.last_price)
            ]

            coils = sorted(filtered, key=lambda s: s.coil_score, reverse=True)[:12]
            ignitions = sorted(filtered, key=lambda s: s.ignition_score, reverse=True)[:12]
            tps = [s for s in filtered if s.ignition_score > 20]
            tps = sorted(tps, key=lambda s: s.ignition_score, reverse=True)[:12]

            coil_table = build_phase_table("A) COILED 🔋", coils, mode="coil", frame=frame)
            ign_table = build_phase_table("B) IGNITION 🚀", ignitions, mode="ignition", frame=frame)
            tp_table = build_phase_table("C) TP LADDER 🎯", tps, mode="tp", frame=frame)

            live = [s for s in states if math.isfinite(s.last_price)]
            stats = await compute_expectancy_stats()
            open_count = len(open_trades)

            header_text = Text()
            header_text.append("🏎️ Swing Sniper – Breakout Potential ", style="bold red")
            header_text.append("| ", style="dim")
            header_text.append("Binance USDT Perp + Bybit Linear", style="cyan")
            header_text.append(" | ", style="dim")
            header_text.append(
                f"Universe: {len(binance_symbols)} B, {len(bybit_symbols)} Y | "
                f"Live: {len(live)} | Filtered: {len(filtered)}",
                style="yellow",
            )
            layout["header"].update(Panel(header_text, style="bold red"))

            stats_text = Text()
            stats_text.append("Race Telemetry (PnL Engine)\n", style="bold magenta")
            stats_text.append(f"Open positions: {open_count}\n", style="bright_white")
            stats_text.append(
                f"Closed trades: {stats['n']}  |  Wins: {stats['wins']} "
                f"({stats['winrate']:.1f}%)\n",
                style="green" if stats["winrate"] >= 50 else "yellow",
            )
            stats_text.append(
                f"Avg PnL: {stats['avg_pnl']:.4f}  |  Total PnL: {stats['total_pnl']:.4f}\n",
                style="bright_green" if stats["total_pnl"] >= 0 else "bright_red",
            )
            stats_text.append(
                f"\nRule: Ignition ≥ {IGNITION_ENTRY_THRESHOLD:.0f}, LONG only, "
                f"TP at TP1, SL {SL_PCT*100:.1f}%\n",
                style="dim",
            )
            layout["stats"].update(Panel(stats_text, title="🏁 Expectancy Panel", border_style="magenta"))

            layout["coil"].update(coil_table)
            layout["ignite"].update(ign_table)
            layout["tp"].update(tp_table)

            frame += 1
            await asyncio.sleep(SCREEN_REFRESH_SECONDS)


# ---------------------------------------------------------------------------
# Replay mode
# ---------------------------------------------------------------------------


def _parse_ts(value: str) -> float:
    """Accept ms or seconds."""
    v = float(value)
    if v > 10_000_000_000:  # ms range
        return v / 1000.0
    return v


async def run_replay(csv_path: str) -> None:
    """
    Basic walk-forward using CSV history.
    Expects columns:
        timestamp,exchange,symbol,price,quote_volume_24h,oi
    """

    await init_db()

    global symbol_states, open_trades, binance_symbols, bybit_symbols
    symbol_states = {}
    open_trades = {}
    binance_symbols = set()
    bybit_symbols = set()

    rows = []
    with open(csv_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            try:
                ts = _parse_ts(r["timestamp"])
                exchange = r["exchange"].upper()
                symbol = r["symbol"]
                price = float(r["price"])
                qvol = float(r.get("quote_volume_24h", "0") or 0)
                oi = float(r.get("oi", "0") or 0)
            except Exception:
                continue
            rows.append((ts, exchange, symbol, price, qvol, oi))

    rows.sort(key=lambda x: x[0])

    for _, ex, sym, *_ in rows:
        if ex == "BINANCE":
            binance_symbols.add(sym)
        elif ex == "BYBIT":
            bybit_symbols.add(sym)

    for sym in binance_symbols:
        symbol_states[("BINANCE", sym)] = SymbolState(exchange="BINANCE", symbol=sym)
    for sym in bybit_symbols:
        symbol_states[("BYBIT", sym)] = SymbolState(exchange="BYBIT", symbol=sym)

    print(f"[*] Replay universe: {len(binance_symbols)} Binance, {len(bybit_symbols)} Bybit")

    for ts, ex, sym, price, qvol, oi in rows:
        key = (ex, sym)
        st = symbol_states.get(key)
        if st is None:
            st = SymbolState(exchange=ex, symbol=sym)
            symbol_states[key] = st
        ts_ms = int(ts * 1000)
        st.update_from_ticker(ts_ms, price, qvol, qvol)
        if oi > 0:
            st.update_oi(oi, ts)
        st.update_scores()

        if int(ts) % 5 == 0:
            await process_trades_for_states(list(symbol_states.values()), ts)

    stats = await compute_expectancy_stats()
    print("\n=== Replay Summary ===")
    print(f"Closed trades: {stats['n']}")
    print(f"Wins: {stats['wins']} ({stats['winrate']:.1f}%)")
    print(f"Avg PnL: {stats['avg_pnl']:.6f}")
    print(f"Total PnL: {stats['total_pnl']:.6f}")


# ---------------------------------------------------------------------------
# Main (live vs replay)
# ---------------------------------------------------------------------------


async def main_live() -> None:
    global binance_symbols, bybit_symbols

    await init_db()

    async with aiohttp.ClientSession() as session:
        print("[*] Discovering Binance USDT perpetuals...")
        binance_list, binance_tick_map = await discover_binance_universe(session)
        binance_symbols = set(binance_list)
        print(f"    -> {len(binance_symbols)} Binance symbols")

        print("[*] Discovering Bybit linear USDT futures...")
        bybit_list, bybit_tick_map = await discover_bybit_universe(session)
        bybit_symbols = set(bybit_list)
        print(f"    -> {len(bybit_symbols)} Bybit symbols")

        async with state_lock:
            for sym in binance_symbols:
                key = ("BINANCE", sym)
                if key not in symbol_states:
                    symbol_states[key] = SymbolState(exchange="BINANCE", symbol=sym)
            for sym in bybit_symbols:
                key = ("BYBIT", sym)
                if key not in symbol_states:
                    symbol_states[key] = SymbolState(exchange="BYBIT", symbol=sym)

        print("[*] Bootstrapping snapshot...")
        await bootstrap_snapshots(binance_tick_map, bybit_tick_map)

        tasks = [
            asyncio.create_task(binance_ws_ticker_loop(), name="binance_ws_ticker"),
            asyncio.create_task(binance_ws_trade_loop(), name="binance_ws_trades"),
            asyncio.create_task(bybit_ws_loop(), name="bybit_ws"),
            asyncio.create_task(refresh_binance_oi(session), name="binance_oi"),
            asyncio.create_task(refresh_bybit_oi(session), name="bybit_oi"),
            asyncio.create_task(trade_engine_loop(), name="trade_engine"),
            asyncio.create_task(ui_loop(), name="ui"),
        ]

        try:
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            print("Shutting down...")
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--replay", type=str, help="Replay CSV file (timestamp,exchange,symbol,price,quote_volume_24h,oi)")
    return ap.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    if args.replay:
        asyncio.run(run_replay(args.replay))
    else:
        asyncio.run(main_live())
