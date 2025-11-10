#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Multi‑Coin Trade Monitor — v5.0 (Robust TA)

- SPEED: Uses `uvloop` for 30-40% faster I/O.
- SPEED: Uses `orjson` for 2-4x faster JSON parsing.
- LOGIC: `MarketAnalyzer` shows directional 'TREND (UP/DOWN)' or 'CHOPPY'.
- FIX: `MarketAnalyzer` now checks if kline data is long enough
       before calculating TA, preventing 'KeyError' on new coins.
- CONFLUENCE: Calculates "Probable Top" & "Probable Bottom".
- UI: "Pressure" gauge is a 21-sample live sparkline.
- All previous features: L2 Book, OI/Liq, Config, SQLite
"""
from __future__ import annotations

import asyncio, math, os, random, sys, time
import yaml
import sqlite3
from pathlib import Path
from dataclasses import dataclass, field
from typing import Deque, Dict, Optional, List
from collections import deque

# --- SPEED (uvloop/orjson) ---
try:
    import uvloop

    HAS_UVLOOP = True
except ImportError:
    HAS_UVLOOP = False
    pass

try:
    import orjson as json  # Use orjson as 'json'

    HAS_ORJSON = True
except ImportError:
    import json  # Fallback to standard json

    HAS_ORJSON = False
# --- END SPEED ---

# --- LOGIC (MarketAnalyzer) ---
try:
    import pandas as pd
    import pandas_ta as ta

    HAS_TA = True
except ImportError:
    HAS_TA = False
# --- END LOGIC ---

import aiohttp, websockets
from loguru import logger
import structlog, psutil
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich import box

BYBIT_WS = "wss://stream.bybit.com/v5/public/linear"
BYBIT_HTTP = "https://api.bybit.com"
BINANCE_FUT_WS_TMPL = "wss://fstream.binance.com/ws/{stream}"
BINANCE_HTTP = "https://fapi.binance.com"

HEARTBEAT_SECS = 20
RECONNECT_MAX_ATTEMPTS = 4
WATCHDOG_STALE_SECS = 12
FALLBACK_AFTER_SECS = 20


def now_ts() -> float:
    return time.time()


def safe_div(a: float, b: float, eps: float = 1e-9) -> float:
    return a / b if abs(b) > eps else 0.0


def ema(prev: Optional[float], x: float, alpha: float) -> float:
    return (1 - alpha) * prev + alpha * x if prev is not None else x


def human_bytes(n: int) -> str:
    s = float(n)
    for u in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if s < 1024:
            return f"{s:.1f} {u}"
        s /= 1024
    return f"{s:.1f} EB"


def say_mac(msg: str) -> None:
    try:
        if sys.platform == "darwin":
            safe = msg.replace('"', "").replace("'", "")
            cmd = f'osascript -e "say \\"{safe}\\"" >/dev/null 2>&1'
            os.system(cmd)
    except Exception:
        pass


# --- UI (Pressure Sparkline) ---
def create_pressure_sparkline(history: Deque[float], width: int) -> str:
    """Creates a rich-formatted pressure sparkline string."""
    data = list(history)
    if not data:
        return "[dim]" + "·" * width + "[/dim]"

    # Sparkline characters from dimmest to brightest
    sparks = " ▂▃▄▅▆▇█"  # 9 levels (index 0-8)

    output_chars = []
    for score in data:
        if score > 0.05:
            # Map (0.05 to 1.0) to (index 1 to 8)
            idx = int(score * 8)
            idx = min(max(1, idx), 8)  # Clamp to 1-8 range
            output_chars.append(f"[green]{sparks[idx]}[/green]")
        elif score < -0.05:
            # Map (-0.05 to -1.0) to (index 1 to 8)
            idx = int(abs(score) * 8)
            idx = min(max(1, idx), 8)  # Clamp to 1-8 range
            output_chars.append(f"[red]{sparks[idx]}[/red]")
        else:
            output_chars.append("[dim]·[/dim]")  # Neutral dot

    # Add padding for when the deque is not full
    padding_count = width - len(output_chars)
    padding_str = "[dim]" + "·" * padding_count + "[/dim]"

    return padding_str + "".join(output_chars)


# --- END UI ---


# --------------------------- Data ---------------------------


@dataclass
class Trade:
    ts: float
    price: float
    qty: float
    side: str


@dataclass
class BookSide:
    ts: float
    bids: Dict[float, float] = field(default_factory=dict)
    asks: Dict[float, float] = field(default_factory=dict)


@dataclass
class Liquidation:
    ts: float
    side: str
    qty: float
    price: float


@dataclass
class RollingState:
    trades_5m: Deque[Trade] = field(default_factory=deque)
    trades_1h: Deque[Trade] = field(default_factory=deque)
    l2_bids: Dict[float, float] = field(default_factory=dict)
    l2_asks: Dict[float, float] = field(default_factory=dict)
    l2_book_ready: bool = False
    liquidations_1m: Deque[Liquidation] = field(default_factory=deque)
    open_interest: float = 0.0
    oi_ema: Optional[float] = None
    last_price: Optional[float] = None
    cvd_5m: float = 0.0
    cvd_1h: float = 0.0
    trade_count_5m: int = 0
    qty_sum_5m: float = 0.0

    # --- FOR PRESSURE SPARKLINE ---
    trades_1s: Deque[Trade] = field(default_factory=deque)
    cvd_1s: float = 0.0  # 1-second rolling CVD
    vol_1s: float = 0.0  # 1-second rolling total volume
    # --- END ---

    mom_ema: Optional[float] = None


# --------------------------- Features ---------------------------


class FeatureEngine:
    def __init__(
        self,
        s_state: RollingState,
        symbol: str,
        weights: Dict[str, float],
        windows: Dict[str, int],
    ):
        self.state = s_state
        self.symbol = symbol.upper()
        self.weights = weights
        self.windows = windows

    def update_trade(self, t: Trade) -> None:
        st = self.state
        st.last_price = t.price
        s = (t.side or "Buy").lower()[0]
        signed = t.qty if s == "b" else -t.qty

        # --- FOR PRESSURE SPARKLINE ---
        st.trades_1s.append(t)
        st.cvd_1s += signed
        st.vol_1s += t.qty
        # --- END ---

        st.trades_5m.append(t)
        st.cvd_5m += signed
        st.trade_count_5m += 1
        st.qty_sum_5m += t.qty
        st.trades_1h.append(t)
        st.cvd_1h += signed

        c5 = t.ts - self.windows.get("trade_sec", 300)
        while st.trades_5m and st.trades_5m[0].ts < c5:
            old = st.trades_5m.popleft()
            s0 = old.side.lower()[0]
            st.cvd_5m -= old.qty if s0 == "b" else -old.qty
            st.trade_count_5m = max(0, st.trade_count_5m - 1)
            st.qty_sum_5m = max(0.0, st.qty_sum_5m - old.qty)

        c1 = t.ts - self.windows.get("trade_sec_long", 3600)
        while st.trades_1h and st.trades_1h[0].ts < c1:
            old = st.trades_1h.popleft()
            s0 = old.side.lower()[0]
            st.cvd_1h -= old.qty if s0 == "b" else -old.qty

        # --- FOR PRESSURE SPARKLINE: Prune 1-second window ---
        cut_1s = t.ts - 1.0  # 1.0 second window
        while st.trades_1s and st.trades_1s[0].ts < cut_1s:
            old_1s = st.trades_1s.popleft()
            s_old = old_1s.side.lower()[0]
            st.cvd_1s -= old_1s.qty if s_old == "b" else -old_1s.qty
            st.vol_1s = max(0.0, st.vol_1s - old_1s.qty)
        # --- END ---

        if len(st.trades_5m) >= 2:
            p0 = st.trades_5m[-2].price
            dp = t.price - p0
            sign = 1.0 if s == "b" else -1.0
            st.mom_ema = ema(st.mom_ema, sign * dp, alpha=0.2)

    def update_liquidation(self, liq: Liquidation) -> None:
        st = self.state
        st.liquidations_1m.append(liq)
        cut = liq.ts - self.windows.get("liq_sec", 60)
        while st.liquidations_1m and st.liquidations_1m[0].ts < cut:
            st.liquidations_1m.popleft()

    def features(self) -> Dict[str, float]:
        st = self.state

        # --- FOR PRESSURE SPARKLINE ---
        pressure_score = safe_div(st.cvd_1s, st.vol_1s)
        # --- END ---

        cvd_norm = safe_div(st.cvd_5m, abs(st.cvd_1h)) if abs(st.cvd_1h) > 0 else 0.0
        obi = 0.0
        if st.l2_book_ready:
            bid_prices = sorted(st.l2_bids.keys(), reverse=True)
            ask_prices = sorted(st.l2_asks.keys())
            bid_sum = sum(st.l2_bids[p] for p in bid_prices[:25])
            ask_sum = sum(st.l2_asks[p] for p in ask_prices[:25])
            den = (bid_sum + ask_sum) or 1.0
            obi = math.tanh((bid_sum - ask_sum) / den)
        mom = st.mom_ema or 0.0
        tc = st.trade_count_5m
        qsum = st.qty_sum_5m
        vol_spike = math.tanh((tc**0.5) * (math.log1p(qsum + 1e-9)) / 20.0)

        oi_ema_val = st.oi_ema if st.oi_ema is not None else st.open_interest
        oi_delta = st.open_interest - oi_ema_val
        f_oi = math.tanh(safe_div(oi_delta, oi_ema_val * 0.01 + 1e-9))

        liq_vol_1m = sum(L.qty for L in st.liquidations_1m)
        f_liq = math.tanh(safe_div(liq_vol_1m, st.qty_sum_5m + 1e-9))

        f_cvd = max(-1, min(1, cvd_norm))
        f_obi = max(-1, min(1, obi))
        norm = abs(st.last_price or 1.0) * 0.001
        f_mom = max(-1, min(1, mom / norm)) if norm > 0 else 0.0
        f_vol = max(-1, min(1, vol_spike))

        w = self.weights
        score = max(
            -1.0,
            min(
                1.0,
                w["cvd"] * f_cvd
                + w["obi"] * f_obi
                + w["momentum"] * f_mom
                + w["vol"] * f_vol
                + w["oi"] * f_oi
                + w["liq"] * f_liq,
            ),
        )
        return dict(
            price=st.last_price or float("nan"),
            cvd_5m=st.cvd_5m,
            cvd_1h=st.cvd_1h,
            cvd_norm=f_cvd,
            obi=f_obi,
            mom=f_mom,
            vol=f_vol,
            oi_delta=f_oi,
            liq_spike=f_liq,
            score=score,
            pressure_score=pressure_score,
            trades_5m=len(st.trades_5m),
            qty_5m=st.qty_sum_5m,
        )


# --------------------------- Streams ---------------------------


def ws_dumps(data: Dict) -> str:
    """Uses orjson to dump json, decodes to str for websocket."""
    if HAS_ORJSON:
        return json.dumps(data).decode("utf-8")
    return json.dumps(data)


class BybitStream:
    def __init__(self, symbol: str):
        self.symbol = symbol.upper()
        self.ws = None

    async def __aenter__(self):
        self.ws = await websockets.connect(BYBIT_WS, ping_interval=None, max_queue=1000)
        await self.ws.send(
            ws_dumps(
                {
                    "op": "subscribe",
                    "args": [
                        f"publicTrade.{self.symbol}",
                        f"orderbook.50.{self.symbol}",
                        f"tickers.{self.symbol}",
                        f"liquidation.{self.symbol}",
                    ],
                }
            )
        )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.ws:
            await self.ws.close()
            self.ws = None

    async def recv(self) -> dict:
        msg = await asyncio.wait_for(self.ws.recv(), timeout=HEARTBEAT_SECS)
        return json.loads(msg)  # Uses orjson.loads

    async def ping(self):
        if self.ws:
            await self.ws.send(ws_dumps({"op": "ping"}))


class BinanceFutFallback:
    def __init__(self, symbol: str):
        sym = symbol.lower()
        stream = f"{sym if sym.endswith('usdt') else sym+'usdt'}@aggTrade"
        self.url = BINANCE_FUT_WS_TMPL.format(stream=stream)
        self.ws = None

    async def __aenter__(self):
        self.ws = await websockets.connect(self.url, ping_interval=None, max_queue=1000)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.ws:
            await self.ws.close()
            self.ws = None

    async def recv(self) -> dict:
        raw = await asyncio.wait_for(self.ws.recv(), timeout=HEARTBEAT_SECS)
        return json.loads(raw)  # Uses orjson.loads


# --------------------------- Warm Start ---------------------------


class WarmClient:
    def __init__(self, session: aiohttp.ClientSession):
        self.sess = session

    async def bybit_price_funding(self, symbol: str) -> Dict[str, Optional[float]]:
        res = {"price": None, "funding": None}
        sym = symbol.upper()
        try:
            url = f"{BYBIT_HTTP}/v5/market/tickers?category=linear&symbol={sym}"
            async with self.sess.get(url, timeout=aiohttp.ClientTimeout(total=6)) as r:
                if r.status == 200:
                    j = await r.json(loads=json.loads)  # Use orjson.loads
                    lst = j.get("result", {}).get("list", [])
                    if lst:
                        it = lst[0]
                        p = it.get("lastPrice")
                        fr = it.get("fundingRate")
                        res["price"] = float(p) if p is not None else None
                        res["funding"] = float(fr) if fr is not None else None
        except Exception:
            pass
        if res["price"] is None:
            try:
                url = f"{BINANCE_HTTP}/fapi/v1/ticker/price?symbol={sym}"
                async with self.sess.get(
                    url, timeout=aiohttp.ClientTimeout(total=6)
                ) as r:
                    if r.status == 200:
                        j = await r.json(loads=json.loads)  # Use orjson.loads
                        p = j.get("price")
                        if p is not None:
                            res["price"] = float(p)
            except Exception:
                pass
        return res


# --- LOGIC (Directional Regime) ---
class MarketAnalyzer:
    """
    Fetches public kline data and calculates:
    1. Regime (CHOPPY, TREND (UP), TREND (DOWN))
    2. Structural High/Low
    """

    def __init__(
        self, symbol: str, config: Dict[str, any], http_session: aiohttp.ClientSession
    ):
        self.symbol = symbol
        self.http_session = http_session
        self.config = config

        self.interval = str(config.get("kline_interval", "60"))
        self.lookback = int(config.get("kline_lookback", 100))
        self.refresh_secs = int(config.get("refresh_secs", 900))

        self.chop_len = int(config.get("chop_length", 14))
        self.chop_th = float(config.get("chop_threshold", 61.8))
        self.adx_len = int(config.get("adx_length", 14))
        self.adx_th = float(config.get("adx_threshold", 25))

        self.regime = "..."  # Default on startup
        self.structural_high: Optional[float] = None
        self.structural_low: Optional[float] = None

        self.last_refresh_ts = 0.0
        self.is_enabled = config.get("enabled", False) and HAS_TA

        if self.is_enabled:
            logger.info(
                f"{symbol}: MarketAnalyzer enabled (ADX<{self.adx_th} | CHOP>{self.chop_th})"
            )
        elif not HAS_TA:
            logger.warning(
                f"{symbol}: MarketAnalyzer disabled. `pandas` or `pandas-ta` not found."
            )

    async def fetch_klines(self) -> Optional[pd.DataFrame]:
        """Fetches public klines from Bybit."""
        params = {
            "category": "linear",
            "symbol": self.symbol,
            "interval": self.interval,
            "limit": self.lookback,
        }
        # --- FIX: Corrected URL from /klines to /kline ---
        url = f"{BYBIT_HTTP}/v5/market/kline"

        try:
            async with self.http_session.get(url, params=params, timeout=5) as r:
                if r.status == 200:
                    data = await r.json(loads=json.loads)  # Use orjson.loads
                    kline_list = data.get("result", {}).get("list", [])
                    if not kline_list:
                        logger.warning(f"{self.symbol}: No kline data returned.")
                        return None

                    df = pd.DataFrame(
                        kline_list,
                        columns=[
                            "ts",
                            "open",
                            "high",
                            "low",
                            "close",
                            "volume",
                            "turnover",
                        ],
                    )
                    df = df.iloc[::-1].reset_index(
                        drop=True
                    )  # Reverse to chronological
                    df["ts"] = pd.to_numeric(df["ts"])
                    df["open"] = pd.to_numeric(df["open"])
                    df["high"] = pd.to_numeric(df["high"])
                    df["low"] = pd.to_numeric(df["low"])
                    df["close"] = pd.to_numeric(df["close"])
                    return df
                else:
                    logger.error(
                        f"{self.symbol}: Failed to fetch klines (HTTP {r.status}): {await r.text()}"
                    )
                    return None
        except Exception as e:
            logger.error(f"{self.symbol}: Error fetching klines: {e}")
            return None

    def calculate_analysis(self, df: pd.DataFrame):
        """Calculates indicators, regime, AND structure."""
        try:
            # --- 1. Structure ---
            self.structural_high = df["high"].max()
            self.structural_low = df["low"].min()

            # --- FIX: Check if DataFrame is long enough for TA ---
            min_rows_needed = 50  # Safe buffer for ADX(14) and CHOP(14)
            if len(df) < min_rows_needed:
                logger.warning(
                    f"{self.symbol}: Kline data is too short ({len(df)} rows) to calculate TA. Need {min_rows_needed}."
                )
                self.regime = "N/A"  # Set regime to N/A
                return  # Skip TA calculation
            # --- END FIX ---

            # --- 2. Regime ---
            df.ta.chop(length=self.chop_len, append=True)
            df.ta.adx(length=self.adx_len, append=True)

            # Get the very last (current) values
            last_chop = df.iloc[-1][f"CHOP_{self.chop_len}"]
            last_adx = df.iloc[-1][f"ADX_{self.adx_len}"]
            last_dmp = df.iloc[-1][f"DMP_{self.adx_len}"]  # +DI line
            last_dmn = df.iloc[-1][f"DMN_{self.adx_len}"]  # -DI line

            if (
                pd.isna(last_chop)
                or pd.isna(last_adx)
                or pd.isna(last_dmp)
                or pd.isna(last_dmn)
            ):
                logger.warning(
                    f"{self.symbol}: TA indicators are NaN (likely insufficient data), skipping regime update."
                )
                return

            # --- The New Directional Regime Logic ---
            new_regime = self.regime
            if last_chop > self.chop_th or last_adx < self.adx_th:
                new_regime = "CHOPPY"
            elif last_dmp > last_dmn:
                new_regime = "TREND (UP)"
            else:  # last_dmn >= last_dmp
                new_regime = "TREND (DOWN)"

            if new_regime != self.regime:
                logger.info(
                    f"{self.symbol}: Regime change -> {new_regime} (CHOP:{last_chop:.1f} ADX:{last_adx:.1f} +DI:{last_dmp:.1f} -DI:{last_dmn:.1f})"
                )
                self.regime = new_regime

        except Exception as e:
            logger.error(f"{self.symbol}: Error calculating TA: {e}")

    async def check_update(self):
        """Public method to check if a refresh is needed."""
        if not self.is_enabled:
            self.regime = "N/A"  # Set to N/A if disabled
            return

        now = now_ts()
        # --- FIX: Run analysis immediately on first call (last_save_ts == 0) ---
        if (
            self.last_refresh_ts == 0
            or (now - self.last_refresh_ts) > self.refresh_secs
        ):
            df = await self.fetch_klines()
            if df is not None:
                self.calculate_analysis(df)
            self.last_refresh_ts = now  # Update timestamp
        # --- END FIX ---

    def get_regime(self) -> str:
        """Returns the current stored regime."""
        return self.regime

    def get_structure(self) -> Dict[str, Optional[float]]:
        """Returns the current structural levels."""
        return {"high": self.structural_high, "low": self.structural_low}


# --- END LOGIC ---


# --------------------------- Symbol monitor ---------------------------


class SymbolMonitor:
    def __init__(
        self,
        symbol: str,
        side: Optional[str],
        weights: Dict[str, float],
        thresholds: Dict[str, float],
        tuning: Dict[str, any],
        windows: Dict[str, int],
        alerts: bool,
        event_q: "asyncio.Queue[None]",
        console: Console,
        http_session: aiohttp.ClientSession,
        db_conn: sqlite3.Connection,
        market_analyzer: MarketAnalyzer,
        mag_levels_cfg: Dict[str, any],
    ):
        self.symbol = symbol.upper()
        self.side = side.lower() if isinstance(side, str) else None
        self.state = RollingState()

        self.weights = weights
        self.thresholds = thresholds
        self.tuning = tuning
        self.windows = windows

        self.alpha_score = tuning.get("alpha_score", 0.33)
        self.dwell_secs = tuning.get("dwell_secs", 2.5)
        self.hyst = tuning.get("hysteresis", 0.04)
        self.cooldown_secs = tuning.get("cooldown_secs", 10.0)

        self.mom_alpha = tuning.get("momentum_gate", {}).get("alpha", 0.40)
        self.mom_slope_alpha = tuning.get("momentum_gate", {}).get("slope_alpha", 0.50)
        self.mom_mag_th = tuning.get("momentum_gate", {}).get("mag_threshold", 0.06)
        self.mom_slope_th = tuning.get("momentum_gate", {}).get(
            "slope_threshold", 0.020
        )
        self.score_min_th = tuning.get("momentum_gate", {}).get(
            "score_min_threshold", 0.10
        )

        self.fe = FeatureEngine(self.state, self.symbol, self.weights, self.windows)
        self.fallback_used = False
        self.alerts = alerts
        self._next_warn = 0.0
        self.event_q = event_q
        self.console = console
        self.http_session = http_session
        self.last_trade_ts = 0.0
        self.status = "connecting…"

        self.last_bucket: Optional[str] = None
        self._bucket_pending: Optional[str] = None
        self._bucket_since: float = 0.0
        self.score_smooth: Optional[float] = None
        self.guidance_text: str = "—"
        self.mom_smooth: float = 0.0
        self._mom_prev: float = 0.0
        self.mom_slope_smooth: float = 0.0
        self.last_switch_ts: float = 0.0

        # --- DB PERSIST ---
        self.db_conn = db_conn
        self.save_interval = tuning.get("save_interval_secs", 60.0)
        self.last_save_ts = 0.0
        self._load_state_from_db()

        # --- CONFLUENCE (v4) ---
        self.market_analyzer = market_analyzer
        self.mag_levels_enabled = mag_levels_cfg.get("enabled", False)
        self.book_scan_depth_pct = (
            mag_levels_cfg.get("book_scan_depth_percent", 5.0) / 100.0
        )
        self.book_min_wall_usd = mag_levels_cfg.get("book_min_wall_usd", 500000)

        # --- UI (Pressure Sparkline) ---
        self.pressure_history: Deque[float] = deque(maxlen=21)

    def _load_state_from_db(self):
        """Warm-starts the monitor's state from the SQLite database."""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(
                """
                SELECT 
                    last_price, cvd_1h, mom_ema, 
                    score_smooth, mom_smooth, mom_slope_smooth, oi_ema
                FROM symbol_state WHERE symbol = ?
                """,
                (self.symbol,),
            )
            row = cursor.fetchone()
            if row:
                self.state.last_price = row[0] if row[0] is not None else None
                self.state.cvd_1h = row[1] if row[1] is not None else 0.0
                self.state.mom_ema = row[2] if row[2] is not None else None
                self.score_smooth = row[3] if row[3] is not None else None
                self.mom_smooth = row[4] if row[4] is not None else 0.0
                self.mom_slope_smooth = row[5] if row[5] is not None else 0.0
                self.state.oi_ema = row[6] if row[6] is not None else None
                self._mom_prev = self.mom_smooth
                logger.info(f"{self.symbol}: Warmed state from DB")
        except Exception as e:
            logger.error(f"{self.symbol}: Failed to load state from DB: {e}")
        finally:
            if cursor:
                cursor.close()

    def _save_state_to_db(self):
        """Saves the monitor's current state to the SQLite database."""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(
                """
                INSERT OR REPLACE INTO symbol_state (
                    symbol, last_updated, last_price, cvd_1h, mom_ema,
                    score_smooth, mom_smooth, mom_slope_smooth, oi_ema
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    self.symbol,
                    now_ts(),
                    self.state.last_price,
                    self.state.cvd_1h,
                    self.state.mom_ema,
                    self.score_smooth,
                    self.mom_smooth,
                    self.mom_slope_smooth,
                    self.state.oi_ema,
                ),
            )
            self.db_conn.commit()
            self.last_save_ts = now_ts()
        except Exception as e:
            logger.error(f"{self.symbol}: Failed to save state to DB: {e}")
        finally:
            if cursor:
                cursor.close()

    async def warm_start(self, warm: WarmClient, store: Dict[str, Dict[str, str]]):
        # --- FIX: Run analyzer once on startup ---
        logger.info(f"{self.symbol}: Running initial market analysis...")
        await self.market_analyzer.check_update()

        snap = await warm.bybit_price_funding(self.symbol)
        price_to_use = self.state.last_price
        if price_to_use is None:
            price_to_use = snap.get("price")

        # Now the first _update_row will have data
        self._update_row(
            store,
            snap.get("funding"),
            price_override=price_to_use,
            status="connecting…",
        )
        try:
            self.event_q.put_nowait(None)
        except asyncio.QueueFull:
            pass

    async def run(self, warm: WarmClient, store: Dict[str, Dict[str, str]]):
        await self.warm_start(warm, store)
        attempts = 0
        while True:
            try:
                async with BybitStream(self.symbol) as stream:
                    self.fallback_used = False
                    attempts = 0
                    self.status = "live"
                    await self._loop_bybit(stream, warm, store)
            except (
                asyncio.TimeoutError,
                websockets.ConnectionClosedError,
                websockets.WebSocketException,
                OSError,
            ) as e:
                attempts += 1
                self.status = "error"
                logger.warning(
                    f"{self.symbol}: Bybit error {e} {attempts}/{RECONNECT_MAX_ATTEMPTS}"
                )
                if attempts >= RECONNECT_MAX_ATTEMPTS:
                    await self._loop_binance_fallback(warm, store)
                    attempts = 0
                await asyncio.sleep(min(60, 2**attempts + random.random()))

    def _handle_l2_book(self, data: dict, msg_type: str):
        st = self.state
        if msg_type == "snapshot":
            st.l2_bids.clear()
            st.l2_asks.clear()
            for price_str, size_str in data.get("b", []):
                st.l2_bids[float(price_str)] = float(size_str)
            for price_str, size_str in data.get("a", []):
                st.l2_asks[float(price_str)] = float(size_str)
            st.l2_book_ready = True
        elif msg_type == "delta" and st.l2_book_ready:
            for price_str, size_str in data.get("b", []):
                price = float(price_str)
                if size_str == "0":
                    st.l2_bids.pop(price, None)
                else:
                    st.l2_bids[price] = float(size_str)
            for price_str, size_str in data.get("a", []):
                price = float(price_str)
                if size_str == "0":
                    st.l2_asks.pop(price, None)
                else:
                    st.l2_asks[price] = float(size_str)

    def _find_magnetic_levels(self) -> Dict[str, Optional[float]]:
        """Finds magnetic levels by combining L2 walls and kline structure."""

        structure = self.market_analyzer.get_structure()
        struct_high = structure.get("high")
        struct_low = structure.get("low")

        price = self.state.last_price
        if not price or not self.state.l2_book_ready:
            return {"top": struct_high, "bottom": struct_low}

        scan_dist = price * self.book_scan_depth_pct
        min_bid_px = price - scan_dist
        max_ask_px = price + scan_dist

        best_bid_wall_px = None
        best_bid_wall_usd = 0
        for p, s in self.state.l2_bids.items():
            if p < min_bid_px:
                continue
            usd_val = p * s
            if usd_val > best_bid_wall_usd and usd_val > self.book_min_wall_usd:
                best_bid_wall_usd = usd_val
                best_bid_wall_px = p

        best_ask_wall_px = None
        best_ask_wall_usd = 0
        for p, s in self.state.l2_asks.items():
            if p > max_ask_px:
                continue
            usd_val = p * s
            if usd_val > best_ask_wall_usd and usd_val > self.book_min_wall_usd:
                best_ask_wall_usd = usd_val
                best_ask_wall_px = p

        probable_top = struct_high
        if best_ask_wall_px:
            if probable_top is None:
                probable_top = best_ask_wall_px
            else:
                probable_top = min(probable_top, best_ask_wall_px)

        probable_bottom = struct_low
        if best_bid_wall_px:
            if probable_bottom is None:
                probable_bottom = best_bid_wall_px
            else:
                probable_bottom = max(probable_bottom, best_bid_wall_px)

        return {"top": probable_top, "bottom": probable_bottom}

    def _decide_guidance(self, score: float) -> str:
        self.score_smooth = ema(self.score_smooth, score, alpha=self.alpha_score)
        raw = score
        sm = self.score_smooth if self.score_smooth is not None else score
        last = self.last_bucket
        now = now_ts()
        if self.side == "long":
            add_thr = self.thresholds["long_add"] + (
                self.hyst if last != "add" else 0.0
            )
            reduce_thr = self.thresholds["long_reduce"] - (
                self.hyst if last != "reduce" else 0.0
            )
            score_ok = sm > self.score_min_th
            mom_dir_ok = (self.mom_smooth > self.mom_mag_th) and (
                self.mom_slope_smooth > self.mom_slope_th
            )
            opp_dir_ok = (self.mom_smooth < -self.mom_mag_th) and (
                self.mom_slope_smooth < -self.mom_slope_th
            )
        else:
            add_thr = self.thresholds["short_add"] - (
                self.hyst if last != "add" else 0.0
            )
            reduce_thr = self.thresholds["short_reduce"] + (
                self.hyst if last != "reduce" else 0.0
            )
            score_ok = sm < -self.score_min_th
            mom_dir_ok = (self.mom_smooth < -self.mom_mag_th) and (
                self.mom_slope_smooth < -self.mom_slope_th
            )
            opp_dir_ok = (self.mom_smooth > self.mom_mag_th) and (
                self.mom_slope_smooth > self.mom_slope_th
            )

        def bucket_for(x: float) -> str:
            if self.side == "long":
                if x > add_thr:
                    return "add"
                if x <= reduce_thr:
                    return "reduce"
                return "caution"
            else:
                if x < add_thr:
                    return "add"
                if x >= reduce_thr:
                    return "reduce"
                return "caution"

        cand_sm = bucket_for(sm)
        cand_raw = bucket_for(raw)

        if (now - self.last_switch_ts) < self.cooldown_secs:
            if self.last_bucket is None:
                self.guidance_text = "⚠️ caution"
            return self.guidance_text

        gate_forward = mom_dir_ok and score_ok
        gate_reverse = opp_dir_ok and (not score_ok)
        cand = self.last_bucket or "caution"

        if self.last_bucket is None:
            if gate_forward:
                if cand_sm != "caution":
                    cand = cand_sm
                elif (
                    cand_raw != "caution"
                    and (now - getattr(self, "_bucket_since", 0.0)) >= self.dwell_secs
                ):
                    cand = cand_raw
            else:
                cand = "caution"
        else:
            if (self.last_bucket == "add" and gate_reverse) or (
                self.last_bucket == "reduce" and gate_forward
            ):
                if cand_sm != "caution":
                    cand = cand_sm
                else:
                    if cand_raw != self.last_bucket:
                        if self._bucket_pending != cand_raw:
                            self._bucket_pending, self._bucket_since = cand_raw, now
                        elif now - self._bucket_since >= self.dwell_secs:
                            cand = cand_raw
            else:
                if cand_sm == "caution" and not gate_forward and not gate_reverse:
                    cand = "caution"
                else:
                    cand = self.last_bucket

        if cand != self.last_bucket:
            self.last_bucket = cand
            self.last_switch_ts = now
            alert_msg = ""
            if cand == "add":
                self.guidance_text = "✅ hold/add"
                alert_msg = f"{self.symbol} ({self.side}): {self.guidance_text}"
            elif cand == "reduce":
                self.guidance_text = "❌ reduce/exit"
                alert_msg = f"{self.symbol} ({self.side}): {self.guidance_text}"
            else:
                self.guidance_text = "⚠️ caution"
                alert_msg = f"{self.symbol} ({self.side}): {self.guidance_text}"

            current_regime = self.market_analyzer.get_regime()
            if self.alerts and alert_msg and current_regime.startswith("TREND"):
                try:
                    logger.bind(alert=True).info(alert_msg)
                except Exception:
                    pass
                try:
                    self.console.bell()
                    if sys.platform == "darwin":
                        say_mac(f"{self.symbol} {cand}")
                except Exception:
                    pass

        return self.guidance_text

    async def _loop_bybit(
        self, stream: BybitStream, warm: WarmClient, store: Dict[str, Dict[str, str]]
    ):
        fr = None
        fr_ts = 0.0
        idle_start = now_ts()
        while True:
            await self.market_analyzer.check_update()

            if now_ts() - fr_ts > 45:
                snap = await warm.bybit_price_funding(self.symbol)
                if snap.get("funding") is not None:
                    fr = snap["funding"]
                if snap.get("price") is not None and self.state.last_price is None:
                    self.state.last_price = snap["price"]
                fr_ts = now_ts()

            try:
                msg = await asyncio.wait_for(stream.recv(), timeout=HEARTBEAT_SECS)
            except asyncio.TimeoutError:
                await stream.ping()
                if (now_ts() - idle_start) > FALLBACK_AFTER_SECS:
                    await self._loop_binance_fallback(warm, store)
                    idle_start = now_ts()
                continue

            if not isinstance(msg, dict):
                continue
            if msg.get("op") in ("pong", "ping"):
                continue
            topic = msg.get("topic", "")
            data = msg.get("data")
            dirty = False

            if topic.startswith("publicTrade.") and isinstance(data, list):
                for t in data:
                    try:
                        ts = float(t["T"]) / 1000.0
                        price = float(t["p"])
                        qty = float(t["v"])
                        side = str(t["S"])
                    except Exception:
                        continue
                    self.last_trade_ts = ts
                    self.fe.update_trade(Trade(ts, price, qty, side))
                    dirty = True
                idle_start = now_ts()
            elif topic.startswith("orderbook.50.") and isinstance(data, dict):
                try:
                    msg_type = msg.get("type")
                    if msg_type:
                        self._handle_l2_book(data, msg_type)
                        dirty = True
                except Exception:
                    pass
            elif topic.startswith("tickers.") and isinstance(data, dict):
                try:
                    oi = data.get("openInterest")
                    if oi is not None:
                        new_oi = float(oi)
                        self.state.open_interest = new_oi
                        self.state.oi_ema = ema(self.state.oi_ema, new_oi, alpha=0.05)
                        dirty = True
                except Exception:
                    pass
            elif topic.startswith("liquidation.") and isinstance(data, dict):
                try:
                    ts = float(data["execTime"]) / 1000.0
                    side = str(data["side"])
                    qty = float(data["size"])
                    price = float(data["price"])
                    self.fe.update_liquidation(Liquidation(ts, side, qty, price))
                    dirty = True
                except Exception:
                    pass

            if dirty:
                self.status = "live"
                self._update_row(store, fr)
                try:
                    self.event_q.put_nowait(None)
                except asyncio.QueueFull:
                    pass
            if (
                self.last_trade_ts
                and now_ts() - self.last_trade_ts > WATCHDOG_STALE_SECS
                and now_ts() >= getattr(self, "_next_warn", 0.0)
            ):
                logger.warning(f"{self.symbol}: stale trades")
                self._next_warn = now_ts() + STALE_WARN_EVERY

    async def _loop_binance_fallback(
        self, warm: WarmClient, store: Dict[str, Dict[str, str]]
    ):
        self.fallback_used = True
        self.status = "fallback"
        fr = None
        sym = self.symbol.lower()
        stream = f"{sym if sym.endswith('usdt') else sym+'usdt'}@aggTrade"
        url = BINANCE_FUT_WS_TMPL.format(stream=stream)
        async with websockets.connect(url, ping_interval=None, max_queue=1000) as ws:
            while True:
                await self.market_analyzer.check_update()

                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=HEARTBEAT_SECS)
                except asyncio.TimeoutError:
                    await ws.ping()
                    self._update_row(store, fr, status="fallback")
                    try:
                        self.event_q.put_nowait(None)
                    except asyncio.QueueFull:
                        pass
                    continue
                except (
                    websockets.ConnectionClosedError,
                    websockets.WebSocketException,
                    OSError,
                ):
                    self.status = "error"
                    break
                try:
                    data = json.loads(msg)
                    payload = data.get("data") or data
                    p = float(payload["p"])
                    q = float(payload["q"])
                    side = "Sell" if payload.get("m", False) else "Buy"
                    self.fe.update_trade(Trade(now_ts(), p, q, side))
                except Exception:
                    pass
                self._update_row(store, fr, status="fallback")
                try:
                    self.event_q.put_nowait(None)
                except asyncio.QueueFull:
                    pass

    def _update_row(
        self,
        store: Dict[str, Dict[str, str]],
        fr: Optional[float],
        *,
        price_override: Optional[float] = None,
        status: Optional[str] = None,
    ):
        if price_override is not None:
            self.state.last_price = price_override

        feats = self.fe.features()
        price = feats.get("price")
        score = float(feats.get("score", 0.0))
        pressure_score = feats.get("pressure_score", 0.0)

        self.pressure_history.append(pressure_score)

        cvd_norm = feats.get("cvd_norm", 0.0)
        obi = feats.get("obi", 0.0)
        mom = feats.get("mom", 0.0)
        vol = feats.get("vol", 0.0)
        oi_delta = feats.get("oi_delta", 0.0)
        liq_spike = feats.get("liq_spike", 0.0)

        self.mom_smooth = (
            1.0 - self.mom_alpha
        ) * self.mom_smooth + self.mom_alpha * mom
        mom_slope_raw = self.mom_smooth - self._mom_prev
        self.mom_slope_smooth = (
            1.0 - self.mom_slope_alpha
        ) * self.mom_slope_smooth + self.mom_slope_alpha * mom_slope_raw
        self._mom_prev = self.mom_smooth

        guid = self._decide_guidance(score) if self.side in ("long", "short") else "—"
        view_status = (
            status or self.status or ("fallback" if self.fallback_used else "live")
        )

        mag_levels = {"top": None, "bottom": None}
        if self.mag_levels_enabled:
            mag_levels = self._find_magnetic_levels()

        def format_level(level):
            if isinstance(level, (float, int)):
                if level > 100:
                    return f"{level:.2f}"
                if level > 1:
                    return f"{level:.4f}"
                return f"{level:.6f}"
            return "—"

        store[self.symbol] = dict(
            Status=view_status,
            Source=("Binance" if self.fallback_used else "Bybit"),
            Side=(self.side or "—"),
            Regime=self.market_analyzer.get_regime(),
            Price=(f"{price:.6f}" if isinstance(price, (float, int)) else "—"),
            Probable_Top=format_level(mag_levels["top"]),
            Probable_Bot=format_level(mag_levels["bottom"]),
            Funding=(f"{fr:+.5f}" if fr is not None else "—"),
            Score=f"{score:+.3f}",
            Pressure=create_pressure_sparkline(self.pressure_history, 21),
            CVD=f"{cvd_norm:+.3f}",
            OBI=f"{obi:+.3f}",
            MOM=f"{mom:+.3f}",
            VOL=f"{vol:+.3f}",
            OI_D=f"{oi_delta:+.3f}",
            LIQ=f"{liq_spike:+.3f}",
            Guidance=guid,
        )

        now = now_ts()
        if (now - self.last_save_ts) > self.save_interval:
            self._save_state_to_db()


# --------------------------- Table ---------------------------


def render_table(store: Dict[str, Dict[str, str]]) -> Panel:
    table = Table(
        title="[bold]Multi‑Coin Trade Monitor[/]", box=box.ROUNDED, expand=True
    )
    cols = [
        "Symbol",
        "Status",
        "Side",
        "Regime",
        "Price",
        "Pressure (21s)",  # Renamed
        "Score",
        "Guidance",
        "Probable_Top",
        "Probable_Bot",
        "CVD",
        "OBI",
        "MOM",
        "VOL",
        "OI_D",
        "LIQ",
        "Funding",
        "Source",
    ]
    for c in cols:
        justify = (
            "right"
            if c
            in (
                "Price",
                "Probable_Top",
                "Probable_Bot",
                "Funding",
                "Score",
                "CVD",
                "OBI",
                "MOM",
                "VOL",
                "OI_D",
                "LIQ",
            )
            else "left"
        )

        if c == "Pressure (21s)":
            justify = "left"  # Sparklines look better left-aligned

        style = "cyan" if c == "Symbol" else ("yellow" if c == "Status" else "white")

        width = 21 if c == "Pressure (21s)" else None  # Set fixed width

        table.add_column(
            c,
            justify=justify,
            style=style,
            no_wrap=(
                c
                in (
                    "Symbol",
                    "Source",
                    "Side",
                    "Guidance",
                    "Status",
                    "Regime",
                    "Pressure (21s)",
                )
            ),
            width=width,
        )

    items = list(store.items())
    # --- UPDATE: Sort alphabetically by symbol (key: kv[0]) ---
    items.sort(key=lambda kv: kv[0])

    for sym, row in items:
        table.add_row(
            sym,
            row.get("Status", "—"),
            row["Side"],
            row["Regime"],
            row["Price"],
            row["Pressure"],
            row["Score"],
            row["Guidance"],
            row["Probable_Top"],
            row["Probable_Bot"],
            row["CVD"],
            row["OBI"],
            row["MOM"],
            row["VOL"],
            row["OI_D"],
            row["LIQ"],
            row["Funding"],
            row["Source"],
        )

    mem = psutil.Process(os.getpid()).memory_info().rss
    cpu = psutil.cpu_percent(interval=None)
    sub = f"[dim]Public‑only. Score=f(CVD,OBI,MOM,VOL,OI,LIQ) | CPU {cpu:.1f}% | RSS {human_bytes(mem)}[/dim]"
    return Panel(table, subtitle=sub, expand=True)


# --------------------------- Orchestrator ---------------------------


class Orchestrator:
    def __init__(self, config: Dict[str, any]):
        self.config = config
        self.app_cfg = config.get("app") or {}
        self.trade_cfg = config.get("trading") or {}

        symbols_raw = self.app_cfg.get("watchlist") or []
        self.symbols = [s.upper() for s in symbols_raw]

        positions_map = self.trade_cfg.get("positions") or {}
        global_side = self.trade_cfg.get("global_side")

        self.positions = {}
        for s in self.symbols:
            side = positions_map.get(s, global_side)
            self.positions[s] = side.lower() if side in ("long", "short") else None

        self.console = Console(
            force_terminal=True, color_system="auto", soft_wrap=False, file=sys.stderr
        )
        structlog.configure(
            processors=[
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.JSONRenderer(),
            ]
        )
        logger.remove()
        logger.add(
            sys.stdout, level="INFO", enqueue=True, backtrace=False, diagnose=False
        )
        logger.add(
            "alerts.log",
            level="INFO",
            rotation="10 MB",
            retention=5,  # Keep 5 old log files
            enqueue=True,
            filter=lambda record: "alert" in record["extra"],
            format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {message}",
        )

        self.store: Dict[str, Dict[str, str]] = {}
        self.event_q: asyncio.Queue = asyncio.Queue(maxsize=1)

        db_path = self.app_cfg.get("db_path", "monitor_state.db")
        logger.info(f"Using persistence DB: {db_path}")
        self.db_conn = sqlite3.connect(db_path)
        self._create_db_table()

    def _create_db_table(self):
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS symbol_state (
                    symbol TEXT PRIMARY KEY,
                    last_updated REAL,
                    last_price REAL,
                    cvd_1h REAL,
                    mom_ema REAL,
                    score_smooth REAL,
                    mom_smooth REAL,
                    mom_slope_smooth REAL,
                    oi_ema REAL
                )
                """
            )
            self.db_conn.commit()
            logger.info("Persistence table 'symbol_state' initialized.")
        except Exception as e:
            logger.error(f"Failed to create DB table: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    async def run(self):
        pos_summary = {
            k: ("long" if v == "long" else "short" if v == "short" else None)
            for k, v in self.positions.items()
        }
        logger.bind(event="startup").info(
            "startup", symbols=self.symbols, positions=pos_summary
        )

        weights_cfg = (self.config.get("features") or {}).get("weights") or {}
        thresholds_cfg = self.trade_cfg.get("thresholds") or {}
        tuning_cfg = self.config.get("tuning") or {}
        windows_cfg = (self.config.get("features") or {}).get("windows") or {}
        regime_cfg = self.config.get("regime_filter") or {}
        mag_levels_cfg = self.config.get("magnetic_levels") or {}

        monitors = []
        tasks = []

        try:
            async with aiohttp.ClientSession() as sess:
                warm = WarmClient(sess)

                market_analyzers = {
                    s: MarketAnalyzer(s, regime_cfg, sess) for s in self.symbols
                }

                monitors = [
                    SymbolMonitor(
                        s,
                        self.positions.get(s),
                        weights=weights_cfg,
                        thresholds=thresholds_cfg,
                        tuning=tuning_cfg,
                        windows=windows_cfg,
                        alerts=self.app_cfg.get("alerts", True),
                        event_q=self.event_q,
                        console=self.console,
                        http_session=sess,
                        db_conn=self.db_conn,
                        market_analyzer=market_analyzers[s],
                        mag_levels_cfg=mag_levels_cfg,
                    )
                    for s in self.symbols
                ]
                tasks = [asyncio.create_task(m.run(warm, self.store)) for m in monitors]
                for m in monitors:
                    try:
                        await m.warm_start(warm, self.store)
                    except Exception as e:
                        logger.error(f"{m.symbol}: Failed to warm start: {e}")

                refresh_hz = self.app_cfg.get("refresh_hz", 4.0)
                last_draw = 0.0
                with Live(
                    render_table(self.store),
                    console=self.console,
                    refresh_per_second=refresh_hz,
                    transient=False,
                ) as live:
                    while True:
                        timeout = max(0.0, (1.0 / refresh_hz) - (now_ts() - last_draw))
                        try:
                            await asyncio.wait_for(self.event_q.get(), timeout=timeout)
                        except asyncio.TimeoutError:
                            pass
                        live.update(render_table(self.store))
                        last_draw = now_ts()
        except KeyboardInterrupt:
            logger.info("Shutdown signal received...")
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            logger.info("Saving final state to DB...")
            for m in monitors:
                try:
                    m._save_state_to_db()
                except Exception as e:
                    logger.error(f"{m.symbol}: Failed to save final state: {e}")
            if self.db_conn:
                self.db_conn.close()
            logger.info("...state saved. Exiting.")


# --------------------------- Entry ---------------------------


def main():
    if HAS_UVLOOP:
        uvloop.install()
        print("Using uvloop for asyncio event loop.", file=sys.stderr)
    else:
        print("uvloop not found, using standard asyncio.", file=sys.stderr)

    if HAS_ORJSON:
        print("Using orjson for JSON parsing.", file=sys.stderr)
    else:
        print("orjson not found, using standard json.", file=sys.stderr)

    config_path = Path(__file__).parent / "config.yml"
    if not config_path.exists():
        sys.stderr.write(f"ERROR: config.yml not found at {config_path}\n")
        sys.stderr.flush()
        sys.exit(1)

    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        if not config:
            raise ValueError("Config file is empty or invalid.")
    except Exception as e:
        sys.stderr.write(f"ERROR: Failed to load or parse config.yml: {e}\n")
        sys.stderr.flush()
        sys.exit(1)

    if not (config.get("app") or {}).get("watchlist"):
        sys.stderr.write(
            "Please set at least one symbol in config.yml under app.watchlist\n"
        )
        sys.stderr.flush()
        sys.exit(2)

    if (config.get("regime_filter") or {}).get("enabled", False) and not HAS_TA:
        sys.stderr.write(
            "WARNING: MarketAnalyzer is enabled in config.yml but `pandas` or `pandas-ta` is not installed.\n"
        )
        sys.stderr.write("Please run: pip install pandas pandas-ta\n")
        sys.stderr.flush()

    orch = Orchestrator(config)
    try:
        asyncio.run(orch.run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
