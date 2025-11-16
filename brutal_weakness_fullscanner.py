#!/usr/bin/env python3
"""
Brutal Weakness Radar – Full 550-coin Binance Futures short scanner
with Bybit OI feed + Rich, non-jumping UI + SQLite trade memory.

- Scans **all** Binance USDT-perp futures (no top-50 cap).
- Uses Binance REST ONLY for:
    * exchangeInfo  (once, at startup)
    * 24h tickers   (once per scan)
    * premiumIndex  (once per scan)
- Gets Open Interest from **Bybit WebSocket tickers** (linear contracts),
  which expose `openInterest` in their stream, so no REST spam for OI.
- Renders a stable Rich layout with 3 tables:
    1. Top Weak Shorts (signals)
    2. Live Trades (virtual, dry-run)
    3. Closed Trades & Verdicts
- Persists signals and trades to SQLite:
    brutal_weakness_trades.sqlite (in the SAME folder as this script)

Dependencies:
    pip install aiohttp rich

Tested on Python 3.12.
"""

import asyncio
import contextlib
import json
import math
import signal
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from rich import box
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table


# ----------------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------------


@dataclass
class ScannerConfig:
    # Binance endpoints
    binance_base_url: str = "https://fapi.binance.com"

    # Bybit WebSocket (public linear contracts)
    bybit_ws_url: str = "wss://stream.bybit.com/v5/public/linear"
    bybit_ticker_topic: str = "tickers.linear"  # subscribe for all linear tickers

    # Universe / liquidity
    min_quote_volume_usdt: float = 1_000_000.0  # filter illiquid dust
    max_symbols: Optional[int] = None  # None = no cap (scan entire USDT-perp universe)

    # Scan cadence
    scan_interval_seconds: int = 30

    # Risk & TP/SL
    account_equity_usdt: float = 1_000.0
    risk_per_trade_frac: float = 0.01  # 1% per trade
    sl_buffer_pct: float = 0.003  # base 0.3%, scaled by volatility

    # Rich UI row counts (fixed → non-jumping tables)
    top_signals_rows: int = 15
    live_trades_rows: int = 10
    closed_trades_rows: int = 10

    # SQLite – pinned next to this script
    db_path: Path = field(
        default_factory=lambda: Path(__file__).with_name("brutal_weakness_trades.sqlite")
    )


cfg = ScannerConfig()
console = Console()


# ----------------------------------------------------------------------------
# Rate limiter (token bucket, iterative)
# ----------------------------------------------------------------------------


class RateLimiter:
    """Simple token bucket limiter for Binance REST (defensive)."""

    def __init__(self, rate: float, per: float):
        self.rate = rate  # tokens per `per` seconds
        self.per = per
        self._tokens = rate
        self._max_tokens = rate
        self._updated_at = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Iterative, non-recursive acquire."""
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self._updated_at
                if elapsed > 0:
                    refill = elapsed * (self.rate / self.per)
                    self._tokens = min(self._max_tokens, self._tokens + refill)
                    self._updated_at = now

                if self._tokens >= 1:
                    self._tokens -= 1
                    return

                # Need to wait until next token is available
                wait_time = (1 - self._tokens) * (self.per / self.rate)
            await asyncio.sleep(wait_time)


# ----------------------------------------------------------------------------
# HTTP client (Binance REST) – IP-safe
# ----------------------------------------------------------------------------


class HttpClient:
    def __init__(self, base_url: str, rate_limiter: RateLimiter):
        self.base_url = base_url.rstrip("/")
        self.rate_limiter = rate_limiter
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self) -> "HttpClient":
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def safe_request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        max_retries: int = 5,
    ) -> Any:
        if self._session is None:
            raise RuntimeError("HttpClient session not started")
        url = self.base_url + path

        for attempt in range(1, max_retries + 1):
            await self.rate_limiter.acquire()
            try:
                async with self._session.request(method, url, params=params) as resp:
                    if resp.status == 200:
                        return await resp.json()

                    # Handle Binance 429/418 and 5xx with backoff
                    body = await resp.text()
                    if resp.status in (418, 429) or 500 <= resp.status < 600:
                        backoff = min(60, 2**attempt)
                        console.print(
                            f"[yellow][WARN] {method} {path} got HTTP {resp.status}. "
                            f"Body: {body[:200]!r}. Backing off {backoff}s...[/]"
                        )
                        await asyncio.sleep(backoff)
                        continue

                    raise RuntimeError(f"HTTP {resp.status} for {path}: {body}")
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                backoff = min(60, 2**attempt)
                console.print(
                    f"[yellow][WARN] {method} {path} exception: {e!r}. Backoff {backoff}s...[/]"
                )
                await asyncio.sleep(backoff)
        raise RuntimeError(f"Failed {method} {path} after {max_retries} retries")


# ----------------------------------------------------------------------------
# Bybit OI feed – WebSocket ticker stream, openInterest field
# ----------------------------------------------------------------------------


class OiCache:
    """In-memory OI store keyed by symbol (e.g. BTCUSDT)."""

    def __init__(self):
        self._data: Dict[str, Tuple[float, float]] = {}  # symbol -> (oi, ts)
        self._lock = asyncio.Lock()

    async def update(self, symbol: str, oi: float, ts: float) -> None:
        async with self._lock:
            self._data[symbol] = (oi, ts)

    async def get(self, symbol: str) -> Optional[Tuple[float, float]]:
        async with self._lock:
            return self._data.get(symbol)


async def bybit_oi_worker(oi_cache: OiCache, stop_event: asyncio.Event) -> None:
    """
    Connects to Bybit public linear WS and listens to ticker stream, which
    includes `openInterest`. This gives you a free, low-latency OI feed
    without hammering any REST endpoint.
    """
    url = cfg.bybit_ws_url
    topic = cfg.bybit_ticker_topic

    while not stop_event.is_set():
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(url, heartbeat=20) as ws:
                    sub_msg = {"op": "subscribe", "args": [topic]}
                    await ws.send_str(json.dumps(sub_msg))
                    console.print(
                        f"[green][INFO] Connected to Bybit WS OI ticker: {url}, topic={topic}[/]"
                    )

                    async for msg in ws:
                        if stop_event.is_set():
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                            except json.JSONDecodeError:
                                continue

                            if data.get("topic", "").startswith("tickers"):
                                dlist = data.get("data") or []
                                for d in dlist:
                                    symbol = (d.get("symbol") or "").upper()
                                    oi_str = d.get("openInterest")
                                    ts = float(d.get("ts", data.get("ts", time.time() * 1000)))
                                    if not symbol or oi_str is None:
                                        continue
                                    try:
                                        oi = float(oi_str)
                                    except (TypeError, ValueError):
                                        continue
                                    await oi_cache.update(symbol, oi, ts / 1000.0)
                        elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
                            break
        except asyncio.CancelledError:
            console.print(
                "[yellow][INFO] Bybit OI worker cancelled. Shutting down WS cleanly...[/]"
            )
            break
        except Exception as e:
            console.print(f"[red][ERROR] Bybit OI worker error: {e!r}. Reconnecting in 5s...[/]")
            await asyncio.sleep(5)


# ----------------------------------------------------------------------------
# SQLite trade/signal store
# ----------------------------------------------------------------------------


class TradeStore:
    def __init__(self, path: Path):
        self.path = path
        self._init_db()

    def _init_db(self) -> None:
        conn = sqlite3.connect(self.path)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts_utc INTEGER,
                    symbol TEXT,
                    score REAL,
                    side TEXT,
                    entry REAL,
                    sl REAL,
                    tp1 REAL,
                    tp2 REAL,
                    tp3 REAL,
                    reasons TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS trades_live (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    opened_ts_utc INTEGER,
                    symbol TEXT,
                    side TEXT,
                    entry REAL,
                    sl REAL,
                    tp1 REAL,
                    tp2 REAL,
                    tp3 REAL,
                    size_usdt REAL,
                    status TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS trades_closed (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    closed_ts_utc INTEGER,
                    symbol TEXT,
                    side TEXT,
                    entry REAL,
                    exit REAL,
                    pnl_pct REAL,
                    verdict TEXT
                )
                """
            )
            conn.commit()
        finally:
            conn.close()

    def log_signal(
        self,
        ts_utc: int,
        symbol: str,
        score: float,
        side: str,
        entry: float,
        sl: float,
        tp1: float,
        tp2: float,
        tp3: float,
        reasons: str,
    ) -> None:
        conn = sqlite3.connect(self.path)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO signals (ts_utc, symbol, score, side, entry, sl, tp1, tp2, tp3, reasons)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (ts_utc, symbol, score, side, entry, sl, tp1, tp2, tp3, reasons),
            )
            conn.commit()
        finally:
            conn.close()

    def open_trade(
        self,
        ts_utc: int,
        symbol: str,
        side: str,
        entry: float,
        sl: float,
        tp1: float,
        tp2: float,
        tp3: float,
        size_usdt: float,
    ) -> None:
        conn = sqlite3.connect(self.path)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO trades_live
                    (opened_ts_utc, symbol, side, entry, sl, tp1, tp2, tp3, size_usdt, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (ts_utc, symbol, side, entry, sl, tp1, tp2, tp3, size_usdt, "OPEN"),
            )
            conn.commit()
        finally:
            conn.close()

    def close_trade(
        self,
        trade_id: int,
        closed_ts_utc: int,
        exit_price: float,
        pnl_pct: float,
        verdict: str,
    ) -> None:
        conn = sqlite3.connect(self.path)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO trades_closed (closed_ts_utc, symbol, side, entry, exit, pnl_pct, verdict)
                SELECT ?, symbol, side, entry, ?, ?, ?
                FROM trades_live WHERE id = ?
                """,
                (closed_ts_utc, exit_price, pnl_pct, verdict, trade_id),
            )
            cur.execute("DELETE FROM trades_live WHERE id = ?", (trade_id,))
            conn.commit()
        finally:
            conn.close()

    def fetch_live_trades(self, limit: int) -> List[Tuple]:
        conn = sqlite3.connect(self.path)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, opened_ts_utc, symbol, side, entry, sl, tp1, tp2, tp3, size_usdt, status
                FROM trades_live
                ORDER BY opened_ts_utc DESC
                LIMIT ?
                """,
                (limit,),
            )
            return cur.fetchall()
        finally:
            conn.close()

    def fetch_closed_trades(self, limit: int) -> List[Tuple]:
        conn = sqlite3.connect(self.path)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT closed_ts_utc, symbol, side, entry, exit, pnl_pct, verdict
                FROM trades_closed
                ORDER BY closed_ts_utc DESC
                LIMIT ?
                """,
                (limit,),
            )
            return cur.fetchall()
        finally:
            conn.close()

    def has_open_trade_for_symbol(self, symbol: str) -> bool:
        conn = sqlite3.connect(self.path)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT 1 FROM trades_live WHERE symbol = ? AND status = 'OPEN' LIMIT 1",
                (symbol,),
            )
            row = cur.fetchone()
            return row is not None
        finally:
            conn.close()


# ----------------------------------------------------------------------------
# Feature engineering & scoring
# ----------------------------------------------------------------------------


@dataclass
class WeaknessFeatures:
    symbol: str
    last_price: float
    price_change_pct: float
    dist_from_high_pct: float
    funding_rate: float
    oi_change_pct: Optional[float]
    range_pct: float          # 24h range in %
    reasons: List[str]
    score: float


def compute_weakness_score(
    symbol: str,
    last_price: float,
    price_change_pct: float,
    dist_from_high_pct: float,
    funding_rate: float,
    oi_change_pct: Optional[float],
    range_pct: float,
) -> WeaknessFeatures:
    reasons: List[str] = []

    # 1. Big 24h up-move (crowded longs)
    up_score = max(price_change_pct, 0.0)
    if up_score > 3:
        reasons.append(f"24h +{price_change_pct:.1f}% (crowded longs)")

    # 2. Rolling over below 24h high
    roll_score = max(dist_from_high_pct, 0.0)
    if roll_score > 1:
        reasons.append(f"{dist_from_high_pct:.1f}% below 24h high (rollover)")

    # 3. Positive funding (longs paying shorts)
    funding_bps = funding_rate * 10_000.0
    fund_score = max(funding_bps, 0.0)
    if fund_score > 0:
        reasons.append(f"Funding +{funding_bps:.1f} bps (bullish positioning)")

    # 4. OI dynamics
    oi_score = 0.0
    if oi_change_pct is not None:
        if oi_change_pct < -5:
            oi_score += 5.0
            reasons.append(f"OI −{abs(oi_change_pct):.1f}% (position washout)")
        elif oi_change_pct > 5:
            oi_score -= 3.0
            reasons.append(f"OI +{oi_change_pct:.1f}% (fresh longs)")

    # Combine with weights (heuristic)
    score = 0.5 * up_score + 0.7 * roll_score + 0.2 * fund_score + oi_score

    return WeaknessFeatures(
        symbol=symbol,
        last_price=last_price,
        price_change_pct=price_change_pct,
        dist_from_high_pct=dist_from_high_pct,
        funding_rate=funding_rate,
        oi_change_pct=oi_change_pct,
        range_pct=range_pct,
        reasons=reasons,
        score=score,
    )


# ----------------------------------------------------------------------------
# Binance universe fetchers
# ----------------------------------------------------------------------------


async def fetch_universe_symbols(client: HttpClient) -> List[str]:
    data = await client.safe_request("GET", "/fapi/v1/exchangeInfo")
    symbols_out: List[str] = []
    for s in data.get("symbols", []):
        if (
            s.get("quoteAsset") == "USDT"
            and s.get("contractType") == "PERPETUAL"
            and s.get("status") == "TRADING"
        ):
            symbols_out.append(s["symbol"])
    symbols_out.sort()
    return symbols_out


async def fetch_ticker_24h(client: HttpClient) -> Dict[str, Dict[str, Any]]:
    data = await client.safe_request("GET", "/fapi/v1/ticker/24hr")
    out: Dict[str, Dict[str, Any]] = {}
    for d in data:
        symbol = d.get("symbol")
        if not symbol:
            continue
        out[symbol] = d
    return out


async def fetch_premium_index(client: HttpClient) -> Dict[str, Dict[str, Any]]:
    data = await client.safe_request("GET", "/fapi/v1/premiumIndex")
    out: Dict[str, Dict[str, Any]] = {}
    for d in data:
        symbol = d.get("symbol")
        if not symbol:
            continue
        out[symbol] = d
    return out


# ----------------------------------------------------------------------------
# Risk & trade sizing
# ----------------------------------------------------------------------------


def compute_tp_sl(
    entry: float,
    dist_from_high_pct: float,
    range_pct: float,
    score: float,
) -> Tuple[float, float, float, float]:
    """
    Advanced TP/SL engine (SHORT bias), with extra wick protection:

    - SL anchored to 24h high + volatility factor.
    - PLUS an ATR-like floor based on 24h range so SL is never too tight.
    - PLUS extra widening for very high scores (we expect cascade, don't get wicked).
    - TP1/TP2/TP3 based on 24h range + weakness score buckets.
    """

    if entry <= 0:
        return entry, entry, entry, entry

    # --- 1) 24h range sanity clamp (volatility unit) ---
    # range_pct = (high - low) / last * 100; clamp to avoid crazy tails
    range_pct = max(2.0, min(range_pct, 60.0))

    # One "unit" ≈ quarter of the daily range
    unit_pct = range_pct / 4.0  # in %

    # --- 2) Score-based aggression multipliers for TPs ---
    if score < 20:
        # softer, mean-reversion dump
        m1, m2, m3 = 0.6, 1.0, 1.3
    elif score < 35:
        # normal
        m1, m2, m3 = 0.9, 1.4, 1.9
    else:
        # brutal weakness, aim deeper
        m1, m2, m3 = 1.1, 1.7, 2.3

    tp1_pct = (unit_pct * m1) / 100.0  # convert back to fraction
    tp2_pct = (unit_pct * m2) / 100.0
    tp3_pct = (unit_pct * m3) / 100.0

    # --- 3) Reconstruct 24h high and base SL from high (structure + vol) ---
    if dist_from_high_pct > 0.01:
        # Rebuild the 24h high from current price + distance below high
        high = entry / (1.0 - dist_from_high_pct / 100.0)
    else:
        # Fallback: synthetic high slightly above entry, scaled by volatility
        synthetic_bump = max(0.005, min(0.02, range_pct / 200.0))
        high = entry * (1.0 + synthetic_bump)

    # Volatility factor for SL buffer (more volatile → wider)
    if range_pct < 10:
        vol_factor = 1.0
    elif range_pct < 30:
        vol_factor = 1.5
    else:
        vol_factor = 2.0

    sl_from_high = high * (1.0 + cfg.sl_buffer_pct * vol_factor)

    # --- 4) ATR-like minimum stop distance from entry (wick floor) ---
    # Use 24h range as a cheap ATR proxy:
    #   * at least 0.5% away
    #   * at most 10% (don't go crazy)
    atr_like_pct = max(0.5, min(range_pct / 6.0, 10.0))  # in %
    sl_from_entry_floor = entry * (1.0 + atr_like_pct / 100.0)

    # --- 5) Extra widening for very high scores (don't get wicked on best setups) ---
    if score > 50:
        score_extra_pct = 0.5   # +0.5% SL widening
    elif score > 35:
        score_extra_pct = 0.35  # +0.35% SL widening
    else:
        score_extra_pct = 0.0

    # Combine all SL protections:
    sl = max(sl_from_high, sl_from_entry_floor)
    sl *= (1.0 + score_extra_pct / 100.0)

    # --- 6) Final TP prices for SHORTS ---
    tp1 = entry * (1.0 - tp1_pct)
    tp2 = entry * (1.0 - tp2_pct)
    tp3 = entry * (1.0 - tp3_pct)

    # Guard against negative or zero TPs on crazy microcaps
    floor_price = entry * 0.02  # don't go below 98% dump in targets
    tp1 = max(tp1, floor_price)
    tp2 = max(tp2, floor_price)
    tp3 = max(tp3, floor_price)

    return sl, tp1, tp2, tp3


def compute_position_size(entry: float, sl: float) -> float:
    risk_usdt = cfg.account_equity_usdt * cfg.risk_per_trade_frac
    if entry <= 0 or sl <= 0 or sl <= entry:
        return 0.0
    risk_per_unit = abs(sl - entry)
    size = risk_usdt / risk_per_unit
    notional = size * entry
    return notional


# ----------------------------------------------------------------------------
# Rich UI builders – non-jumping layout (fixed rows)
# ----------------------------------------------------------------------------


def style_pct(val: Optional[float]) -> str:
    if val is None or math.isnan(val):
        return "-"
    color = "green" if val < 0 else "red"
    return f"[{color}]{val:+.2f}%[/]"


def style_small_pct(val: Optional[float]) -> str:
    if val is None or math.isnan(val):
        return "-"
    color = "green" if val < 0 else "red"
    return f"[{color}]{val:+.1f}%[/]"


def style_number(val: Optional[float], decimals: int = 4) -> str:
    if val is None or math.isnan(val):
        return "-"
    return f"{val:.{decimals}f}"


def build_signals_table(features: List[WeaknessFeatures]) -> Table:
    table = Table(
        title="💀 Brutal Weakness – Top Shorts (Binance USDT-Perps)",
        box=box.MINIMAL_HEAVY_HEAD,
        expand=True,
        show_lines=False,
    )
    table.add_column("Rank", justify="right", style="bold cyan")
    table.add_column("Symbol", style="bold white")
    table.add_column("Score", justify="right", style="bold yellow")
    table.add_column("Price", justify="right")
    table.add_column("24h %", justify="right")
    table.add_column("↓ From High", justify="right")
    table.add_column("Funding", justify="right")
    table.add_column("OI Δ%", justify="right")
    table.add_column("Notes", overflow="fold")

    rows_to_show = cfg.top_signals_rows
    for idx in range(rows_to_show):
        if idx < len(features):
            f = features[idx]
            rank = f"{idx+1:02d}"
            sym = f"{f.symbol}"
            score_str = f"{f.score:6.1f}"
            price_str = style_number(f.last_price, 4)
            chg_str = style_pct(f.price_change_pct)
            dist_str = style_small_pct(f.dist_from_high_pct)
            funding_bps = f.funding_rate * 10_000.0
            funding_str = style_number(funding_bps, 1)
            if funding_bps > 0:
                funding_str = f"[red]{funding_str}[/]"
            elif funding_bps < 0:
                funding_str = f"[green]{funding_str}[/]"
            oi_str = style_small_pct(f.oi_change_pct)
            note = " · ".join(f.reasons) if f.reasons else ""
            if idx == 0:
                sym = f"🚨 {sym}"
            elif idx < 3:
                sym = f"⚠️ {sym}"
            table.add_row(rank, sym, score_str, price_str, chg_str, dist_str, funding_str, oi_str, note)
        else:
            table.add_row("", "", "", "", "", "", "", "", "")

    return table


def build_live_trades_table(
    live_trades: List[Tuple],
    price_lookup: Dict[str, float],
) -> Table:
    table = Table(
        title="📈 Live Trades (Paper)",
        box=box.MINIMAL_HEAVY_HEAD,
        expand=True,
        show_lines=False,
    )
    table.add_column("ID", justify="right", style="cyan")
    table.add_column("Symbol", style="bold white")
    table.add_column("Side", style="magenta")
    table.add_column("Entry", justify="right")
    table.add_column("Last", justify="right")
    table.add_column("SL", justify="right")
    table.add_column("TP1/TP2/TP3", justify="right")
    table.add_column("Size USDT", justify="right")
    table.add_column("P/L %", justify="right")
    table.add_column("Status", style="bold")

    rows_to_show = cfg.live_trades_rows
    for idx in range(rows_to_show):
        if idx < len(live_trades):
            (
                trade_id,
                opened_ts,
                symbol,
                side,
                entry,
                sl,
                tp1,
                tp2,
                tp3,
                size_usdt,
                status,
            ) = live_trades[idx]
            last = price_lookup.get(symbol)
            pl_pct = None
            if last and entry:
                if side.upper() == "SHORT":
                    pl_pct = (entry - last) / entry * 100.0
                else:
                    pl_pct = (last - entry) / entry * 100.0

            # Entry – sky blue
            raw_entry = style_number(entry, 4)
            entry_str = f"[bright_cyan]{raw_entry}[/]"

            # Last – neutral
            last_str = style_number(last, 4)

            # SL – red
            raw_sl = style_number(sl, 4)
            sl_str = f"[red]{raw_sl}[/]"

            # TP1/TP2/TP3 – gradient greens
            tp1_raw = style_number(tp1, 4)
            tp2_raw = style_number(tp2, 4)
            tp3_raw = style_number(tp3, 4)

            tp1_str = f"[green1]{tp1_raw}[/]"   # light green
            tp2_str = f"[green3]{tp2_raw}[/]"   # medium green
            tp3_str = f"[green4]{tp3_raw}[/]"   # darker green

            tp_str = f"{tp1_str}/{tp2_str}/{tp3_str}"

            size_str = f"{size_usdt:.0f}"
            pl_str = style_pct(pl_pct)

            status_style = "yellow"
            if pl_pct is not None:
                if pl_pct > 0:
                    status_style = "green"
                elif pl_pct < 0:
                    status_style = "red"
            table.add_row(
                str(trade_id),
                symbol,
                side.upper(),
                entry_str,
                last_str,
                sl_str,
                tp_str,
                size_str,
                pl_str,
                f"[{status_style}]{status}[/]",
            )
        else:
            table.add_row("", "", "", "", "", "", "", "", "", "")

    return table


def build_closed_trades_table(closed_trades: List[Tuple]) -> Table:
    table = Table(
        title="✅ Closed Trades & Verdicts",
        box=box.MINIMAL_HEAVY_HEAD,
        expand=True,
        show_lines=False,
    )
    table.add_column("Time", justify="right")
    table.add_column("Symbol")
    table.add_column("Side")
    table.add_column("Entry", justify="right")
    table.add_column("Exit", justify="right")
    table.add_column("P/L %", justify="right")
    table.add_column("Verdict")

    rows_to_show = cfg.closed_trades_rows
    for idx in range(rows_to_show):
        if idx < len(closed_trades):
            closed_ts, symbol, side, entry, exit_price, pnl_pct, verdict = closed_trades[idx]
            entry_str = style_number(entry, 4)
            exit_str = style_number(exit_price, 4)
            pnl_str = style_pct(pnl_pct)
            if pnl_pct is not None and pnl_pct > 0:
                verdict_str = f"[green]{verdict}[/]"
            elif pnl_pct is not None and pnl_pct < 0:
                verdict_str = f"[red]{verdict}[/]"
            else:
                verdict_str = f"[yellow]{verdict}[/]"
            time_str = time.strftime("%H:%M", time.gmtime(closed_ts))
            table.add_row(time_str, symbol, side.upper(), entry_str, exit_str, pnl_str, verdict_str)
        else:
            table.add_row("", "", "", "", "", "", "")

    return table


# ----------------------------------------------------------------------------
# Trade engine (very lightweight, paper-only)
# ----------------------------------------------------------------------------


async def manage_trades(
    store: TradeStore,
    top_signals: List[WeaknessFeatures],
    price_lookup: Dict[str, float],
) -> None:
    """
    Very simple paper-trade engine:
    - If a symbol is in the top 3 signals and has no open trade → open SHORT.
    - Close trades when TP3 or SL is hit.
    """
    now_utc = int(time.time())

    # Open new trades for top 3
    for f in top_signals[:3]:
        symbol = f.symbol
        if store.has_open_trade_for_symbol(symbol):
            continue
        entry = f.last_price
        sl, tp1, tp2, tp3 = compute_tp_sl(
            entry=entry,
            dist_from_high_pct=f.dist_from_high_pct,
            range_pct=f.range_pct,
            score=f.score,
        )
        notional = compute_position_size(entry, sl)
        if notional <= 0:
            continue
        store.open_trade(
            ts_utc=now_utc,
            symbol=symbol,
            side="SHORT",
            entry=entry,
            sl=sl,
            tp1=tp1,
            tp2=tp2,
            tp3=tp3,
            size_usdt=notional,
        )

    # Check exits for live trades
    live_trades = store.fetch_live_trades(limit=cfg.live_trades_rows * 3)
    for row in live_trades:
        trade_id, opened_ts, symbol, side, entry, sl, tp1, tp2, tp3, size_usdt, status = row
        last = price_lookup.get(symbol)
        if last is None or status != "OPEN":
            continue
        if side.upper() == "SHORT":
            # Stop loss
            if last >= sl:
                pnl_pct = (entry - last) / entry * 100.0
                store.close_trade(trade_id, int(time.time()), last, pnl_pct, "Stopped Out")
            # TP3
            elif last <= tp3:
                pnl_pct = (entry - last) / entry * 100.0
                store.close_trade(trade_id, int(time.time()), last, pnl_pct, "TP3 Hit")
            else:
                continue


# ----------------------------------------------------------------------------
# Main scan loop
# ----------------------------------------------------------------------------


async def scan_loop() -> None:
    rate_limiter = RateLimiter(rate=120, per=60.0)  # conservative
    trade_store = TradeStore(cfg.db_path)
    oi_cache = OiCache()
    oi_state: Dict[str, float] = {}

    stop_event = asyncio.Event()

    # Graceful shutdown handlers
    def handle_sig(*_: Any) -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_sig)
        except NotImplementedError:
            pass

    # Background Bybit OI task
    oi_task = asyncio.create_task(bybit_oi_worker(oi_cache, stop_event))

    async with HttpClient(cfg.binance_base_url, rate_limiter) as client:
        # Build Binance futures universe once
        symbols = await fetch_universe_symbols(client)
        console.print(
            f"[green][INFO] Binance USDT-perp universe: {len(symbols)} symbols "
            f"(min quote vol filter will apply per scan)[/]"
        )

        with Live(console=console, screen=True, refresh_per_second=4) as live:
            try:
                while not stop_event.is_set():
                    loop_start = time.time()
                    try:
                        ticker_24h, premium_idx = await asyncio.gather(
                            fetch_ticker_24h(client),
                            fetch_premium_index(client),
                        )
                    except Exception as e:
                        console.print(f"[red][ERROR] Failed fetching Binance data: {e!r}[/]")
                        await asyncio.sleep(5)
                        continue

                    features: List[WeaknessFeatures] = []
                    price_lookup: Dict[str, float] = {}

                    for sym in symbols:
                        t = ticker_24h.get(sym)
                        p = premium_idx.get(sym)
                        if not t or not p:
                            continue

                        try:
                            last_price = float(t.get("lastPrice", 0.0))
                            price_change_pct = float(t.get("priceChangePercent", 0.0))
                            quote_volume = float(t.get("quoteVolume", 0.0))
                            high_price = float(t.get("highPrice", 0.0))
                            low_price = float(t.get("lowPrice", 0.0))
                            funding_rate = float(p.get("lastFundingRate", 0.0) or 0.0)
                        except (TypeError, ValueError):
                            continue

                        if last_price <= 0 or high_price <= 0 or low_price <= 0:
                            continue
                        if quote_volume < cfg.min_quote_volume_usdt:
                            continue

                        if cfg.max_symbols is not None and len(features) >= cfg.max_symbols:
                            break

                        # 24h range % (volatility proxy)
                        range_pct = (
                            (high_price - low_price) / last_price * 100.0
                            if last_price > 0
                            else 0.0
                        )
                        range_pct = max(2.0, min(range_pct, 60.0))

                        dist_from_high_pct = (1.0 - last_price / high_price) * 100.0
                        price_lookup[sym] = last_price

                        # OI change from Bybit cache
                        oi_change_pct: Optional[float] = None
                        oi_entry = await oi_cache.get(sym)
                        if oi_entry is not None:
                            oi_now, _ = oi_entry
                            if oi_now > 0:
                                oi_prev = oi_state.get(sym, oi_now)
                                oi_change_pct = (oi_now - oi_prev) / oi_prev * 100.0
                                oi_state[sym] = oi_now

                        wf = compute_weakness_score(
                            symbol=sym,
                            last_price=last_price,
                            price_change_pct=price_change_pct,
                            dist_from_high_pct=dist_from_high_pct,
                            funding_rate=funding_rate,
                            oi_change_pct=oi_change_pct,
                            range_pct=range_pct,
                        )
                        features.append(wf)

                    # Rank by descending score
                    features.sort(key=lambda f: f.score, reverse=True)
                    now_utc = int(time.time())

                    # Log signals for top rows; open trades for top 3
                    top_for_trades = features[: cfg.top_signals_rows]
                    for f in top_for_trades[:3]:
                        sl, tp1, tp2, tp3 = compute_tp_sl(
                            entry=f.last_price,
                            dist_from_high_pct=f.dist_from_high_pct,
                            range_pct=f.range_pct,
                            score=f.score,
                        )
                        trade_store.log_signal(
                            ts_utc=now_utc,
                            symbol=f.symbol,
                            score=f.score,
                            side="SHORT",
                            entry=f.last_price,
                            sl=sl,
                            tp1=tp1,
                            tp2=tp2,
                            tp3=tp3,
                            reasons=" | ".join(f.reasons),
                        )

                    await manage_trades(trade_store, top_for_trades, price_lookup)

                    # Fetch DB views for UI
                    live_trades = trade_store.fetch_live_trades(cfg.live_trades_rows)
                    closed_trades = trade_store.fetch_closed_trades(cfg.closed_trades_rows)

                    signals_table = build_signals_table(features)
                    live_table = build_live_trades_table(live_trades, price_lookup)
                    closed_table = build_closed_trades_table(closed_trades)

                    layout = Group(
                        Panel(signals_table, title="Brutal Weakness Radar", border_style="bright_blue"),
                        Panel(live_table, title="Trade Monitor (Dry-Run)", border_style="green"),
                        Panel(closed_table, title="Trade Verdicts", border_style="magenta"),
                    )
                    live.update(layout)

                    elapsed = time.time() - loop_start
                    sleep_for = max(1.0, cfg.scan_interval_seconds - elapsed)
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=sleep_for)
                    except asyncio.TimeoutError:
                        pass
            finally:
                stop_event.set()
                oi_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await oi_task


def main() -> None:
    try:
        import uvloop  # type: ignore

        uvloop.install()
    except Exception:
        pass

    try:
        asyncio.run(scan_loop())
    except KeyboardInterrupt:
        print("\nStopped by user.")


if __name__ == "__main__":
    main()
