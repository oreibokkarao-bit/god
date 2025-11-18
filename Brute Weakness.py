#!/usr/bin/env python3
"""
Brutal Weakness – Fullscanner (Upgraded)

- Scans (almost) the full Binance USDT-perp universe.
- Uses light Binance REST for 24h stats + funding (premiumIndex).
- Streams Bybit linear tickers over WebSocket to get live OI.
- Computes a Brutal Weakness score:
    * crowded longs (24h up, near 24h high, positive funding)
    * price starting to roll
    * OI not confirming (flat/down is best)
- Ranks symbols, opens paper SHORTS with entry/SL/TP1–TP3.
- Tracks open & closed trades in SQLite.
- Shows a 3-panel Rich dashboard with FIXED row counts:
    1) Radar (Top Brutal Weakness candidates)
    2) Live paper trades
    3) Closed trades with verdicts
- IP-safe: single 24h/funding batch per scan, WS for OI, safe_request.
- Clean shutdown: Ctrl+C stops gracefully without ugly asyncio traces.

Dependencies:
    pip install aiohttp rich aiosqlite

NOTE: This is an upgraded rewrite that follows the same logic/blueprint
we discussed. It is not a 1:1 line patch of your old file, but it’s
designed to be a drop-in replacement with similar behaviour + stability.
"""

import asyncio
import math
import signal
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from rich.align import Align
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

# ==========================
# CONFIG
# ==========================

BINANCE_FAPI_URL = "https://fapi.binance.com"
BYBIT_LINEAR_WS = "wss://stream.bybit.com/v5/public/linear"

DB_PATH = Path(__file__).with_name("brutal_weakness_trades.sqlite")

SCAN_INTERVAL = 15  # seconds between full scans
MIN_QUOTE_VOLUME_USDT = 10_000_000  # filter out illiquid junk
MAX_SYMBOLS = 550  # safety cap for universe; 0 = no cap

# UI
RADAR_ROWS = 15
OPEN_ROWS = 10
CLOSED_ROWS = 10

# RISK
ACCOUNT_EQUITY_USDT = 10_000
RISK_PER_TRADE = 0.01  # 1% of equity
MAX_OPEN_TRADES = 5


console = Console()


# ==========================
# DATA MODELS
# ==========================

@dataclass
class SymbolSnapshot:
    symbol: str
    last_price: float
    high_price: float
    low_price: float
    price_change_pct: float
    quote_volume: float
    funding_rate: float
    oi: Optional[float] = None
    prev_oi: Optional[float] = None

    @property
    def dist_from_high_pct(self) -> float:
        if self.high_price <= 0:
            return 0.0
        return (self.high_price - self.last_price) / self.high_price * 100.0

    @property
    def dist_from_low_pct(self) -> float:
        if self.low_price <= 0:
            return 0.0
        return (self.last_price - self.low_price) / self.low_price * 100.0

    @property
    def oi_change_pct(self) -> float:
        if self.oi is None or self.prev_oi is None or self.prev_oi <= 0:
            return 0.0
        return (self.oi - self.prev_oi) / self.prev_oi * 100.0


@dataclass
class Signal:
    symbol: str
    score: float
    snapshot: SymbolSnapshot
    ts: float = field(default_factory=time.time)


@dataclass
class Trade:
    id: int
    symbol: str
    direction: str  # "SHORT"
    entry: float
    sl: float
    tp1: float
    tp2: float
    tp3: float
    size_usdt: float
    opened_at: float
    closed_at: Optional[float] = None
    exit_price: Optional[float] = None
    pnl_pct: Optional[float] = None
    verdict: Optional[str] = None  # WIN / LOSS / SCRATCH


# ==========================
# SQLITE
# ==========================

def init_db(path: Path = DB_PATH) -> None:
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL,
            symbol TEXT,
            score REAL,
            last_price REAL,
            price_change_pct REAL,
            dist_from_high REAL,
            dist_from_low REAL,
            funding_rate REAL,
            oi REAL,
            oi_change_pct REAL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS trades_open (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            direction TEXT,
            entry REAL,
            sl REAL,
            tp1 REAL,
            tp2 REAL,
            tp3 REAL,
            size_usdt REAL,
            opened_at REAL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS trades_closed (
            id INTEGER PRIMARY KEY,
            symbol TEXT,
            direction TEXT,
            entry REAL,
            sl REAL,
            tp1 REAL,
            tp2 REAL,
            tp3 REAL,
            size_usdt REAL,
            opened_at REAL,
            closed_at REAL,
            exit_price REAL,
            pnl_pct REAL,
            verdict TEXT
        )
        """
    )
    conn.commit()
    conn.close()


def log_signal(sig: Signal) -> None:
    snap = sig.snapshot
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO signals (
            ts, symbol, score, last_price, price_change_pct,
            dist_from_high, dist_from_low, funding_rate, oi, oi_change_pct
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            sig.ts,
            sig.symbol,
            sig.score,
            snap.last_price,
            snap.price_change_pct,
            snap.dist_from_high_pct,
            snap.dist_from_low_pct,
            snap.funding_rate,
            snap.oi,
            snap.oi_change_pct,
        ),
    )
    conn.commit()
    conn.close()


def open_trade_from_signal(sig: Signal) -> Optional[Trade]:
    """Create a paper SHORT from a signal based on 1R = distance to SL."""
    snap = sig.snapshot
    price = snap.last_price
    if price <= 0:
        return None

    # SHORT: SL above current price, TPs below.
    # Use structural-ish levels. Here we approximate:
    sl = price * 1.02  # 2% above
    one_r = sl - price
    if one_r <= 0:
        return None
    tp1 = price - one_r
    tp2 = price - 2 * one_r
    tp3 = price - 3 * one_r

    risk_usdt = ACCOUNT_EQUITY_USDT * RISK_PER_TRADE
    qty = risk_usdt / one_r if one_r > 0 else 0
    if qty <= 0:
        return None

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO trades_open (
            symbol, direction, entry, sl, tp1, tp2, tp3, size_usdt, opened_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            sig.symbol,
            "SHORT",
            price,
            sl,
            tp1,
            tp2,
            tp3,
            qty * price,
            sig.ts,
        ),
    )
    trade_id = cur.lastrowid
    conn.commit()
    conn.close()

    return Trade(
        id=trade_id,
        symbol=sig.symbol,
        direction="SHORT",
        entry=price,
        sl=sl,
        tp1=tp1,
        tp2=tp2,
        tp3=tp3,
        size_usdt=qty * price,
        opened_at=sig.ts,
    )


def fetch_open_trades() -> List[Trade]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, symbol, direction, entry, sl, tp1, tp2, tp3,
               size_usdt, opened_at
        FROM trades_open
        ORDER BY opened_at DESC
        """
    )
    rows = cur.fetchall()
    conn.close()
    trades: List[Trade] = []
    for r in rows:
        trades.append(
            Trade(
                id=r[0],
                symbol=r[1],
                direction=r[2],
                entry=r[3],
                sl=r[4],
                tp1=r[5],
                tp2=r[6],
                tp3=r[7],
                size_usdt=r[8],
                opened_at=r[9],
            )
        )
    return trades


def close_trade(trade: Trade, exit_price: float, verdict: str) -> None:
    if trade.entry <= 0:
        pnl_pct = 0.0
    else:
        if trade.direction == "SHORT":
            pnl_pct = (trade.entry - exit_price) / trade.entry * 100.0
        else:
            pnl_pct = (exit_price - trade.entry) / trade.entry * 100.0

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM trades_open WHERE id = ?",
        (trade.id,),
    )
    cur.execute(
        """
        INSERT OR REPLACE INTO trades_closed (
            id, symbol, direction, entry, sl, tp1, tp2, tp3,
            size_usdt, opened_at, closed_at, exit_price, pnl_pct, verdict
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            trade.id,
            trade.symbol,
            trade.direction,
            trade.entry,
            trade.sl,
            trade.tp1,
            trade.tp2,
            trade.tp3,
            trade.size_usdt,
            trade.opened_at,
            time.time(),
            exit_price,
            pnl_pct,
            verdict,
        ),
    )
    conn.commit()
    conn.close()


def fetch_closed_trades(limit: int = 50) -> List[Trade]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, symbol, direction, entry, sl, tp1, tp2, tp3,
               size_usdt, opened_at, closed_at, exit_price, pnl_pct, verdict
        FROM trades_closed
        ORDER BY closed_at DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()
    out: List[Trade] = []
    for r in rows:
        out.append(
            Trade(
                id=r[0],
                symbol=r[1],
                direction=r[2],
                entry=r[3],
                sl=r[4],
                tp1=r[5],
                tp2=r[6],
                tp3=r[7],
                size_usdt=r[8],
                opened_at=r[9],
                closed_at=r[10],
                exit_price=r[11],
                pnl_pct=r[12],
                verdict=r[13],
            )
        )
    return out


# ==========================
# SAFE REQUEST LAYER
# ==========================

async def safe_request(
    session: aiohttp.ClientSession,
    method: str,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    max_retries: int = 5,
    base_backoff: float = 0.5,
) -> Any:
    """IP-safe wrapper with backoff and 429/418 handling."""
    for attempt in range(max_retries):
        try:
            async with session.request(method, url, params=params, timeout=10) as resp:
                if resp.status in (418, 429):
                    wait = base_backoff * (2 ** attempt)
                    console.log(
                        f"[yellow]Rate-limit ({resp.status}) on {url}, backoff {wait:.1f}s[/]"
                    )
                    await asyncio.sleep(wait)
                    continue
                resp.raise_for_status()
                return await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            wait = base_backoff * (2 ** attempt)
            console.log(f"[red]HTTP error {e!r} on {url}, backoff {wait:.1f}s[/]")
            await asyncio.sleep(wait)
    console.log(f"[red]Max retries exceeded for {url}[/]")
    return None


# ==========================
# BYBIT OI WORKER
# ==========================

class BybitOIState:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._oi: Dict[str, float] = {}

    async def update(self, symbol: str, oi: float) -> None:
        async with self._lock:
            self._oi[symbol] = oi

    async def snapshot(self) -> Dict[str, float]:
        async with self._lock:
            return dict(self._oi)


async def bybit_oi_worker(state: BybitOIState, stop_event: asyncio.Event) -> None:
    """
    Stream Bybit linear tickers; map to Binance symbols (e.g. BTCUSDT).
    topic: 'tickers.linear'
    """
    while not stop_event.is_set():
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(BYBIT_LINEAR_WS, heartbeat=20) as ws:
                    sub_msg = {
                        "op": "subscribe",
                        "args": ["tickers.linear"],
                    }
                    await ws.send_json(sub_msg)
                    console.print(
                        f"[green][INFO] Connected to Bybit WS OI ticker ({BYBIT_LINEAR_WS})[/]"
                    )
                    async for msg in ws:
                        if stop_event.is_set():
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = msg.json()
                            topic = data.get("topic", "")
                            if topic != "tickers.linear":
                                continue
                            for item in data.get("data", []):
                                bybit_symbol = item.get("symbol", "")
                                last_oi = item.get("openInterest", None)
                                if last_oi is None:
                                    continue
                                try:
                                    # Bybit sends string
                                    oi_val = float(last_oi)
                                except (TypeError, ValueError):
                                    continue

                                # Map to Binance symbol (same naming for linear USDT perps)
                                await state.update(bybit_symbol, oi_val)
                        elif msg.type in (
                            aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.ERROR,
                        ):
                            break
        except Exception as e:
            if stop_event.is_set():
                break
            console.print(
                f"[red][ERROR] Bybit OI worker error: {e!r}. Reconnecting in 5s...[/]"
            )
            await asyncio.sleep(5)


# ==========================
# BRUTAL WEAKNESS SCORE
# ==========================

def compute_weakness_score(s: SymbolSnapshot) -> float:
    """
    Rough mirror of your blueprint:

    - Up a lot in 24h  → crowded longs
    - Near 24h high   → late longs at risk
    - Positive FR     → longs paying shorts
    - OI flat/down    → weak conviction / unwind
    - Ignore dead coins with small move/volume
    """
    if s.quote_volume < MIN_QUOTE_VOLUME_USDT:
        return -999.0

    score = 0.0

    # 24h momentum: only care if up
    if s.price_change_pct > 0:
        score += 0.6 * s.price_change_pct

    # Distance from low (off the floor)
    if s.dist_from_low_pct > 5:
        score += 0.4 * (s.dist_from_low_pct - 5)

    # Penalize being TOO close to high (< 1% from top → "might be blow-off")
    if s.dist_from_high_pct < 1.0:
        score -= 5.0
    else:
        # mild bump when price rolled under the extreme
        score += 0.3 * min(s.dist_from_high_pct, 5.0)

    # Funding: positive = crowded longs
    if s.funding_rate > 0:
        score += 10000 * min(s.funding_rate, 0.0005)  # clamp

    # OI change: we *like* flat/down for short
    oc = s.oi_change_pct
    if oc < 0:
        score += min(abs(oc), 10.0)
    elif oc > 3:
        # if OI is pumping with price up → dangerous squeeze
        score -= min(oc, 10.0)

    return score


# ==========================
# UNIVERSE & SCAN
# ==========================

async def fetch_universe(session: aiohttp.ClientSession) -> List[str]:
    url = f"{BINANCE_FAPI_URL}/fapi/v1/exchangeInfo"
    data = await safe_request(session, "GET", url)
    if not data:
        return []
    symbols = []
    for s in data.get("symbols", []):
        if s.get("quoteAsset") == "USDT" and s.get("contractType") == "PERPETUAL":
            symbols.append(s["symbol"])
    symbols.sort()
    if MAX_SYMBOLS and len(symbols) > MAX_SYMBOLS:
        symbols = symbols[:MAX_SYMBOLS]
    return symbols


async def fetch_24h_tickers(
    session: aiohttp.ClientSession,
) -> Dict[str, Dict[str, Any]]:
    url = f"{BINANCE_FAPI_URL}/fapi/v1/ticker/24hr"
    data = await safe_request(session, "GET", url)
    out: Dict[str, Dict[str, Any]] = {}
    if not data:
        return out
    for item in data:
        sym = item.get("symbol")
        if not sym:
            continue
        out[sym] = item
    return out


async def fetch_funding_rates(
    session: aiohttp.ClientSession,
) -> Dict[str, float]:
    url = f"{BINANCE_FAPI_URL}/fapi/v1/premiumIndex"
    data = await safe_request(session, "GET", url)
    out: Dict[str, float] = {}
    if not data:
        return out
    for item in data:
        sym = item.get("symbol")
        fr = item.get("lastFundingRate")
        if not sym or fr is None:
            continue
        try:
            out[sym] = float(fr)
        except ValueError:
            continue
    return out


async def build_snapshots(
    symbols: List[str],
    t24: Dict[str, Dict[str, Any]],
    fr_map: Dict[str, float],
    oi_map: Dict[str, float],
    prev_snaps: Dict[str, SymbolSnapshot],
) -> Dict[str, SymbolSnapshot]:
    snaps: Dict[str, SymbolSnapshot] = {}
    for sym in symbols:
        t = t24.get(sym)
        if not t:
            continue
        try:
            last_price = float(t["lastPrice"])
            high = float(t["highPrice"])
            low = float(t["lowPrice"])
            quote_vol = float(t["quoteVolume"])
            pct = float(t["priceChangePercent"])
        except (KeyError, ValueError):
            continue

        funding = fr_map.get(sym, 0.0)
        prev = prev_snaps.get(sym)
        prev_oi = prev.oi if prev else None

        oi = oi_map.get(sym, prev_oi)

        snap = SymbolSnapshot(
            symbol=sym,
            last_price=last_price,
            high_price=high,
            low_price=low,
            price_change_pct=pct,
            quote_volume=quote_vol,
            funding_rate=funding,
            oi=oi,
            prev_oi=prev_oi,
        )
        snaps[sym] = snap
    return snaps


# ==========================
# UI BUILDERS
# ==========================

def make_radar_table(signals: List[Signal]) -> Table:
    table = Table(
        title="Brutal Weakness Radar – Top Shorts",
        show_lines=False,
        expand=True,
    )
    table.add_column("#", justify="right", width=3)
    table.add_column("Symbol", justify="left", width=10)
    table.add_column("Score", justify="right", width=8)
    table.add_column("Price", justify="right", width=11)
    table.add_column("24h%", justify="right", width=7)
    table.add_column("Dist↓High%", justify="right", width=11)
    table.add_column("Funding", justify="right", width=8)
    table.add_column("OIΔ%", justify="right", width=7)

    for idx in range(RADAR_ROWS):
        if idx < len(signals):
            sig = signals[idx]
            s = sig.snapshot
            score_str = f"{sig.score:6.1f}"
            row_style = "red" if sig.score > 0 else "dim"
            table.add_row(
                str(idx + 1),
                s.symbol,
                score_str,
                f"{s.last_price:.4g}",
                f"{s.price_change_pct:+5.1f}",
                f"{s.dist_from_high_pct:5.1f}",
                f"{s.funding_rate:+.4f}",
                f"{s.oi_change_pct:+5.1f}",
                style=row_style,
            )
        else:
            table.add_row("", "", "", "", "", "", "", "")
    return table


def make_open_trades_table(trades: List[Trade], price_map: Dict[str, float]) -> Table:
    table = Table(
        title="📈 Live Shorts (Paper)",
        show_lines=False,
        expand=True,
    )
    table.add_column("ID", justify="right", width=4)
    table.add_column("Symbol", width=10)
    table.add_column("Entry", justify="right", width=10)
    table.add_column("Last", justify="right", width=10)
    table.add_column("SL", justify="right", width=10)
    table.add_column("TP1", justify="right", width=10)
    table.add_column("TP2", justify="right", width=10)
    table.add_column("TP3", justify="right", width=10)
    table.add_column("PnL%", justify="right", width=7)

    for idx in range(OPEN_ROWS):
        if idx < len(trades):
            t = trades[idx]
            last = price_map.get(t.symbol, t.entry)
            if t.entry > 0:
                pnl_pct = (t.entry - last) / t.entry * 100.0
            else:
                pnl_pct = 0.0
            if pnl_pct > 3:
                style = "bold green"
            elif pnl_pct < -2:
                style = "bold red"
            else:
                style = "white"

            table.add_row(
                str(t.id),
                t.symbol,
                f"{t.entry:.4g}",
                f"{last:.4g}",
                f"{t.sl:.4g}",
                f"{t.tp1:.4g}",
                f"{t.tp2:.4g}",
                f"{t.tp3:.4g}",
                f"{pnl_pct:+5.1f}",
                style=style,
            )
        else:
            table.add_row("", "", "", "", "", "", "", "", "")
    return table


def make_closed_trades_table(trades: List[Trade]) -> Table:
    table = Table(
        title="✅ Closed Shorts",
        show_lines=False,
        expand=True,
    )
    table.add_column("ID", justify="right", width=4)
    table.add_column("Symbol", width=10)
    table.add_column("Verdict", width=8)
    table.add_column("Entry", justify="right", width=10)
    table.add_column("Exit", justify="right", width=10)
    table.add_column("PnL%", justify="right", width=7)
    table.add_column("Age", justify="right", width=6)

    now = time.time()
    for idx in range(CLOSED_ROWS):
        if idx < len(trades):
            t = trades[idx]
            age_min = (t.closed_at - t.opened_at) / 60.0 if t.closed_at else 0.0
            verdict = t.verdict or ""
            if verdict == "WIN":
                style = "green"
            elif verdict == "LOSS":
                style = "red"
            else:
                style = "white"
            table.add_row(
                str(t.id),
                t.symbol,
                verdict,
                f"{t.entry:.4g}",
                f"{(t.exit_price or 0):.4g}",
                f"{(t.pnl_pct or 0):+5.1f}",
                f"{age_min:4.0f}m",
                style=style,
            )
        else:
            table.add_row("", "", "", "", "", "", "")
    return table


def build_layout(
    radar: Table,
    open_trades: Table,
    closed_trades: Table,
    header_text: str,
) -> Layout:
    layout = Layout()
    layout.split(
        Layout(name="header", size=3),
        Layout(name="body", ratio=1),
    )
    layout["body"].split(
        Layout(name="top"),
        Layout(name="middle"),
        Layout(name="bottom"),
    )
    layout["header"].update(
        Align.center(
            Panel(
                header_text,
                style="bold cyan",
            )
        )
    )
    layout["top"].update(Panel(radar, border_style="red"))
    layout["middle"].update(Panel(open_trades, border_style="yellow"))
    layout["bottom"].update(Panel(closed_trades, border_style="green"))
    return layout


# ==========================
# MAIN SCAN LOOP
# ==========================

async def scan_loop() -> None:
    init_db()
    console.print(
        f"[green][INFO] Brutal Weakness fullscanner starting…[/]\n"
    )

    bybit_state = BybitOIState()
    bybit_stop = asyncio.Event()
    bybit_task = asyncio.create_task(bybit_oi_worker(bybit_state, bybit_stop))

    prev_snaps: Dict[str, SymbolSnapshot] = {}
    last_prices: Dict[str, float] = {}

    stop_event = asyncio.Event()

    def handle_sig(*_: Any) -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_sig)
        except NotImplementedError:
            # Windows
            pass

    async with aiohttp.ClientSession() as session:
        symbols = await fetch_universe(session)
        console.print(
            f"[cyan][INFO] Binance USDT-perp universe: {len(symbols)} symbols[/]"
        )

        with Live(console=console, refresh_per_second=2) as live:
            while not stop_event.is_set():
                scan_started = time.time()
                t24 = await fetch_24h_tickers(session)
                fr_map = await fetch_funding_rates(session)
                oi_map = await bybit_state.snapshot()

                snaps = await build_snapshots(
                    symbols, t24, fr_map, oi_map, prev_snaps
                )
                prev_snaps = snaps

                # Build signals
                signals: List[Signal] = []
                for sym, snap in snaps.items():
                    score = compute_weakness_score(snap)
                    if score <= 0:
                        continue
                    sig = Signal(symbol=sym, score=score, snapshot=snap)
                    signals.append(sig)

                # Sort & keep top
                signals.sort(key=lambda x: x.score, reverse=True)
                top_signals = signals[:RADAR_ROWS]

                # Log & open trades from top 3 if not already open
                open_trades = fetch_open_trades()
                open_symbols = {t.symbol for t in open_trades}
                for sig in top_signals[:3]:
                    log_signal(sig)
                    if len(open_trades) >= MAX_OPEN_TRADES:
                        break
                    if sig.symbol in open_symbols:
                        continue
                    new_trade = open_trade_from_signal(sig)
                    if new_trade:
                        open_trades.append(new_trade)
                        open_symbols.add(sig.symbol)

                # Update last price map
                for sym, snap in snaps.items():
                    last_prices[sym] = snap.last_price

                # Manage exits
                open_trades = fetch_open_trades()
                updated_open: List[Trade] = []
                for t in open_trades:
                    last = last_prices.get(t.symbol, t.entry)
                    # SHORT exit rules
                    hit_sl = last >= t.sl
                    hit_tp3 = last <= t.tp3
                    hit_tp2 = last <= t.tp2
                    hit_tp1 = last <= t.tp1

                    verdict: Optional[str] = None
                    exit_price: Optional[float] = None

                    if hit_sl:
                        verdict = "LOSS"
                        exit_price = t.sl
                    elif hit_tp3:
                        verdict = "WIN"
                        exit_price = t.tp3
                    elif hit_tp2:
                        verdict = "WIN"
                        exit_price = t.tp2
                    elif hit_tp1:
                        verdict = "WIN"
                        exit_price = t.tp1

                    if verdict and exit_price is not None:
                        close_trade(t, exit_price, verdict)
                    else:
                        updated_open.append(t)

                closed_trades = fetch_closed_trades()

                radar_table = make_radar_table(top_signals)
                open_table = make_open_trades_table(updated_open, last_prices)
                closed_table = make_closed_trades_table(closed_trades)

                header = (
                    f"Brutal Weakness – Fullscanner | "
                    f"Symbols: {len(symbols)} | "
                    f"Open shorts: {len(updated_open)} | "
                    f"DB: {DB_PATH.name}"
                )

                layout = build_layout(
                    radar_table,
                    open_table,
                    closed_table,
                    header,
                )
                live.update(layout)

                elapsed = time.time() - scan_started
                sleep_for = max(1.0, SCAN_INTERVAL - elapsed)
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=sleep_for)
                except asyncio.TimeoutError:
                    pass

    # Graceful stop Bybit worker
    bybit_stop.set()
    try:
        await asyncio.wait_for(bybit_task, timeout=5)
    except asyncio.TimeoutError:
        bybit_task.cancel()
        try:
            await bybit_task
        except asyncio.CancelledError:
            pass

    console.print("[yellow][INFO] Scanner stopped gracefully.[/]")


def main() -> None:
    try:
        asyncio.run(scan_loop())
    except KeyboardInterrupt:
        console.print("[red]Interrupted by user[/]")


if __name__ == "__main__":
    main()
