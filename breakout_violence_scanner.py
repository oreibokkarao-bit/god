import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Dict, Tuple, List, Optional
import statistics
import sqlite3
from pathlib import Path
import math

import httpx
import websockets
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from rich.table import Table as RichTable
from rich import box

console = Console()

# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

BINANCE_FAPI_REST = "https://fapi.binance.com"
BINANCE_FAPI_WS_ALL = "wss://fstream.binance.com/ws/!ticker@arr"

BYBIT_REST = "https://api.bybit.com"
BYBIT_WS_LINEAR = "wss://stream.bybit.com/v5/public/linear"

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

MIN_24H_QUOTE_VOLUME = 100_000      # USDT; keeps microcaps, kills pure dust
DEPTH_LEVELS = 20
ORDERBOOK_REFRESH_SEC = 40
FUNDING_REFRESH_SEC = 120
TABLE_REFRESH_SEC = 5
TOP_OBI_CANDIDATES = 60
MAX_MINUTE_CANDLES = 300            # ~5h of 1m candles

SIGNAL_DB_PATH = Path("violent_breakout_sniper.sqlite")
SIGNAL_MIN_INTERVAL_SEC = 15 * 60   # min gap per symbol between signal logs
SIGNAL_A_PLUS_THRESHOLD = 75.0
SIGNAL_A_THRESHOLD = 60.0
SIGNAL_RADAR_THRESHOLD = 50.0
SIGNAL_HORIZON_SEC = 24 * 60 * 60   # 24h

# ---------------------------------------------------------------------------

def clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, x))


def safe_float(x, default: float = 0.0) -> float:
    """
    Robust float() wrapper for exchange fields:
    - '', '   ', None, 'null', 'NaN' => default
    - int/float => float
    - numeric strings => float
    """
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if s == "" or s.lower() in {"null", "nan"}:
            return default
        return float(s)
    except Exception:
        return default


@dataclass
class SymbolState:
    exchange: str            # "binance_fut" / "bybit_lin"
    symbol: str              # raw exchange symbol ("BTCUSDT")
    canonical: str           # "BTC/USDT"

    last_price: float = 0.0

    # Factors (0-1)
    vol_comp: float = 0.0        # volatility compression
    deriv_bias: float = 0.0      # funding / derivatives asymmetry
    obi: float = 0.5             # order-book imbalance (0.5 neutral)
    liquidity: float = 0.0       # 24h participation
    flow_score: float = 0.0      # RVOL-based flow factor

    violence_score: float = 0.0  # composite 0–100

    # 24h backbone
    _high24: float = 0.0
    _low24: float = 0.0
    _qv24: float = 0.0           # last seen 24h quote volume
    _funding_rate: float = 0.0
    base_vol_comp_24h: float = 0.0

    # 1m OHLCV builder
    minute_bucket: int = 0       # unix_minute index
    minute_open: float = 0.0
    minute_high: float = 0.0
    minute_low: float = 0.0
    minute_close: float = 0.0
    minute_volume: float = 0.0

    # Finished 1m candles: (bucket, o, h, l, c, v)
    candles: List[Tuple[int, float, float, float, float, float]] = field(default_factory=list)

    # RVOL stack
    rvol_5m: float = 0.0
    rvol_15m: float = 0.0
    rvol_60m: float = 0.0


StateKey = Tuple[str, str]
StateMap = Dict[StateKey, SymbolState]

# SQLite global
_db_conn: Optional[sqlite3.Connection] = None
_last_signal_logged: Dict[StateKey, float] = {}

# ---------------------------------------------------------------------------
# SQLite helpers
# ---------------------------------------------------------------------------

def get_db() -> sqlite3.Connection:
    global _db_conn
    if _db_conn is None:
        _db_conn = sqlite3.connect(SIGNAL_DB_PATH, check_same_thread=False)
        _db_conn.row_factory = sqlite3.Row
        init_db(_db_conn)
    else:
        init_db(_db_conn)
    return _db_conn


def init_db(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")

    # Base schema
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            canonical TEXT NOT NULL,
            tier TEXT NOT NULL,
            ts_signal INTEGER NOT NULL,
            violence_score REAL NOT NULL,
            vol_comp REAL NOT NULL,
            flow_score REAL NOT NULL,
            deriv_bias REAL NOT NULL,
            obi REAL NOT NULL,
            liquidity REAL NOT NULL,
            entry_price REAL NOT NULL,
            max_price REAL NOT NULL,
            min_price REAL NOT NULL,
            status TEXT NOT NULL DEFAULT 'OPEN',
            closed_ts INTEGER,
            max_runup REAL,
            max_drawdown REAL,
            hit_20p INTEGER
        );
        """
    )

    # Trade-engine columns
    upgrade_cols = [
        ("ideal_entry", "REAL"),
        ("sl_price", "REAL"),
        ("tp1", "REAL"),
        ("tp2", "REAL"),
        ("tp3", "REAL"),
        ("rr1", "REAL"),
        ("rr2", "REAL"),
        ("rr3", "REAL"),
    ]
    for name, coltype in upgrade_cols:
        try:
            cur.execute(f"ALTER TABLE signals ADD COLUMN {name} {coltype};")
        except sqlite3.OperationalError:
            pass

    conn.commit()


def compute_trade_levels(state: SymbolState) -> Dict[str, float]:
    """
    High RR trade engine:
    - volatility-based, wick-proof SL
    - pullback entry
    - magnetic TP ladder (biased toward +20–25%)
    """
    entry_spot = state.last_price or 0.0
    if entry_spot <= 0:
        return {
            "ideal_entry": 0.0,
            "sl_price": 0.0,
            "tp1": 0.0,
            "tp2": 0.0,
            "tp3": 0.0,
            "rr1": 0.0,
            "rr2": 0.0,
            "rr3": 0.0,
        }

    # 1h range as ATR-ish proxy
    r_1h = _intraday_range_pct(state, 60)
    if r_1h is not None:
        range_pct = r_1h * 100.0
    elif state._high24 > 0 and state._low24 > 0:
        range_pct = (state._high24 - state._low24) / entry_spot * 100.0
    else:
        range_pct = 4.0

    base_stop_pct = max(3.0, min(range_pct * 0.75, 12.0))  # 3–12%
    risk = base_stop_pct / 100.0

    # Swing low for wick-proofing (last 20 candles)
    swing_low = None
    for (_b, _o, _h, l, _c, _v) in state.candles[-20:]:
        if swing_low is None or l < swing_low:
            swing_low = l

    if swing_low is not None and swing_low < entry_spot:
        sl_candidate = swing_low * 0.985  # ~1.5% below swing low
        sl_price = min(entry_spot * (1 - risk), sl_candidate)
    else:
        sl_price = entry_spot * (1 - risk)

    sl_price = max(sl_price, entry_spot * 0.5)  # never more than 50% loss

    # Pullback entry: 35% of risk as dip
    pullback_frac = 0.35
    ideal_entry = entry_spot * (1 - pullback_frac * risk)

    risk_abs = max(ideal_entry - sl_price, 1e-9)

    tp1_raw = ideal_entry + 1.8 * risk_abs
    tp2_raw = ideal_entry + 3.0 * risk_abs
    tp3_candidate = ideal_entry + 4.0 * risk_abs

    # Pump bias: at least +25%, cap around +35%
    target25 = ideal_entry * 1.25
    tp3 = max(tp3_candidate, target25)
    tp3 = min(tp3, ideal_entry * 1.35)

    tp1 = tp1_raw
    tp2 = tp2_raw

    rr1 = (tp1 - ideal_entry) / risk_abs
    rr2 = (tp2 - ideal_entry) / risk_abs
    rr3 = (tp3 - ideal_entry) / risk_abs

    return {
        "ideal_entry": ideal_entry,
        "sl_price": sl_price,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "rr1": rr1,
        "rr2": rr2,
        "rr3": rr3,
    }


def log_signal(state: SymbolState, tier: str, now: float) -> None:
    conn = get_db()
    cur = conn.cursor()

    levels = compute_trade_levels(state)
    ideal_entry = levels["ideal_entry"]
    sl_price = levels["sl_price"]
    tp1 = levels["tp1"]
    tp2 = levels["tp2"]
    tp3 = levels["tp3"]
    rr1 = levels["rr1"]
    rr2 = levels["rr2"]
    rr3 = levels["rr3"]

    cur.execute(
        """
        INSERT INTO signals (
            exchange, symbol, canonical,
            tier, ts_signal, violence_score,
            vol_comp, flow_score, deriv_bias, obi, liquidity,
            entry_price, max_price, min_price, status,
            ideal_entry, sl_price, tp1, tp2, tp3, rr1, rr2, rr3
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN',
                ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        (
            state.exchange,
            state.symbol,
            state.canonical,
            tier,
            int(now),
            state.violence_score,
            state.vol_comp,
            state.flow_score,
            state.deriv_bias,
            state.obi,
            state.liquidity,
            state.last_price,
            state.last_price,
            state.last_price,
            ideal_entry,
            sl_price,
            tp1,
            tp2,
            tp3,
            rr1,
            rr2,
            rr3,
        ),
    )
    conn.commit()


def update_open_signals(states: StateMap) -> None:
    """
    For each OPEN signal, update max/min price and close if 24h elapsed.
    """
    conn = get_db()
    cur = conn.cursor()
    now = int(time.time())

    cur.execute("SELECT * FROM signals WHERE status='OPEN';")
    rows = cur.fetchall()
    for row in rows:
        ex = row["exchange"]
        sym = row["symbol"]
        key: StateKey = ("binance_fut" if ex == "binance_fut" else "bybit_lin", sym)
        st = states.get(key)
        if st is None or st.last_price <= 0:
            continue

        entry_price = row["entry_price"]
        max_price = max(row["max_price"], st.last_price)
        min_price = min(row["min_price"], st.last_price)

        cur.execute(
            """
            UPDATE signals
            SET max_price=?, min_price=?
            WHERE id=?;
            """,
            (max_price, min_price, row["id"]),
        )

        ts_signal = row["ts_signal"]
        if now - ts_signal >= SIGNAL_HORIZON_SEC:
            max_runup = (max_price - entry_price) / entry_price * 100.0
            max_drawdown = (min_price - entry_price) / entry_price * 100.0
            hit_20p = 1 if max_runup >= 20.0 else 0
            cur.execute(
                """
                UPDATE signals
                SET status='CLOSED',
                    closed_ts=?,
                    max_runup=?,
                    max_drawdown=?,
                    hit_20p=?
                WHERE id=?;
                """,
                (now, max_runup, max_drawdown, hit_20p, row["id"]),
            )

    conn.commit()


def get_event_stats():
    """
    Return stats per tier: { tier: (count, p20, avg_runup, avg_drawdown) }
    """
    conn = get_db()
    cur = conn.cursor()
    stats = {}
    for tier in ("A+", "A", "Radar"):
        cur.execute(
            """
            SELECT COUNT(*) AS n,
                   COALESCE(SUM(hit_20p),0) AS hits,
                   AVG(max_runup) AS avg_ru,
                   AVG(max_drawdown) AS avg_dd
            FROM signals
            WHERE status='CLOSED' AND tier=?;
            """,
            (tier,),
        )
        row = cur.fetchone()
        n = row["n"] or 0
        hits = row["hits"] or 0
        if n > 0:
            p20 = hits / n * 100.0
        else:
            p20 = 0.0
        stats[tier] = (
            n,
            round(p20, 1),
            round(row["avg_ru"] or 0.0, 1),
            round(row["avg_dd"] or 0.0, 1),
        )
    return stats

# ---------------------------------------------------------------------------
# Feature calculators
# ---------------------------------------------------------------------------

def update_from_24h_ticker(state: SymbolState,
                           last_price: float,
                           high: float,
                           low: float,
                           quote_vol: float) -> None:
    state.last_price = last_price
    state._high24 = high
    state._low24 = low
    state._qv24 = quote_vol

    if last_price > 0 and high > 0 and low > 0:
        range_pct = (high - low) / last_price
        base_comp = clamp((0.20 - range_pct) / 0.20)
    else:
        base_comp = 0.0

    state.base_vol_comp_24h = base_comp
    if not state.candles:
        state.vol_comp = base_comp

    liq_raw = quote_vol / 10_000_000.0
    state.liquidity = clamp(liq_raw)


def _intraday_range_pct(state: SymbolState, window_minutes: int) -> Optional[float]:
    if not state.candles or state.last_price <= 0:
        return None

    last_bucket = state.candles[-1][0]
    min_bucket = last_bucket - window_minutes + 1

    hi = None
    lo = None
    for bucket, _o, h, l, _c, _v in state.candles:
        if bucket < min_bucket:
            continue
        if hi is None or h > hi:
            hi = h
        if lo is None or l < lo:
            lo = l

    if hi is None or lo is None:
        return None

    return (hi - lo) / state.last_price


def _compute_rvol(state: SymbolState, window: int, baseline_window: int = 60) -> float:
    c = state.candles
    if len(c) < baseline_window + window:
        return 0.0

    recent = [v for (_b, _o, _h, _l, _c, v) in c[-window:]]
    past = [v for (_b, _o, _h, _l, _c, v) in c[-(baseline_window + window):-window]]

    if not past:
        return 0.0

    recent_mean = sum(recent) / window
    past_med = statistics.median(past)
    if past_med <= 0:
        return 0.0

    r = recent_mean / past_med
    return max(0.0, min(r, 10.0))


def update_intraday_features(state: SymbolState) -> None:
    r_1h = _intraday_range_pct(state, 60)
    r_4h = _intraday_range_pct(state, 240)

    comps = []
    if r_1h is not None:
        comps.append(clamp((0.03 - r_1h) / 0.03))
    if r_4h is not None:
        comps.append(clamp((0.10 - r_4h) / 0.10))

    if comps:
        intraday_comp = sum(comps) / len(comps)
        state.vol_comp = clamp(0.6 * intraday_comp + 0.4 * state.base_vol_comp_24h)
    else:
        state.vol_comp = state.base_vol_comp_24h

    state.rvol_5m = _compute_rvol(state, 5)
    state.rvol_15m = _compute_rvol(state, 15)
    state.rvol_60m = _compute_rvol(state, 60)

    parts = []

    def norm(v: float, target: float = 3.0) -> float:
        return clamp(v / target)

    if state.rvol_5m > 0:
        parts.append(0.4 * norm(state.rvol_5m))
    if state.rvol_15m > 0:
        parts.append(0.3 * norm(state.rvol_15m))
    if state.rvol_60m > 0:
        parts.append(0.3 * norm(state.rvol_60m))

    state.flow_score = clamp(sum(parts))


def on_price_tick(state: SymbolState,
                  price: float,
                  ts_ms: Optional[int] = None,
                  vol_delta: float = 0.0) -> None:
    if price <= 0:
        return

    state.last_price = price
    now_ms = ts_ms if ts_ms is not None else int(time.time() * 1000)
    minute = now_ms // 60_000

    if state.minute_bucket == 0:
        state.minute_bucket = minute
        state.minute_open = state.minute_high = state.minute_low = state.minute_close = price
        state.minute_volume = vol_delta
        return

    if minute == state.minute_bucket:
        if price > state.minute_high:
            state.minute_high = price
        if price < state.minute_low:
            state.minute_low = price
        state.minute_close = price
        state.minute_volume += vol_delta
        return

    state.candles.append(
        (state.minute_bucket,
         state.minute_open,
         state.minute_high,
         state.minute_low,
         state.minute_close,
         state.minute_volume)
    )

    if len(state.candles) > MAX_MINUTE_CANDLES:
        state.candles = state.candles[-MAX_MINUTE_CANDLES:]

    state.minute_bucket = minute
    state.minute_open = state.minute_high = state.minute_low = state.minute_close = price
    state.minute_volume = vol_delta

    update_intraday_features(state)


def update_from_funding(state: SymbolState, funding_rate: float) -> None:
    state._funding_rate = funding_rate
    extreme = clamp(abs(funding_rate) / 0.0005)

    if funding_rate < 0:
        bias = 0.7 * extreme + 0.3 * state.vol_comp
    else:
        bias = 0.3 * extreme + 0.2 * state.vol_comp

    state.deriv_bias = clamp(bias)


def compute_obi_from_depth(bids: List[List[str]],
                           asks: List[List[str]],
                           last_price: float) -> float:
    if last_price <= 0:
        return 0.5

    lo = last_price * 0.95
    hi = last_price * 1.05

    def side_notional(levels, is_bid: bool) -> float:
        total = 0.0
        for p_str, q_str in levels:
            p = safe_float(p_str, 0.0)
            q = safe_float(q_str, 0.0)
            if p <= 0 or q <= 0:
                continue
            if is_bid and p < lo:
                continue
            if (not is_bid) and p > hi:
                continue
            total += p * q
        return total

    bid_notional = side_notional(bids, True)
    ask_notional = side_notional(asks, False)
    total = bid_notional + ask_notional
    if total <= 0:
        return 0.5

    raw = (bid_notional - ask_notional) / total
    return clamp(0.5 + 0.5 * raw)


def recompute_violence_score(state: SymbolState) -> None:
    w_vol = 0.30
    w_deriv = 0.25
    w_flow = 0.20
    w_obi = 0.15
    w_liq = 0.10

    score_0_1 = (
        w_vol * state.vol_comp +
        w_deriv * state.deriv_bias +
        w_flow * state.flow_score +
        w_obi * state.obi +
        w_liq * state.liquidity
    )
    state.violence_score = round(100.0 * clamp(score_0_1), 2)


def classify_tier(state: SymbolState):
    signals = []
    signals.append(("comp", state.vol_comp >= 0.7))
    signals.append(("flow", state.flow_score >= 0.7))
    signals.append(("deriv", state.deriv_bias >= 0.6))
    signals.append(("obi", state.obi >= 0.6))
    signals.append(("liq", state.liquidity >= 0.3))

    k = sum(1 for _, ok in signals if ok)
    n = len(signals)

    if k >= 4 and state.violence_score >= 70:
        tier = "A+"
    elif k >= 3 and state.violence_score >= 55:
        tier = "A"
    elif k >= 2:
        tier = "Radar"
    else:
        tier = "-"

    return tier, k, n

# ---------------------------------------------------------------------------
# Universe discovery (REST)
# ---------------------------------------------------------------------------

async def load_binance_universe(client: httpx.AsyncClient,
                                states: StateMap) -> None:
    r = await client.get(f"{BINANCE_FAPI_REST}/fapi/v1/ticker/24hr")
    r.raise_for_status()
    data = r.json()

    count = 0
    for item in data:
        symbol = item.get("symbol")
        if not symbol or not symbol.endswith("USDT"):
            continue

        qv = safe_float(item.get("quoteVolume"), 0.0)
        if qv < MIN_24H_QUOTE_VOLUME:
            continue

        last_price = safe_float(item.get("lastPrice"), 0.0)
        high = safe_float(item.get("highPrice"), last_price)
        low = safe_float(item.get("lowPrice"), last_price)

        base = symbol[:-4]
        canonical = f"{base}/USDT"

        key: StateKey = ("binance_fut", symbol)
        st = SymbolState(exchange="binance_fut",
                         symbol=symbol,
                         canonical=canonical)
        update_from_24h_ticker(st, last_price, high, low, qv)
        recompute_violence_score(st)
        states[key] = st
        count += 1

    console.print(f"[green]Binance futures universe:[/green] {count} symbols")


async def load_bybit_universe(client: httpx.AsyncClient,
                              states: StateMap) -> None:
    params = {"category": "linear"}
    r = await client.get(f"{BYBIT_REST}/v5/market/tickers", params=params)
    r.raise_for_status()
    payload = r.json()
    result = payload.get("result", {}) or {}
    rows = result.get("list", []) or []

    count = 0
    for item in rows:
        symbol = item.get("symbol")
        quote = item.get("quoteCoin", "USDT")
        if not symbol or quote != "USDT":
            continue

        last_price = safe_float(item.get("lastPrice"), 0.0)
        high = safe_float(item.get("highPrice24h"), last_price)
        low = safe_float(item.get("lowPrice24h"), last_price)
        qv = safe_float(item.get("turnover24h"), 0.0)

        if qv < MIN_24H_QUOTE_VOLUME:
            continue

        base = symbol.replace("USDT", "")
        canonical = f"{base}/USDT"

        key: StateKey = ("bybit_lin", symbol)
        st = SymbolState(exchange="bybit_lin",
                         symbol=symbol,
                         canonical=canonical)
        update_from_24h_ticker(st, last_price, high, low, qv)
        recompute_violence_score(st)
        states[key] = st
        count += 1

    console.print(f"[green]Bybit linear universe:[/green] {count} symbols")

# ---------------------------------------------------------------------------
# Funding refresh
# ---------------------------------------------------------------------------

async def refresh_binance_funding(client: httpx.AsyncClient,
                                  states: StateMap) -> None:
    r = await client.get(f"{BINANCE_FAPI_REST}/fapi/v1/premiumIndex")
    r.raise_for_status()
    data = r.json()
    funding_map = {
        row["symbol"]: safe_float(row.get("lastFundingRate"), 0.0)
        for row in data
        if "symbol" in row
    }

    for (ex, symbol), st in states.items():
        if ex != "binance_fut":
            continue
        f = funding_map.get(symbol)
        if f is None:
            continue
        update_from_funding(st, f)
        recompute_violence_score(st)


async def refresh_bybit_funding(client: httpx.AsyncClient,
                                states: StateMap) -> None:
    params = {"category": "linear"}
    r = await client.get(f"{BYBIT_REST}/v5/market/tickers", params=params)
    r.raise_for_status()
    payload = r.json()
    result = payload.get("result", {}) or {}
    rows = result.get("list", []) or []

    funding_map = {
        row["symbol"]: safe_float(row.get("fundingRate"), 0.0)
        for row in rows
        if "symbol" in row
    }

    for (ex, symbol), st in states.items():
        if ex != "bybit_lin":
            continue
        f = funding_map.get(symbol)
        if f is None:
            continue
        update_from_funding(st, f)
        recompute_violence_score(st)


async def periodic_funding_refresh(states: StateMap) -> None:
    async with httpx.AsyncClient(timeout=10.0) as client:
        while True:
            try:
                await refresh_binance_funding(client, states)
                await refresh_bybit_funding(client, states)
            except Exception as e:
                console.print(f"[red]Funding refresh error:[/red] {e}")
            await asyncio.sleep(FUNDING_REFRESH_SEC)

# ---------------------------------------------------------------------------
# Order book imbalance snapshots
# ---------------------------------------------------------------------------

async def fetch_binance_depth(client: httpx.AsyncClient, symbol: str):
    params = {"symbol": symbol, "limit": DEPTH_LEVELS}
    r = await client.get(f"{BINANCE_FAPI_REST}/fapi/v1/depth", params=params)
    r.raise_for_status()
    return r.json()


async def fetch_bybit_depth(client: httpx.AsyncClient, symbol: str):
    params = {"category": "linear", "symbol": symbol, "limit": DEPTH_LEVELS}
    r = await client.get(f"{BYBIT_REST}/v5/market/orderbook", params=params)
    r.raise_for_status()
    data = r.json()["result"]
    bids = [[x["price"], x["size"]] for x in data.get("b", [])]
    asks = [[x["price"], x["size"]] for x in data.get("a", [])]
    return {"bids": bids, "asks": asks}


async def periodic_orderbook_refresh(states: StateMap) -> None:
    async with httpx.AsyncClient(timeout=10.0) as client:
        while True:
            try:
                all_states = list(states.values())
                for st in all_states:
                    recompute_violence_score(st)

                all_states.sort(
                    key=lambda s: (s.vol_comp + s.deriv_bias + 0.3 * s.liquidity + 0.4 * s.flow_score),
                    reverse=True,
                )
                candidates = all_states[:TOP_OBI_CANDIDATES]

                tasks = []
                for st in candidates:
                    if st.exchange == "binance_fut":
                        tasks.append(fetch_binance_depth(client, st.symbol))
                    else:
                        tasks.append(fetch_bybit_depth(client, st.symbol))

                results = await asyncio.gather(*tasks, return_exceptions=True)

                for st, res in zip(candidates, results):
                    if isinstance(res, Exception):
                        continue
                    bids = res.get("bids", [])
                    asks = res.get("asks", [])
                    st.obi = compute_obi_from_depth(bids, asks, st.last_price)
                    recompute_violence_score(st)

            except Exception as e:
                console.print(f"[red]Orderbook refresh error:[/red] {e}")

            await asyncio.sleep(ORDERBOOK_REFRESH_SEC)

# ---------------------------------------------------------------------------
# WebSockets – Binance futures all-ticker
# ---------------------------------------------------------------------------

async def binance_ws_loop(states: StateMap) -> None:
    while True:
        try:
            console.print("[cyan]Connecting Binance futures WS (!ticker@arr)...[/cyan]")
            async with websockets.connect(
                BINANCE_FAPI_WS_ALL,
                ping_interval=20,
                ping_timeout=20,
            ) as ws:
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    if isinstance(msg, list):
                        tickers = msg
                    elif isinstance(msg, dict):
                        if "data" in msg and isinstance(msg["data"], list):
                            tickers = msg["data"]
                        else:
                            tickers = [msg]
                    else:
                        continue

                    for t in tickers:
                        symbol = t.get("s")
                        if not symbol:
                            continue

                        key: StateKey = ("binance_fut", symbol)
                        st = states.get(key)
                        if st is None:
                            continue

                        last_price = safe_float(t.get("c"), st.last_price or 0.0)
                        high = safe_float(t.get("h"), st._high24 or last_price)
                        low = safe_float(t.get("l"), st._low24 or last_price)
                        qv = safe_float(t.get("q"), st._qv24 or 0.0)

                        prev_qv = st._qv24
                        vol_delta = qv - prev_qv if prev_qv > 0 and qv >= prev_qv else 0.0

                        update_from_24h_ticker(st, last_price, high, low, qv)
                        event_time = int(t.get("E", int(time.time() * 1000)))
                        on_price_tick(st, last_price, event_time, vol_delta)
                        recompute_violence_score(st)

        except Exception as e:
            console.print(f"[red]Binance WS error:[/red] {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)

# ---------------------------------------------------------------------------
# WebSockets – Bybit v5 tickers.*
# ---------------------------------------------------------------------------

async def bybit_ws_loop(states: StateMap) -> None:
    sub_msg = {"op": "subscribe", "args": ["tickers.*"]}

    while True:
        try:
            console.print("[cyan]Connecting Bybit WS (tickers.*)...[/cyan]")
            async with websockets.connect(
                BYBIT_WS_LINEAR,
                ping_interval=20,
                ping_timeout=20,
            ) as ws:
                await ws.send(json.dumps(sub_msg))

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    topic = msg.get("topic", "")
                    if not topic.startswith("tickers."):
                        continue

                    ts = int(msg.get("ts", int(time.time() * 1000)))
                    data_list = msg.get("data", []) or []

                    for row in data_list:
                        symbol = row.get("symbol")
                        if not symbol:
                            continue

                        key: StateKey = ("bybit_lin", symbol)
                        st = states.get(key)
                        if st is None:
                            continue

                        last_price = safe_float(row.get("lastPrice"), st.last_price or 0.0)
                        high = safe_float(row.get("highPrice24h"), st._high24 or last_price)
                        low = safe_float(row.get("lowPrice24h"), st._low24 or last_price)
                        qv = safe_float(row.get("turnover24h"), st._qv24 or 0.0)
                        funding = safe_float(row.get("fundingRate"), st._funding_rate or 0.0)

                        prev_qv = st._qv24
                        vol_delta = qv - prev_qv if prev_qv > 0 and qv >= prev_qv else 0.0

                        update_from_24h_ticker(st, last_price, high, low, qv)
                        update_from_funding(st, funding)
                        on_price_tick(st, last_price, ts, vol_delta)
                        recompute_violence_score(st)

        except Exception as e:
            console.print(f"[red]Bybit WS error:[/red] {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)

# ---------------------------------------------------------------------------
# Regime + Coach
# ---------------------------------------------------------------------------

def compute_regime(states: StateMap) -> str:
    btc_state = None
    for (ex, sym), st in states.items():
        if ex == "binance_fut" and sym == "BTCUSDT":
            btc_state = st
            break
    if btc_state is None:
        return "?"

    if btc_state.flow_score >= 0.7 and btc_state.deriv_bias >= 0.6:
        return "Risk ON"
    if btc_state.flow_score <= 0.3 and btc_state.deriv_bias <= 0.4:
        return "Risk OFF"
    return "Choppy"


def coach_for_state(st: SymbolState, tier: str, regime: str) -> str:
    if st._high24 > 0 and st._low24 > 0:
        range_pct = (st._high24 - st._low24) / max(st.last_price, 1e-9) * 100.0
    else:
        range_pct = 0.0

    if tier == "A+" and st.vol_comp >= 0.7 and st.flow_score >= 0.7 and st.deriv_bias >= 0.6:
        return "🟢 Fresh coil + strong flows – prep breakout."
    if range_pct >= 35.0 and st.flow_score > 0.5:
        return "🔴 Already +35% 24h range – late chase risk."
    if st.flow_score >= 0.6 and st.deriv_bias >= 0.8:
        return "🟡 Flow strong but funding hot – size down."
    if regime == "Risk OFF" and tier in ("A+", "A"):
        return "🟡 Signal good but regime Risk OFF – halve size."
    if tier == "Radar":
        return "🟢 Coil building – stalk for trigger."
    return "⚪ Mixed signals – wait for cleaner alignment."

# ---------------------------------------------------------------------------
# Signal logging loops
# ---------------------------------------------------------------------------

def maybe_log_signals(states: StateMap) -> None:
    now = time.time()
    for key, st in states.items():
        tier, k, n = classify_tier(st)
        if tier == "-":
            continue

        score = st.violence_score
        if tier == "A+" and score < SIGNAL_A_PLUS_THRESHOLD:
            continue
        if tier == "A" and score < SIGNAL_A_THRESHOLD:
            continue
        if tier == "Radar" and score < SIGNAL_RADAR_THRESHOLD:
            continue

        last = _last_signal_logged.get(key, 0)
        if now - last < SIGNAL_MIN_INTERVAL_SEC:
            continue

        if st.last_price <= 0:
            continue

        log_signal(st, tier, now)
        _last_signal_logged[key] = now


async def signal_manager_loop(states: StateMap) -> None:
    while True:
        try:
            maybe_log_signals(states)
            update_open_signals(states)
        except Exception as e:
            console.print(f"[red]Signal manager error:[/red] {e}")
        await asyncio.sleep(60)

# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

def make_sniper_table(states: StateMap, regime: str) -> Table:
    tbl = Table(
        title="🎯 A+ / A Violent Breakout Sniper List",
        expand=True,
        box=box.SIMPLE_HEAVY,
    )
    tbl.add_column("#", justify="right", style="bold")
    tbl.add_column("Tier")
    tbl.add_column("Exch")
    tbl.add_column("Symbol")
    tbl.add_column("Price", justify="right")
    tbl.add_column("Viol", justify="right")
    tbl.add_column("Heat")
    tbl.add_column("Strong")
    tbl.add_column("Vol", justify="right")
    tbl.add_column("Flow", justify="right")
    tbl.add_column("Deriv", justify="right")
    tbl.add_column("OBI", justify="right")
    tbl.add_column("Liq", justify="right")
    tbl.add_column("Entry", justify="right")
    tbl.add_column("SL", justify="right")
    tbl.add_column("TP1", justify="right")
    tbl.add_column("TP2", justify="right")
    tbl.add_column("TP3", justify="right")
    tbl.add_column("RR3", justify="right")
    tbl.add_column("Coach")

    candidates = []
    for st in states.values():
        tier, k, n = classify_tier(st)
        if tier in ("A+", "A"):
            candidates.append((st, tier, k, n))

    candidates.sort(key=lambda x: x[0].violence_score, reverse=True)
    top = candidates[:10]

    for idx, (st, tier, k, n) in enumerate(top, start=1):
        score = st.violence_score

        if score >= 80 and tier == "A+":
            heat = "🔥🔥🔥"
            row_style = "bold red"
        elif score >= 60:
            heat = "🔥🔥"
            row_style = "bold yellow"
        else:
            heat = "🔥"
            row_style = ""

        coach = coach_for_state(st, tier, regime)
        levels = compute_trade_levels(st)
        ideal_entry = levels["ideal_entry"]
        sl_price = levels["sl_price"]
        tp1 = levels["tp1"]
        tp2 = levels["tp2"]
        tp3 = levels["tp3"]
        rr3 = levels["rr3"]

        tbl.add_row(
            str(idx),
            tier,
            "BIN" if st.exchange == "binance_fut" else "BYB",
            st.canonical,
            f"{st.last_price:.4g}" if st.last_price else "?",
            f"{score:5.1f}",
            heat,
            f"{k}/{n}",
            f"{st.vol_comp:4.2f}",
            f"{st.flow_score:4.2f}",
            f"{st.deriv_bias:4.2f}",
            f"{st.obi:4.2f}",
            f"{st.liquidity:4.2f}",
            f"{ideal_entry:.4g}" if ideal_entry else "-",
            f"{sl_price:.4g}" if sl_price else "-",
            f"{tp1:.4g}" if tp1 else "-",
            f"{tp2:.4g}" if tp2 else "-",
            f"{tp3:.4g}" if tp3 else "-",
            f"{rr3:4.2f}" if rr3 else "-",
            coach,
            style=row_style,
        )

    return tbl


def make_radar_table(states: StateMap, regime: str) -> Table:
    tbl = Table(
        title="👀 Radar – Early Coils & Build-ups",
        expand=True,
        box=box.SIMPLE_HEAVY,
    )
    tbl.add_column("#", justify="right", style="bold")
    tbl.add_column("Tier")
    tbl.add_column("Exch")
    tbl.add_column("Symbol")
    tbl.add_column("Price", justify="right")
    tbl.add_column("Viol", justify="right")
    tbl.add_column("Strong")
    tbl.add_column("Vol", justify="right")
    tbl.add_column("Flow", justify="right")
    tbl.add_column("Deriv", justify="right")
    tbl.add_column("OBI", justify="right")
    tbl.add_column("Liq", justify="right")
    tbl.add_column("Entry", justify="right")
    tbl.add_column("SL", justify="right")
    tbl.add_column("Coach")

    candidates = []
    for st in states.values():
        tier, k, n = classify_tier(st)
        if tier == "Radar":
            candidates.append((st, tier, k, n))

    candidates.sort(key=lambda x: x[0].violence_score, reverse=True)
    top = candidates[:10]

    for idx, (st, tier, k, n) in enumerate(top, start=1):
        coach = coach_for_state(st, tier, regime)
        levels = compute_trade_levels(st)
        ideal_entry = levels["ideal_entry"]
        sl_price = levels["sl_price"]

        tbl.add_row(
            str(idx),
            tier,
            "BIN" if st.exchange == "binance_fut" else "BYB",
            st.canonical,
            f"{st.last_price:.4g}" if st.last_price else "?",
            f"{st.violence_score:5.1f}",
            f"{k}/{n}",
            f"{st.vol_comp:4.2f}",
            f"{st.flow_score:4.2f}",
            f"{st.deriv_bias:4.2f}",
            f"{st.obi:4.2f}",
            f"{st.liquidity:4.2f}",
            f"{ideal_entry:.4g}" if ideal_entry else "-",
            f"{sl_price:.4g}" if sl_price else "-",
            coach,
        )

    return tbl


def make_stats_panel() -> Panel:
    stats = get_event_stats()
    t = RichTable(box=box.SIMPLE_HEAVY, expand=True, title="📊 Event Study (Closed Signals)")
    t.add_column("Tier")
    t.add_column("N closed", justify="right")
    t.add_column("P(+20% 24h)", justify="right")
    t.add_column("Avg Max Run-up %", justify="right")
    t.add_column("Avg Max Drawdown %", justify="right")

    for tier in ("A+", "A", "Radar"):
        n, p20, ru, dd = stats[tier]
        t.add_row(
            tier,
            str(n),
            f"{p20:5.1f}",
            f"{ru:5.1f}",
            f"{dd:5.1f}",
        )

    return Panel(t, title="Backtest Brain", border_style="cyan")


def make_root_renderable(states: StateMap) -> Panel:
    regime = compute_regime(states)
    sniper_tbl = make_sniper_table(states, regime)
    radar_tbl = make_radar_table(states, regime)
    stats_panel = make_stats_panel()

    top_panel = Panel(sniper_tbl, title=f"Top A+ / A  (Regime: {regime})", border_style="magenta")
    radar_panel = Panel(radar_tbl, title="Radar Watchlist", border_style="yellow")

    grid = RichTable.grid(expand=True)
    grid.add_row(top_panel)
    grid.add_row(radar_panel)
    grid.add_row(stats_panel)

    return grid


async def ui_loop(states: StateMap) -> None:
    with Live(make_root_renderable(states), refresh_per_second=1, console=console) as live:
        while True:
            await asyncio.sleep(TABLE_REFRESH_SEC)
            live.update(make_root_renderable(states))

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    states: StateMap = {}

    get_db()  # ensure DB ready

    async with httpx.AsyncClient(timeout=10.0) as client:
        await load_binance_universe(client, states)
        await load_bybit_universe(client, states)

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            await refresh_binance_funding(client, states)
            await refresh_bybit_funding(client, states)
    except Exception as e:
        console.print(f"[yellow]Initial funding snapshot failed:[/yellow] {e}")

    for st in states.values():
        st.obi = 0.5
        recompute_violence_score(st)

    tasks = [
        asyncio.create_task(binance_ws_loop(states)),
        asyncio.create_task(bybit_ws_loop(states)),
        asyncio.create_task(periodic_orderbook_refresh(states)),
        asyncio.create_task(periodic_funding_refresh(states)),
        asyncio.create_task(signal_manager_loop(states)),
        asyncio.create_task(ui_loop(states)),
    ]

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("[yellow]Shutting down…[/yellow]")
