import os
import asyncio
import logging
import datetime
import sqlite3
from collections import deque, defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any

# -----------------------------------------------------------------------------
# Fast event loop (uvloop) – optional but recommended
# -----------------------------------------------------------------------------
try:
    import uvloop  # type: ignore
    uvloop.install()
except Exception:
    pass

# -----------------------------------------------------------------------------
# Fast JSON (orjson) with fallback
# -----------------------------------------------------------------------------
try:
    import orjson  # type: ignore

    def json_loads(s: Any) -> Any:
        return orjson.loads(s)

except Exception:
    import json

    def json_loads(s: Any) -> Any:
        return json.loads(s)

import aiohttp
import websockets
import numpy as np
from rich.live import Live
from rich.table import Table
from rich.layout import Layout
from rich import box

# Optional parquet stack
try:
    import pandas as pd  # type: ignore

    HAVE_PANDAS = True
except Exception:
    HAVE_PANDAS = False

# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------
DB_NAME = "flow_trades.sqlite"

BINANCE_FUTURES_TICKER_WS = "wss://fstream.binance.com/stream?streams=!ticker@arr"
BINANCE_SPOT_MINI_WS = "wss://stream.binance.com:9443/ws/!miniTicker@arr"
BINANCE_FUTURES_DEPTH_REST = "https://fapi.binance.com/fapi/v1/depth"
BINANCE_FUTURES_EXCHANGE_INFO = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BINANCE_FUTURES_DEPTH_WS_TEMPLATE = "wss://fstream.binance.com/stream?streams={streams}"

BYBIT_LINEAR_WS = "wss://stream.bybit.com/v5/public/linear"

# depth/order-book config
TOP_N_DEPTH_SYMBOLS = 30   # build OB for top N futures symbols by vol
DEPTH_LIMIT = 50

MIN_NOTIONAL_USDT = 500_000
WARMUP_TICKS = 30

ANOMALY_HALF_LIFE_SEC = 2 * 3600
ANOMALY_MEMORY_WINDOW_SEC = 4 * 3600

PRICE_BIAS_EPS = 0.003
OI_DIR_EPS = 0.5
CVD_DIR_EPS = 0.5

MIN_CONF_FOR_TRADE = 55.0

STABLE_FILTER = ["USDT", "USDC", "FDUSD", "TUSD", "BUSD", "USDP"]

DEBUG_MODE = False

# tick archive
TICK_ARCHIVE_DIR = "ticks"
os.makedirs(TICK_ARCHIVE_DIR, exist_ok=True)
TICK_ARCHIVE_FLUSH_INTERVAL = 60.0   # seconds

# Telegram alerts (optional)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# -----------------------------------------------------------------------------
# DATA MODELS
# -----------------------------------------------------------------------------

@dataclass
class MarketState:
    symbol: str
    exchange: str

    price_history: deque = field(default_factory=lambda: deque(maxlen=200))
    vol_history: deque = field(default_factory=lambda: deque(maxlen=200))
    oi_history: deque = field(default_factory=lambda: deque(maxlen=200))

    current_price: float = 0.0
    current_volume: float = 0.0
    current_oi: float = 0.0
    current_funding: float = 0.0
    vwap: float = 0.0

    oi_change_pct: float = 0.0
    cvd_proxy: float = 0.0
    cvd_true: float = 0.0
    cvd_history: deque = field(default_factory=lambda: deque(maxlen=200))

    anomaly_events: deque = field(default_factory=lambda: deque(maxlen=500))

    summary_text: str = ""
    trade_signal: Optional[str] = None
    signal_quality: str = ""
    sl: float = 0.0
    tp1: float = 0.0
    tp2: float = 0.0
    tp3: float = 0.0
    last_signal_time: Optional[datetime.datetime] = None

    active_archetypes: List[str] = field(default_factory=list)

    bias: str = ""
    bias_score: float = 0.0
    confidence: float = 0.0

    def update(
        self,
        price: Optional[float],
        vol: Optional[float],
        oi: Optional[float] = None,
        funding: Optional[float] = None,
    ) -> None:
        # 1) price & volume
        if price is not None and price > 0:
            price_delta = price - self.current_price if self.current_price > 0 else 0.0
            vol_delta = (
                vol - self.current_volume
                if (vol is not None and self.current_volume > 0)
                else 0.0
            )
            if vol_delta < 0:
                vol_delta = 0.0
            if vol is not None:
                if price_delta > 0:
                    direction = 1
                elif price_delta < 0:
                    direction = -1
                else:
                    direction = 0
                self.cvd_proxy += direction * vol_delta

            self.current_price = price
            if vol is not None:
                self.current_volume = vol

            self.price_history.append(price)
            if vol is not None:
                self.vol_history.append(vol)

            if self.price_history:
                self.vwap = float(np.mean(self.price_history))

        # 2) OI
        if oi is not None and oi > 0:
            self.current_oi = oi
            self.oi_history.append(oi)
            if len(self.oi_history) > 10 and self.oi_history[-10] > 0:
                self.oi_change_pct = (
                    (self.current_oi - self.oi_history[-10]) / self.oi_history[-10]
                ) * 100.0

        if funding is not None:
            self.current_funding = funding

        if self.current_price > 0:
            update_trade_status(self.symbol, self.current_price)
            self.generate_summary()

    def record_anomaly(self, kind: str, score: float) -> None:
        try:
            s = float(score)
        except (TypeError, ValueError):
            return
        now = datetime.datetime.utcnow()
        self.anomaly_events.append((now, kind, s))

    def anomaly_memory_score(self, now: Optional[datetime.datetime] = None) -> float:
        if now is None:
            now = datetime.datetime.utcnow()
        cutoff = now - datetime.timedelta(seconds=ANOMALY_MEMORY_WINDOW_SEC)
        while self.anomaly_events and self.anomaly_events[0][0] < cutoff:
            self.anomaly_events.popleft()
        total = 0.0
        for ts, kind, score in self.anomaly_events:
            dt = (now - ts).total_seconds()
            decay = 2.0 ** (-dt / ANOMALY_HALF_LIFE_SEC)
            total += score * decay
        return total

    def generate_summary(self) -> None:
        txt = "[dim]😴 Sleeping...[/dim]"
        is_pump = "shock_up" in self.active_archetypes
        is_breakout = "zec" in self.active_archetypes
        is_squeeze = "mel" in self.active_archetypes
        is_dump = "capitulation" in self.active_archetypes
        has_whales = "oi_build" in self.active_archetypes

        if has_whales and is_pump:
            txt = "[bold magenta]🐋 WHALES APING[/bold magenta]"
        elif has_whales and is_squeeze:
            txt = "[bold green]🔫 LOADING GUN[/bold green]"
        elif is_pump and not has_whales:
            txt = "[yellow]🤡 RETAIL FOMO[/yellow]"
        elif is_dump and has_whales:
            txt = "[bold red]🪤 SHORT TRAP[/bold red]"
        elif is_dump:
            txt = "[red]🩸 PANIC DUMP[/red]"
        elif is_breakout:
            txt = "[green]🚀 MOONING[/green]"
        elif is_squeeze:
            txt = "[blue]🤐 QUIET[/blue]"
        self.summary_text = txt


@dataclass
class OrderBookState:
    symbol: str
    bids: Dict[float, float] = field(default_factory=dict)
    asks: Dict[float, float] = field(default_factory=dict)
    last_update_id: int = 0
    initialized: bool = False


@dataclass
class FeedMetrics:
    name: str
    messages: int = 0
    last_messages: int = 0
    last_message_ts: float = 0.0
    sequence_gaps: int = 0
    dead_alert_sent: bool = False


# -----------------------------------------------------------------------------
# GLOBAL STATE
# -----------------------------------------------------------------------------
market_data: Dict[str, MarketState] = {}
order_books: Dict[str, OrderBookState] = {}
feed_metrics: Dict[str, FeedMetrics] = {}
market_event_queue: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue(maxsize=20000)

# tick archive buffers
tick_archive_buffers: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

# -----------------------------------------------------------------------------
# DB LAYER
# -----------------------------------------------------------------------------
def init_db() -> None:
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS trade_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            time TEXT,
            symbol TEXT,
            confidence REAL,
            signal TEXT,
            entry REAL,
            stop_loss REAL,
            tp1 REAL,
            tp2 REAL,
            tp3 REAL,
            result TEXT
        )
        """
    )
    conn.commit()
    conn.close()


def log_trade_to_db(
    symbol: str,
    signal: str,
    entry: float,
    sl: float,
    tp1: float,
    tp2: float,
    tp3: float,
    result: str = "OPEN",
    confidence: float = 0.0,
) -> None:
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO trade_history (time, symbol, confidence, signal, entry, stop_loss, tp1, tp2, tp3, result)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            datetime.datetime.utcnow().isoformat(),
            symbol,
            confidence,
            signal,
            entry,
            sl,
            tp1,
            tp2,
            tp3,
            result,
        ),
    )
    conn.commit()
    conn.close()


def fetch_recent_trades(limit: int = 10) -> List[Tuple]:
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT time, symbol, confidence, signal, entry, stop_loss, tp2, result
        FROM trade_history
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def update_trade_status(symbol: str, current_price: float) -> None:
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, signal, stop_loss, tp2
        FROM trade_history
        WHERE symbol = ? AND result = 'OPEN'
        """,
        (symbol,),
    )
    for trade_id, signal, sl, tp2 in cur.fetchall():
        new_result = None
        if signal == "LONG":
            if current_price >= tp2:
                new_result = "WIN"
            elif current_price <= sl:
                new_result = "LOSS"
        elif signal == "SHORT":
            if current_price <= tp2:
                new_result = "WIN"
            elif current_price >= sl:
                new_result = "LOSS"
        if new_result:
            cur.execute(
                "UPDATE trade_history SET result=? WHERE id=?",
                (new_result, trade_id),
            )
    conn.commit()
    conn.close()


# -----------------------------------------------------------------------------
# TELEGRAM ALERTS
# -----------------------------------------------------------------------------
async def send_telegram_alert(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=payload, timeout=10) as resp:
                _ = await resp.text()
    except Exception as e:
        logging.warning(f"Telegram alert failed: {e}")


# -----------------------------------------------------------------------------
# ANOMALY / BIAS / EXITS
# -----------------------------------------------------------------------------
def robust_zscore(values: List[float], current: float, min_len: int = 20) -> float:
    clean = [float(v) for v in values if v is not None]
    if len(clean) < min_len:
        return 0.0
    arr = np.array(clean, dtype=float)
    med = float(np.median(arr))
    mad = float(np.median(np.abs(arr - med)))
    if mad < 1e-9:
        return 0.0
    return float((float(current) - med) / (1.4826 * mad))


def _dir_from_val(val: float, eps: float) -> int:
    if val > eps:
        return 1
    if val < -eps:
        return -1
    return 0


def compute_bias(
    prices: List[float],
    oi_vals: List[float],
    cvd_vals: List[float],
    price_z: float,
    oi_z: float,
    cvd_z: float,
) -> Tuple[str, float, Tuple[int, int, int]]:
    if not prices:
        return ("Neutral", 0.0, (0, 0, 0))
    arr_p = np.array(prices, dtype=float)
    med_p = float(np.median(arr_p))
    curr_p = float(arr_p[-1])
    price_change = (curr_p - med_p) / med_p if med_p > 0 else 0.0
    price_dir = _dir_from_val(price_change, PRICE_BIAS_EPS)
    oi_dir = _dir_from_val(oi_z, OI_DIR_EPS)
    cvd_dir = _dir_from_val(cvd_z, CVD_DIR_EPS)
    triple = (price_dir, oi_dir, cvd_dir)

    if triple == (1, 1, 1):
        return ("Strong Bullish (trend build)", 80.0, triple)
    if triple == (1, 1, -1):
        return ("Cautious Bullish (CVD lagging)", 40.0, triple)
    if triple == (1, -1, 1):
        return ("Short covering coil", 35.0, triple)
    if triple == (1, -1, -1):
        return ("Distribution up", -20.0, triple)
    if triple == (-1, 1, -1):
        return ("Strong Bearish (trend build)", -80.0, triple)
    if triple == (-1, 1, 1):
        return ("Bear Trap Risk", -30.0, triple)
    if triple == (-1, -1, -1):
        return ("Bleed / Trend Down", -60.0, triple)
    if triple == (-1, -1, 1):
        return ("Capitulation + dip buying", -10.0, triple)
    if triple == (0, 1, 1):
        return ("Bullish build (sideways price)", 50.0, triple)
    if triple == (0, 1, -1):
        return ("Leverage build, CVD down", -10.0, triple)
    if triple == (0, -1, 1):
        return ("Short covering coil", 20.0, triple)
    if triple == (0, -1, -1):
        return ("De-leveraging bleed", -30.0, triple)
    return ("Neutral", 0.0, triple)


def quality_to_base_conf(quality: str) -> float:
    if not quality:
        return 0.0
    q = quality.upper()
    if q.startswith("S+"):
        return 40.0
    if q.startswith("S"):
        return 35.0
    if q.startswith("A+"):
        return 32.0
    if q.startswith("A"):
        return 28.0
    if q.startswith("B"):
        return 22.0
    if q.startswith("C"):
        return 10.0
    return 5.0


def compute_confidence(
    signal_type: Optional[str],
    quality: str,
    detected_tags: List[str],
    anomaly_memory: float,
    bias_score: float,
) -> float:
    if not signal_type:
        return 0.0
    conf = quality_to_base_conf(quality)
    if "oi_build" in detected_tags:
        conf += 15.0
    if "cvd_kick" in detected_tags:
        conf += 15.0
    if "vol_anom" in detected_tags:
        conf += 10.0
    memory_boost = max(0.0, min(anomaly_memory, 30.0))
    conf += memory_boost

    stype = signal_type.upper()
    direction = 0
    if "LONG" in stype:
        direction = 1
    elif "SHORT" in stype:
        direction = -1
    if direction != 0:
        align_raw = bias_score * direction / 3.0
        align = max(-20.0, min(20.0, align_raw))
        conf += align
    return max(0.0, min(100.0, conf))


def build_volume_profile(
    prices: List[float], vols: List[float], bins: int = 24
) -> Optional[Tuple[np.ndarray, np.ndarray]]:
    if len(prices) < 5 or len(vols) < 5:
        return None
    arr_p = np.array(prices, dtype=float)
    arr_v = np.array(vols, dtype=float)
    if np.any(np.isnan(arr_p)) or np.any(np.isnan(arr_v)):
        return None
    diff_v = np.diff(arr_v, prepend=arr_v[0])
    diff_v = np.where(diff_v < 0, 0.0, diff_v)
    p_min, p_max = float(np.min(arr_p)), float(np.max(arr_p))
    if p_max <= p_min:
        return None
    bin_edges = np.linspace(p_min, p_max, bins + 1)
    bin_vol = np.zeros(bins, dtype=float)
    for p, dv in zip(arr_p, diff_v):
        if dv <= 0:
            continue
        idx = np.searchsorted(bin_edges, p, side="right") - 1
        if 0 <= idx < bins:
            bin_vol[idx] += dv
    if np.all(bin_vol <= 0):
        return None
    centers = (bin_edges[:-1] + bin_edges[1:]) / 2.0
    return centers, bin_vol


def hvn_lvn_levels(
    centers: np.ndarray, bin_vol: np.ndarray
) -> Tuple[float, List[float], List[float]]:
    poc_idx = int(np.argmax(bin_vol))
    poc = float(centers[poc_idx])
    positive_vol = bin_vol[bin_vol > 0]
    if len(positive_vol) == 0:
        return poc, [], []
    median_vol = float(np.median(positive_vol))
    if median_vol <= 0:
        return poc, [], []
    hvn_idx = np.where(bin_vol >= median_vol * 1.5)[0]
    lvn_idx = np.where(bin_vol <= median_vol * 0.5)[0]
    hvns = [float(centers[i]) for i in hvn_idx]
    lvns = [float(centers[i]) for i in lvn_idx]
    return poc, hvns, lvns


def find_nearest_levels(
    current_price: float, levels: List[float]
) -> Tuple[Optional[float], Optional[float]]:
    below = [lv for lv in levels if lv < current_price]
    above = [lv for lv in levels if lv > current_price]
    nearest_below = max(below) if below else None
    nearest_above = min(above) if above else None
    return nearest_below, nearest_above


def compute_dynamic_exits(
    current_price: float,
    prices: List[float],
    vols: List[float],
    direction: str,
    weakness_score: float,
) -> Tuple[float, float, float, float]:
    arr = np.array(prices[-50:], dtype=float)
    if len(arr) < 5:
        atr = max(current_price * 0.002, 0.01)
    else:
        diffs = np.abs(np.diff(arr))
        atr = max(float(np.mean(diffs)), current_price * 0.002)

    base_dist = atr * 2.5
    factor = 1.0 + (weakness_score / 100.0)
    dist = base_dist * factor
    direction_u = direction.upper()

    if direction_u == "LONG":
        sl = current_price - dist
        tp1 = current_price + dist * 0.6
        tp2 = current_price + dist * 1.2
        tp3 = current_price + dist * 1.8
    else:
        sl = current_price + dist
        tp1 = current_price - dist * 0.6
        tp2 = current_price - dist * 1.2
        tp3 = current_price - dist * 1.8

    profile = build_volume_profile(prices, vols, bins=24)
    if profile is None:
        return sl, tp1, tp2, tp3

    centers, bin_vol = profile
    poc, hvns, lvns = hvn_lvn_levels(centers, bin_vol)
    hvn_below, hvn_above = find_nearest_levels(current_price, hvns)
    lvn_below, lvn_above = find_nearest_levels(current_price, lvns)

    if direction_u == "LONG":
        if lvn_below is not None:
            sl = min(sl, lvn_below)
        if hvn_above is not None:
            tp2 = max(tp2, hvn_above)
            extra = (hvn_above - current_price) * 0.5
            tp3 = max(tp3, hvn_above + max(extra, 0))
    elif direction_u == "SHORT":
        if lvn_above is not None:
            sl = max(sl, lvn_above)
        if hvn_below is not None:
            tp2 = min(tp2, hvn_below)
            extra = (current_price - hvn_below) * 0.5
            tp3 = min(tp3, hvn_below - max(extra, 0))
    return sl, tp1, tp2, tp3


# -----------------------------------------------------------------------------
# ANALYSIS ENGINE
# -----------------------------------------------------------------------------
async def analyze_all_loop() -> None:
    while True:
        for st in list(market_data.values()):
            analyze(st)
        await asyncio.sleep(1.0)


def analyze(state: MarketState) -> None:
    detected: List[str] = []
    quality: str = ""

    prices = list(state.price_history)
    vols = list(state.vol_history)
    ois = list(state.oi_history)
    cvds = list(state.cvd_history)

    if DEBUG_MODE and len(prices) > 2 and state.current_price > 0:
        state.trade_signal = "LONG"
        state.signal_quality = "DEBUG"
        state.sl, state.tp1, state.tp2, state.tp3 = compute_dynamic_exits(
            state.current_price, prices, vols, "LONG", 60.0
        )
        return

    if len(prices) < WARMUP_TICKS:
        return

    curr_p = prices[-1]
    vol_z = robust_zscore(vols, state.current_volume) if vols else 0.0
    oi_z = robust_zscore(ois, state.current_oi) if ois else 0.0
    cvd_current = cvds[-1] if cvds else state.cvd_true
    cvd_z = robust_zscore(cvds, cvd_current) if cvds else 0.0
    now_utc = datetime.datetime.utcnow()

    if vol_z >= 2.5:
        detected.append("vol_anom")
        state.record_anomaly("VOL", vol_z)
    if oi_z >= 3.0:
        detected.append("oi_build")
        state.record_anomaly("OI", oi_z)
    if cvd_z >= 2.5:
        detected.append("cvd_kick")
        state.record_anomaly("CVD", cvd_z)

    anomaly_memory = state.anomaly_memory_score(now_utc)

    bias_label, bias_score, triple = compute_bias(
        prices, ois, cvds, price_z=0.0, oi_z=oi_z, cvd_z=cvd_z
    )
    state.bias = bias_label
    state.bias_score = bias_score

    arr_p = np.array(prices, dtype=float)
    max_past = float(np.max(arr_p[:-1])) if len(arr_p) > 1 else curr_p
    is_breakout = curr_p >= max_past

    if is_breakout:
        detected.append("zec")
    std_p = float(np.std(arr_p))
    mean_p = float(np.mean(arr_p)) if len(arr_p) > 0 else curr_p
    if std_p > 0 and (curr_p - mean_p) > 4 * std_p:
        detected.append("shock_up")
    if std_p > 0 and (mean_p - curr_p) > 4 * std_p:
        detected.append("capitulation")

    state.active_archetypes = detected.copy()

    signal_type: Optional[str] = None
    weakness_score = 50.0
    if "shock_up" in detected and "zec" in detected and state.current_price > state.vwap:
        signal_type = "LONG"
        quality = "S+ (Whale Breakout)" if "oi_build" in detected else "A+ (Pump)"
        weakness_score = 70.0 if "oi_build" in detected else 60.0
    elif "capitulation" in detected and state.current_price < state.vwap * 0.97:
        signal_type = "LONG"
        quality = "B (Knife Catch)"
        weakness_score = 50.0
    elif "zec" in detected and "oi_build" in detected:
        signal_type = "LONG"
        quality = "S (Trend Breakout)"
        weakness_score = 65.0

    symbol_up = state.symbol.upper()
    if any(stable in symbol_up for stable in STABLE_FILTER):
        signal_type = None
        quality = ""

    has_flow_anomaly = any(
        tag in detected for tag in ["vol_anom", "oi_build", "cvd_kick"]
    )

    conf = compute_confidence(
        signal_type, quality, detected, anomaly_memory, bias_score
    )
    state.confidence = conf

    trade_dir = 0
    if signal_type:
        if "LONG" in signal_type.upper():
            trade_dir = 1
        elif "SHORT" in signal_type.upper():
            trade_dir = -1

    bias_ok = False
    if trade_dir == 1 and bias_score > 10:
        bias_ok = True
    if trade_dir == -1 and bias_score < -10:
        bias_ok = True
    if trade_dir == 0:
        bias_ok = True

    if (
        signal_type
        and conf >= MIN_CONF_FOR_TRADE
        and has_flow_anomaly
        and bias_ok
    ):
        now = datetime.datetime.utcnow()
        if (not state.last_signal_time) or (
            now - state.last_signal_time
        ).total_seconds() > 60:
            state.sl, state.tp1, state.tp2, state.tp3 = compute_dynamic_exits(
                state.current_price, prices, vols, signal_type, weakness_score
            )
            state.trade_signal = signal_type
            state.signal_quality = quality
            state.last_signal_time = now
            log_trade_to_db(
                state.symbol,
                signal_type,
                state.current_price,
                state.sl,
                state.tp1,
                state.tp2,
                state.tp3,
                "OPEN",
                confidence=conf,
            )
    else:
        if signal_type:
            state.trade_signal = None
            state.signal_quality = f"Candidate ({quality or 'N/A'}, conf={conf:.0f})"
        else:
            state.trade_signal = None
            if not state.signal_quality:
                state.signal_quality = ""


# -----------------------------------------------------------------------------
# UI HELPERS & TABLES
# -----------------------------------------------------------------------------
def format_bias_cell(bias_label: str, bias_score: float) -> str:
    if not bias_label:
        bias_label = "Neutral"
    short = bias_label
    if len(short) > 22:
        short = short[:19] + "..."
    if bias_score > 40:
        return f"[bold green]{short}[/]"
    if bias_score > 10:
        return f"[green]{short}[/]"
    if bias_score < -40:
        return f"[bold red]{short}[/]"
    if bias_score < -10:
        return f"[red]{short}[/]"
    return f"[dim]{short}[/]"


def format_conf_cell(conf_val: float) -> str:
    if conf_val >= 80:
        return f"[bold green]{conf_val:3.0f} 🔥[/]"
    if conf_val >= 60:
        return f"[green]{conf_val:3.0f} ✅[/]"
    if conf_val >= 40:
        return f"[yellow]{conf_val:3.0f} ⚠️[/]"
    if conf_val > 0:
        return f"[dim]{conf_val:3.0f} 💤[/]"
    return f"[dim]{conf_val:3.0f} ?[/]"


def generate_scanner_table() -> Table:
    title = f"📡 OMNI-SCANNER (Tracking {len(market_data)} Pairs)"
    table = Table(
        expand=True,
        title=title,
        show_lines=True,
        box=box.SIMPLE_HEAD,
        border_style="dim",
    )
    table.add_column("Sym", style="bold")
    table.add_column("Price")
    table.add_column("Vol (24h)")
    table.add_column("OI Δ")
    table.add_column("CVD")
    table.add_column("Bias")
    table.add_column("Conf")
    table.add_column("Vibe Summary")

    states = sorted(market_data.values(), key=lambda s: s.current_volume, reverse=True)
    for st in states[:80]:
        sym = st.symbol.replace("USDT", "")
        price_str = f"{st.current_price:,.4f}" if st.current_price > 0 else "-"

        vol_m = st.current_volume / 1_000_000
        if vol_m >= 5000:
            vol_str = f"[bold magenta]{vol_m:.1f}M[/]"
        elif vol_m >= 1000:
            vol_str = f"[bold green]{vol_m:.1f}M[/]"
        elif vol_m >= 200:
            vol_str = f"[green]{vol_m:.1f}M[/]"
        elif vol_m > 0:
            vol_str = f"[dim]{vol_m:.1f}M[/]"
        else:
            vol_str = "[dim]0.0M[/dim]"

        oi_val = st.oi_change_pct
        if oi_val > 0.5:
            oi_str = f"[green]+{oi_val:.2f}%[/]"
        elif oi_val < -0.5:
            oi_str = f"[red]{oi_val:.2f}%[/]"
        else:
            oi_str = f"[dim]{oi_val:+.2f}%[/dim]"

        if st.cvd_history:
            cvd_val = st.cvd_history[-1] / 1000.0
        else:
            cvd_val = st.cvd_proxy / 1000.0
        if cvd_val > 0:
            cvd_str = f"[green]+{cvd_val:.0f}K[/]"
        elif cvd_val < 0:
            cvd_str = f"[red]{cvd_val:.0f}K[/]"
        else:
            cvd_str = "[dim]-[/dim]"

        bias_str = format_bias_cell(st.bias, st.bias_score)
        conf_str = format_conf_cell(st.confidence)
        vibe = st.summary_text or "[dim]😴 Sleeping...[/dim]"
        table.add_row(sym, price_str, vol_str, oi_str, cvd_str, bias_str, conf_str, vibe)
    return table


def generate_active_trades_table() -> Table:
    table = Table(
        expand=True,
        title="⚡ ACTIVE TRADE SIGNALS (CONF ≥ 55)",
        show_lines=True,
        box=box.SIMPLE_HEAD,
        border_style="dim",
    )
    table.add_column("Symbol")
    table.add_column("Bias")
    table.add_column("Signal")
    table.add_column("Entry")
    table.add_column("🛡️ SL")
    table.add_column("TP1")
    table.add_column("TP2")
    table.add_column("TP3")

    active_rows = [
        st
        for st in market_data.values()
        if st.trade_signal and st.confidence >= MIN_CONF_FOR_TRADE
    ]
    if not active_rows:
        table.add_row("-", "-", "Scanning...", "-", "-", "-", "-", "-")
        return table

    for st in active_rows:
        bias_str = format_bias_cell(st.bias, st.bias_score)
        sig = st.trade_signal.upper()
        if "LONG" in sig:
            sig_str = "[green]LONG[/]"
        elif "SHORT" in sig:
            sig_str = "[red]SHORT[/]"
        else:
            sig_str = sig
        table.add_row(
            st.symbol,
            bias_str,
            sig_str,
            f"{st.current_price:.4f}",
            f"{st.sl:.4f}",
            f"{st.tp1:.4f}",
            f"{st.tp2:.4f}",
            f"{st.tp3:.4f}",
        )
    return table


def generate_history_table() -> Table:
    table = Table(
        expand=True,
        title="📜 EXECUTED HISTORY & VERDICT",
        show_lines=True,
        box=box.SIMPLE_HEAD,
        border_style="dim",
    )
    table.add_column("Time")
    table.add_column("Symbol")
    table.add_column("Conf")
    table.add_column("Signal")
    table.add_column("Entry")
    table.add_column("SL")
    table.add_column("TP2")
    table.add_column("VERDICT")

    rows = fetch_recent_trades(limit=12)
    if not rows:
        table.add_row("-", "-", "-", "-", "-", "-", "-", "No history yet")
        return table

    for t, sym, conf, sig, entry, sl, tp2, result in rows:
        time_str = t.split("T")[-1][:8]
        conf_val = conf if conf is not None else 0.0
        conf_str = format_conf_cell(conf_val)
        sig_u = (sig or "").upper()
        if "LONG" in sig_u:
            sig_str = "[green]LONG[/]"
        elif "SHORT" in sig_u:
            sig_str = "[red]SHORT[/]"
        else:
            sig_str = sig or "-"
        if result == "OPEN":
            res_str = "[cyan]⏳ OPEN[/cyan]"
        elif result == "WIN":
            res_str = "[bold green]✅ WIN[/bold green]"
        elif result == "LOSS":
            res_str = "[bold red]❌ LOSS[/bold red]"
        else:
            res_str = f"[dim]{result}[/dim]"
        table.add_row(
            time_str,
            sym,
            conf_str,
            sig_str,
            f"{entry:.4f}",
            f"{sl:.4f}",
            f"{tp2:.4f}",
            res_str,
        )
    return table


def generate_dashboard() -> Layout:
    layout = Layout()
    layout.split(
        Layout(name="top", ratio=3),
        Layout(name="bottom", ratio=2),
    )
    layout["top"].update(generate_scanner_table())
    bottom = Layout()
    bottom.split_row(
        Layout(generate_active_trades_table(), name="active"),
        Layout(generate_history_table(), name="history"),
    )
    layout["bottom"].update(bottom)
    return layout


# -----------------------------------------------------------------------------
# SYMBOL DISCOVERY & ORDER-BOOK SNAPSHOT
# -----------------------------------------------------------------------------
async def fetch_binance_futures_universe() -> List[str]:
    async with aiohttp.ClientSession() as session:
        async with session.get(BINANCE_FUTURES_EXCHANGE_INFO, timeout=10) as resp:
            data = await resp.json()
    symbols = []
    for s in data.get("symbols", []):
        if s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING":
            symbols.append(s["symbol"])
    return symbols


async def fetch_depth_snapshot(symbol: str, limit: int = DEPTH_LIMIT) -> OrderBookState:
    params = {"symbol": symbol, "limit": limit}
    async with aiohttp.ClientSession() as session:
        async with session.get(BINANCE_FUTURES_DEPTH_REST, params=params, timeout=10) as resp:
            data = await resp.json()
    ob = OrderBookState(symbol=symbol)
    ob.last_update_id = int(data["lastUpdateId"])
    for p, q in data.get("bids", []):
        price = float(p)
        qty = float(q)
        if qty > 0:
            ob.bids[price] = qty
    for p, q in data.get("asks", []):
        price = float(p)
        qty = float(q)
        if qty > 0:
            ob.asks[price] = qty
    ob.initialized = True
    return ob


def apply_depth_update(
    ob: OrderBookState,
    first_update_id: int,
    last_update_id: int,
    bids: List[List[str]],
    asks: List[List[str]],
    feed_name: str,
) -> None:
    fm = feed_metrics.setdefault(feed_name, FeedMetrics(name=feed_name))
    # sequence logic from Binance docs
    if not ob.initialized:
        return
    if last_update_id <= ob.last_update_id:
        return
    if first_update_id > ob.last_update_id + 1:
        # gap detected
        fm.sequence_gaps += 1
        ob.initialized = False
        logging.warning(f"Sequence gap for {ob.symbol} – marking OB as uninitialized")
        return
    # apply deltas
    for p, q in bids:
        price = float(p)
        qty = float(q)
        if qty == 0:
            ob.bids.pop(price, None)
        else:
            ob.bids[price] = qty
    for p, q in asks:
        price = float(p)
        qty = float(q)
        if qty == 0:
            ob.asks.pop(price, None)
        else:
            ob.asks[price] = qty
    ob.last_update_id = last_update_id


# -----------------------------------------------------------------------------
# QUEUED EVENT PIPELINE
# -----------------------------------------------------------------------------
async def market_data_worker(worker_id: int) -> None:
    while True:
        event = await market_event_queue.get()
        try:
            etype = event.get("type")
            if etype == "binance_fut_ticker":
                sym = event["symbol"]
                price = event["price"]
                vol = event["volume"]
                st = market_data.setdefault(
                    sym, MarketState(symbol=sym, exchange="BINANCE-FUT")
                )
                st.update(price, vol, None, None)
                # tick archive
                tick_archive_buffers[sym].append(
                    {
                        "ts": event["ts"],
                        "symbol": sym,
                        "price": price,
                        "volume_quote_24h": vol,
                    }
                )
            elif etype == "binance_spot_miniticker":
                sym = event["symbol"]
                price = event["price"]
                vol = event["volume"]
                st = market_data.setdefault(
                    sym, MarketState(symbol=sym, exchange="BINANCE-SPOT")
                )
                st.update(price, vol, None, None)
            elif etype == "bybit_ticker":
                sym = event["symbol"]
                oi_val = event["oi"]
                funding = event["funding"]
                st = market_data.get(sym)
                if st:
                    st.update(None, None, oi=oi_val, funding=funding)
            elif etype == "bybit_trade":
                sym = event["symbol"]
                side = event["side"]
                qty = event["qty"]
                st = market_data.get(sym)
                if st:
                    if side == "Buy":
                        st.cvd_true += qty
                    elif side == "Sell":
                        st.cvd_true -= qty
                    st.cvd_history.append(st.cvd_true)
            elif etype == "binance_depth":
                sym = event["symbol"]
                ob = order_books.get(sym)
                if ob is None:
                    return
                apply_depth_update(
                    ob,
                    event["U"],
                    event["u"],
                    event["bids"],
                    event["asks"],
                    feed_name="binance_depth",
                )
        except Exception as e:
            logging.error(f"Worker {worker_id} error handling event {event.get('type')}: {e}")
        finally:
            market_event_queue.task_done()


# -----------------------------------------------------------------------------
# WS CONSUMERS (PRODUCERS)
# -----------------------------------------------------------------------------
async def binance_futures_ticker_consumer():
    fname = "binance_futures_ticker"
    feed_metrics[fname] = FeedMetrics(name=fname)
    async for ws in websockets.connect(BINANCE_FUTURES_TICKER_WS, ping_interval=20):
        try:
            async for msg in ws:
                data = json_loads(msg)
                payloads = data.get("data", [])
                if isinstance(payloads, dict):
                    payloads = [payloads]
                fm = feed_metrics[fname]
                fm.messages += 1
                fm.last_message_ts = asyncio.get_event_loop().time()
                for p in payloads:
                    s = p.get("s")
                    if not s:
                        continue
                    price = float(p.get("c", 0.0) or 0.0)
                    vol_quote = float(p.get("q", 0.0) or 0.0)
                    evt = {
                        "type": "binance_fut_ticker",
                        "symbol": s,
                        "price": price,
                        "volume": vol_quote,
                        "ts": datetime.datetime.utcnow().isoformat(),
                    }
                    await market_event_queue.put(evt)
        except websockets.ConnectionClosed:
            logging.warning("Binance futures ticker WS closed, reconnecting...")
            await asyncio.sleep(5)
            continue
        except Exception as e:
            logging.error(f"Binance futures ticker WS error: {e}")
            await asyncio.sleep(5)


async def binance_spot_miniticker_consumer():
    fname = "binance_spot_miniticker"
    feed_metrics[fname] = FeedMetrics(name=fname)
    async for ws in websockets.connect(BINANCE_SPOT_MINI_WS, ping_interval=20):
        try:
            async for msg in ws:
                data = json_loads(msg)
                arr = data if isinstance(data, list) else data.get("data") or []
                fm = feed_metrics[fname]
                fm.messages += 1
                fm.last_message_ts = asyncio.get_event_loop().time()
                for p in arr:
                    symbol = p.get("s")
                    if not symbol or not symbol.endswith("USDT"):
                        continue
                    price = float(p.get("c", 0.0) or 0.0)
                    vol_quote = float(p.get("q", 0.0) or 0.0)
                    evt = {
                        "type": "binance_spot_miniticker",
                        "symbol": symbol,
                        "price": price,
                        "volume": vol_quote,
                    }
                    await market_event_queue.put(evt)
        except websockets.ConnectionClosed:
            logging.warning("Binance spot WS closed, reconnecting...")
            await asyncio.sleep(5)
            continue
        except Exception as e:
            logging.error(f"Binance spot WS error: {e}")
            await asyncio.sleep(5)


async def bybit_ticker_consumer():
    fname = "bybit_ticker"
    feed_metrics[fname] = FeedMetrics(name=fname)
    # we subscribe to tickers for all known symbols
    while not market_data:
        await asyncio.sleep(2)
    symbols = list(market_data.keys())
    chunks = [symbols[i:i + 50] for i in range(0, len(symbols), 50)]
    async for ws in websockets.connect(BYBIT_LINEAR_WS, ping_interval=20):
        try:
            for chunk in chunks:
                args = [f"tickers.{sym}" for sym in chunk]
                await ws.send(orjson.dumps({"op": "subscribe", "args": args}))
                await asyncio.sleep(0.1)
            async for msg in ws:
                data = json_loads(msg)
                topic = data.get("topic", "")
                if not topic.startswith("tickers."):
                    continue
                fm = feed_metrics[fname]
                fm.messages += 1
                fm.last_message_ts = asyncio.get_event_loop().time()
                payload = data.get("data")
                items = payload if isinstance(payload, list) else [payload]
                for item in items:
                    sym = item.get("symbol")
                    if not sym:
                        continue
                    oi_val = float(item.get("openInterest", 0.0) or 0.0)
                    funding = float(item.get("fundingRate", 0.0) or 0.0)
                    evt = {
                        "type": "bybit_ticker",
                        "symbol": sym,
                        "oi": oi_val,
                        "funding": funding,
                    }
                    await market_event_queue.put(evt)
        except websockets.ConnectionClosed:
            logging.warning("Bybit ticker WS closed, reconnecting...")
            await asyncio.sleep(5)
            continue
        except Exception as e:
            logging.error(f"Bybit ticker WS error: {e}")
            await asyncio.sleep(5)


async def bybit_trades_consumer():
    fname = "bybit_trades"
    feed_metrics[fname] = FeedMetrics(name=fname)
    while not market_data:
        await asyncio.sleep(2)
    symbols = list(market_data.keys())
    chunks = [symbols[i:i + 10] for i in range(0, len(symbols), 10)]
    async for ws in websockets.connect(BYBIT_LINEAR_WS, ping_interval=20):
        try:
            for chunk in chunks:
                args = [f"publicTrade.{sym}" for sym in chunk]
                await ws.send(orjson.dumps({"op": "subscribe", "args": args}))
                await asyncio.sleep(0.1)
            async for msg in ws:
                data = json_loads(msg)
                topic = data.get("topic", "")
                if "publicTrade." not in topic:
                    continue
                fm = feed_metrics[fname]
                fm.messages += 1
                fm.last_message_ts = asyncio.get_event_loop().time()
                trades = data.get("data", [])
                for t in trades:
                    sym = t.get("s")
                    side = t.get("S")
                    qty_str = t.get("v")
                    if not sym or not side or not qty_str:
                        continue
                    try:
                        qty = float(qty_str)
                    except (TypeError, ValueError):
                        continue
                    evt = {
                        "type": "bybit_trade",
                        "symbol": sym,
                        "side": side,
                        "qty": qty,
                    }
                    await market_event_queue.put(evt)
        except websockets.ConnectionClosed:
            logging.warning("Bybit trades WS closed, reconnecting...")
            await asyncio.sleep(5)
            continue
        except Exception as e:
            logging.error(f"Bybit trades WS error: {e}")
            await asyncio.sleep(5)


async def binance_depth_consumer(top_symbols: List[str]):
    if not top_symbols:
        return
    streams = "/".join(f"{sym.lower()}@depth{DEPTH_LIMIT}@100ms" for sym in top_symbols)
    url = BINANCE_FUTURES_DEPTH_WS_TEMPLATE.format(streams=streams)
    fname = "binance_depth"
    feed_metrics[fname] = FeedMetrics(name=fname)
    async for ws in websockets.connect(url, ping_interval=20):
        try:
            async for msg in ws:
                data = json_loads(msg)
                payload = data.get("data")
                if not payload:
                    continue
                fm = feed_metrics[fname]
                fm.messages += 1
                fm.last_message_ts = asyncio.get_event_loop().time()
                sym = payload.get("s")
                if not sym or sym not in order_books:
                    continue
                evt = {
                    "type": "binance_depth",
                    "symbol": sym,
                    "U": int(payload.get("U")),
                    "u": int(payload.get("u")),
                    "bids": payload.get("b", []),
                    "asks": payload.get("a", []),
                }
                await market_event_queue.put(evt)
        except websockets.ConnectionClosed:
            logging.warning("Binance depth WS closed, reconnecting...")
            await asyncio.sleep(5)
            continue
        except Exception as e:
            logging.error(f"Binance depth WS error: {e}")
            await asyncio.sleep(5)


# -----------------------------------------------------------------------------
# METRICS & TICK ARCHIVE LOOPS
# -----------------------------------------------------------------------------
async def metrics_loop():
    while True:
        await asyncio.sleep(30.0)
        loop_time = asyncio.get_event_loop().time()
        qsize = market_event_queue.qsize()
        msg = [f"[Metrics] queue_depth={qsize}"]
        for name, fm in feed_metrics.items():
            delta_msgs = fm.messages - fm.last_messages
            fm.last_messages = fm.messages
            mps = delta_msgs / 30.0
            age = loop_time - fm.last_message_ts if fm.last_message_ts > 0 else 9999
            msg.append(
                f"{name}: mps={mps:.1f}, last_age={age:.1f}s, seq_gaps={fm.sequence_gaps}"
            )
            if age > 90 and not fm.dead_alert_sent:
                fm.dead_alert_sent = True
                logging.error(f"Feed {name} appears dead (no msgs for {age:.0f}s)")
                await send_telegram_alert(
                    f"⚠️ Feed {name} appears dead (no messages for {age:.0f}s)"
                )
        logging.info(" | ".join(msg))


async def tick_archive_flush_loop():
    if not HAVE_PANDAS:
        logging.warning(
            "pandas/pyarrow not installed – tick archive to Parquet is disabled."
        )
        return
    while True:
        await asyncio.sleep(TICK_ARCHIVE_FLUSH_INTERVAL)
        to_flush = dict(tick_archive_buffers)
        tick_archive_buffers.clear()
        for sym, rows in to_flush.items():
            if not rows:
                continue
            df = pd.DataFrame(rows)
            date_str = df["ts"].iloc[0][:10]
            fname = os.path.join(TICK_ARCHIVE_DIR, f"{sym}_{date_str}.parquet")
            try:
                if os.path.exists(fname):
                    df_existing = pd.read_parquet(fname)
                    df = pd.concat([df_existing, df], ignore_index=True)
                df.to_parquet(fname, index=False)
                logging.info(f"Flushed {len(rows)} ticks for {sym} to {fname}")
            except Exception as e:
                logging.error(f"Failed to flush ticks for {sym}: {e}")


def replay_ticks_from_parquet(path: str):
    """
    Simple offline replay helper: yields (ts, symbol, price, volume_quote_24h).
    """
    if not HAVE_PANDAS:
        raise RuntimeError("pandas not installed – cannot replay parquet.")
    df = pd.read_parquet(path)
    for row in df.itertuples(index=False):
        yield row.ts, row.symbol, row.price, row.volume_quote_24h


# -----------------------------------------------------------------------------
# UI LOOP
# -----------------------------------------------------------------------------
async def ui_loop(live: Live) -> None:
    while True:
        live.update(generate_dashboard())
        await asyncio.sleep(0.5)


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------
async def main():
    init_db()

    # warm-up universe from Binance futures
    fut_syms = await fetch_binance_futures_universe()
    for sym in fut_syms:
        if sym not in market_data:
            market_data[sym] = MarketState(symbol=sym, exchange="BINANCE-FUT")

    # choose top-N by 24h volume from initial snapshot later (for now first N)
    top_depth_syms = fut_syms[:TOP_N_DEPTH_SYMBOLS]
    # build initial depth snapshots
    for sym in top_depth_syms:
        try:
            ob = await fetch_depth_snapshot(sym)
            order_books[sym] = ob
        except Exception as e:
            logging.warning(f"Failed snapshot for {sym}: {e}")

    # workers
    workers = [asyncio.create_task(market_data_worker(i)) for i in range(4)]

    with Live(generate_dashboard(), refresh_per_second=4, screen=True) as live:
        await asyncio.gather(
            binance_futures_ticker_consumer(),
            binance_spot_miniticker_consumer(),
            bybit_ticker_consumer(),
            bybit_trades_consumer(),
            binance_depth_consumer(top_depth_syms),
            analyze_all_loop(),
            metrics_loop(),
            tick_archive_flush_loop(),
            ui_loop(live),
            *workers,
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
