import asyncio
import sqlite3
import logging
import time
import statistics
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

import ccxt.async_support as ccxt
from rich import box
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

# ============================================================
#   MEXC ORCHESTRATOR V2.6.1
#   FULL-UNIVERSE (ROTATING BATCH) + EARLY BREAKOUT +
#   BYBIT OI/CVD Side-Car v2.5 + Risk Manager
#
#   NEW in V2.6.1:
#   - Full-universe coverage kept, but deep scan is done in
#     ROTATING BATCHES (MAX_SYMBOLS_PER_CYCLE) each loop.
#   - Fixes "blank screen forever" caused by scanning 100s of
#     coins with heavy metrics in a single cycle.
#
#   From V2.6:
#   - Removed 24h % filter; all liquid USDT pairs considered.
#   - Full-universe mode, but now rotated.
#
#   From V2.5 / 2.4 / 2.3:
#   - Early Breakout Engine tuned (range_expansion, velocity_3).
#   - Pump stage classifier (EARLY/MID/LATE) from 24h %.
#   - >80% 24h = ⛔ TOO_LATE (score 0).
#   - Bybit OI/CVD side-car labels:
#       SPOT+SQZ, SMART_LONGS, FUT_TRAP, EXHAUST, SQUEEZE_RISK.
#   - Volatility based TP ladder:
#       SL  = entry - 1σ
#       TP1 = entry + 3σ
#       TP2 = entry + 5σ
#       TP3 = entry + 8σ
#   - RiskManager requires RR >= min_expect_rr (~4.6R default).
# ============================================================

# ---------- CONFIGURATION ----------

@dataclass
class Config:
    # Universe filters
    min_24h_volume_usdt: float = 50_000
    max_24h_volume_usdt: float = 50_000_000
    # NOTE: min_price_change_24h is unused in full-universe mode
    min_price_change_24h: float = 5.0

    # Liquidity / spread filters
    max_spread_pct: float = 2.0
    min_book_depth_usdt: float = 10_000

    # Baseline periods
    baseline_period: int = 90  # for 5m and 1h

    # OTR / manipulation
    max_otr_threshold: float = 60.0

    # Scan loop
    scan_interval_seconds: int = 20

    # Score thresholds
    pre_ignition_threshold: int = 45
    alert_threshold: int = 70

    # Early Breakout Engine thresholds (V2.4 tuned)
    ebe_min_range_expansion: float = 2.0   # 2x avg 5m range
    ebe_strong_range_expansion: float = 3.0
    ebe_velocity_med: float = 0.03         # +3% over last 3 bars
    ebe_velocity_strong: float = 0.05      # +5% over last 3 bars
    ebe_min_early_score: int = 15          # min EBE score to apply bias

    # Risk manager config
    min_expect_rr: float = 3.0             # minimum expected RR (blended)


cfg = Config()

DB_NAME = "mexc_orchestrator.sqlite"
console = Console()

# Deep scan batch size per loop (rotating over full universe)
MAX_SYMBOLS_PER_CYCLE = 120

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


# ---------- STATE ----------

class ScreenerState:
    def __init__(self):
        self.status = "Initializing Pump Anticipator V2.6 (Full-Universe)..."
        self.last_scan_time = "N/A"
        self.candidates: List[Dict] = []
        self.alerts = deque(maxlen=10)
        self.active_trades: Dict[str, "ActiveTrade"] = {}
        self.futures_insights: Dict[str, Dict[str, Any]] = {}
        # where in the full candidate list we are (for rotation)
        self.rotation_offset: int = 0


state = ScreenerState()


# ---------- RISK MANAGER ----------

class RiskManager:
    """
    Symbol-level risk gate enforcing:
      - minimum expected RR based on TP ladder vs SL.

    Ladder in σ:
      SL  = entry - 1σ
      TP1 = entry + 3σ  (50% size)
      TP2 = entry + 5σ  (30% size)
      TP3 = entry + 8σ  (20% size)

    Expected gain:
      E[gain] ≈ 0.5*3 + 0.3*5 + 0.2*8 = 4.6 σ
      Risk = 1 σ  → RR ≈ 4.6R
    """

    def __init__(self, config: Config):
        self.cfg = config

    def estimate_expected_rr(self, sigma: float) -> float:
        if sigma <= 0:
            return 0.0
        sl_dist = 1.0
        tp1_dist = 3.0
        tp2_dist = 5.0
        tp3_dist = 8.0
        exp_gain_sigma = 0.5 * tp1_dist + 0.3 * tp2_dist + 0.2 * tp3_dist
        rr = exp_gain_sigma / sl_dist
        return rr

    def can_open_trade(self, symbol: str, entry_price: float, sigma: float) -> Tuple[bool, str]:
        if sigma <= 0 or entry_price <= 0:
            return False, "BAD_SIGMA"

        rr = self.estimate_expected_rr(sigma)
        if rr < self.cfg.min_expect_rr:
            return False, f"RR_TOO_LOW({rr:.2f})"

        return True, "OK"


risk_manager = RiskManager(cfg)


# ---------- ACTIVE TRADE / TP ENGINE ----------

class ActiveTrade:
    def __init__(self, symbol, entry_price, volatility_unit, start_time):
        self.symbol = symbol
        self.entry_price = entry_price
        self.entry_time = start_time
        self.volatility = volatility_unit  # Sigma (σ)
        self.current_price = entry_price
        self.highest_price = entry_price
        self.status = "ENTRY"

        # High-R:R TP ladder.
        self.sl = entry_price - (1.0 * volatility_unit)      # -1σ
        self.tp1 = entry_price + (3.0 * volatility_unit)     # +3σ
        self.tp2 = entry_price + (5.0 * volatility_unit)     # +5σ
        self.tp3 = entry_price + (8.0 * volatility_unit)     # +8σ

        self.logs: List[str] = []
        self.pnl_locked = 0.0

    def update(self, current_price, vol_z_5m, rsi, ask_wall_price=None):
        self.current_price = current_price
        if current_price > self.highest_price:
            self.highest_price = current_price

        # --- ASK-WALL SMART ADJUSTMENT ---
        if ask_wall_price:
            if self.status == "ENTRY" and self.tp1 > ask_wall_price > current_price:
                self.tp1 = ask_wall_price * 0.999
                self.log_event(f"⚠️ TP1 adjusted below wall: {self.tp1:.5f}")
            elif self.status == "GEAR_1" and self.tp2 > ask_wall_price > current_price:
                self.tp2 = ask_wall_price * 0.999
                self.log_event(f"⚠️ TP2 adjusted below wall: {self.tp2:.5f}")

        # --- GEAR LOGIC ---

        # GEAR 1: safety lock at TP1
        if self.status == "ENTRY" and current_price >= self.tp1:
            self.status = "GEAR_1"
            self.sl = self.entry_price * 1.002  # breakeven + fees
            self.pnl_locked += (self.tp1 - self.entry_price) * 0.5
            self.log_event("✅ TP1 HIT: Sold 50% | SL -> Breakeven+fees")
            update_trade_db(self, "TP1_HIT")

        # GEAR 2: trend rider
        elif self.status == "GEAR_1" and current_price >= self.tp2:
            self.status = "GEAR_2"
            self.pnl_locked += (self.tp2 - self.entry_price) * 0.3
            self.log_event("✅ TP2 HIT: Sold 30% | Runner live")
            update_trade_db(self, "TP2_HIT")

        # GEAR 3: moonbag mode
        elif self.status == "GEAR_2" and current_price >= self.tp3:
            self.status = "GEAR_3"
            self.log_event("🚀 TP3 CROSSED: Moonbag mode | Panic sensor ON")
            update_trade_db(self, "TP3_CROSS")

        # --- TRAILING STOP ---
        if self.status in ["GEAR_2", "GEAR_3"]:
            trail_dist = 1.5 * self.volatility
            if vol_z_5m < 0:  # volume fading
                trail_dist = 1.0 * self.volatility
            new_sl = self.highest_price - trail_dist
            if new_sl > self.sl:
                self.sl = new_sl

        # --- STOP / EXIT ---
        if current_price <= self.sl:
            self.status = "CLOSED"
            reason = "STOP_LOSS" if self.sl < self.entry_price else "TRAILING_PROFIT"
            self.log_event(f"🛑 TRADE CLOSED: {reason} @ {current_price}")
            update_trade_db(self, "CLOSED")
            return False

        # --- PANIC SENSOR in parabolic phase ---
        if self.status == "GEAR_3":
            is_climax = (vol_z_5m > 5.0 and current_price < self.highest_price * 0.99)
            if is_climax or rsi > 90:
                self.status = "CLOSED"
                self.log_event(f"🚨 PANIC SELL: Climax Detected (Vol Z: {vol_z_5m:.1f})")
                update_trade_db(self, "CLOSED_PANIC")
                return False

        return True

    def log_event(self, message: str):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.logs.append(f"[{timestamp}] {message}")


# ---------- DB HELPERS ----------

def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS mexc_alerts (
            symbol TEXT PRIMARY KEY,
            alert_timestamp INTEGER,
            alert_price REAL,
            volume_24h REAL,
            change_24h REAL,
            score INTEGER,
            tags TEXT
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS trade_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            entry_time TEXT,
            entry_price REAL,
            tp1_target REAL,
            tp2_target REAL,
            tp3_threshold REAL,
            exit_price REAL,
            status TEXT,
            last_event TEXT
        )
        """
    )
    conn.commit()
    conn.close()


def has_been_alerted(symbol: str) -> bool:
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    threshold = int(time.time()) - (4 * 3600)
    cursor.execute(
        "SELECT 1 FROM mexc_alerts WHERE symbol = ? AND alert_timestamp > ?",
        (symbol, threshold),
    )
    res = cursor.fetchone()
    conn.close()
    return res is not None


def record_alert(data: Dict[str, Any]):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT OR REPLACE INTO mexc_alerts 
        (symbol, alert_timestamp, alert_price, volume_24h, change_24h, score, tags) 
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            data["symbol"],
            int(time.time()),
            data["price"],
            data["volume_24h"],
            data["change_24h"],
            data["score"],
            ",".join(data["tags"]),
        ),
    )
    conn.commit()
    conn.close()
    state.alerts.appendleft(
        {
            "time": datetime.now().strftime("%H:%M:%S"),
            "symbol": data["symbol"],
            "price": data["price"],
            "change": data["change_24h"],
            "score": data["score"],
            "tags": data["tags"],
        }
    )


def update_trade_db(trade: ActiveTrade, event_type: str):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    last_event = trade.logs[-1] if trade.logs else event_type
    cursor.execute(
        """
        INSERT INTO trade_history 
        (symbol, entry_time, entry_price, tp1_target, tp2_target, tp3_threshold, exit_price, status, last_event) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            trade.symbol,
            trade.entry_time,
            trade.entry_price,
            trade.tp1,
            trade.tp2,
            trade.tp3,
            trade.current_price,
            trade.status,
            last_event,
        ),
    )
    conn.commit()
    conn.close()


# ---------- ANALYTICS / FEATURES ----------

def calculate_z_score(values: List[float]) -> float:
    if len(values) < 10:
        return 0.0
    history = values[:-1]
    current = values[-1]
    mean = statistics.mean(history)
    stdev = statistics.stdev(history)
    return (current - mean) / stdev if stdev != 0 else 0.0


async def check_liquidity_veto(exchange, symbol: str) -> Optional[Dict[str, Any]]:
    """
    Returns:
        {
            'failed': bool,
            'wall_price': float | None,
            'obi': float,
            'depth_usd': float
        }
    """
    try:
        book = await exchange.fetch_order_book(symbol, limit=20)
        bids = book["bids"]
        asks = book["asks"]
        if not bids or not asks:
            return None

        best_bid = bids[0][0]
        best_ask = asks[0][0]
        mid = (best_bid + best_ask) / 2

        spread_pct = ((best_ask - best_bid) / mid) * 100
        if spread_pct > cfg.max_spread_pct:
            return {"failed": True, "wall_price": None, "obi": 0.0, "depth_usd": 0.0}

        bid_depth_usd = sum(p * q for p, q in bids if p >= mid * 0.98)
        ask_depth_usd = sum(p * q for p, q in asks if p <= mid * 1.02)
        depth_usd = bid_depth_usd + ask_depth_usd
        if depth_usd < cfg.min_book_depth_usdt:
            return {"failed": True, "wall_price": None, "obi": 0.0, "depth_usd": depth_usd}

        if (bid_depth_usd + ask_depth_usd) > 0:
            obi = (bid_depth_usd - ask_depth_usd) / (bid_depth_usd + ask_depth_usd)
        else:
            obi = 0.0

        avg_ask_size = sum(q for _, q in asks[:10]) / 10 if len(asks) >= 10 else 0
        wall_price = None
        if avg_ask_size > 0:
            for p, q in asks[:10]:
                if q > avg_ask_size * 5:
                    wall_price = p
                    break

        return {
            "failed": False,
            "wall_price": wall_price,
            "obi": obi,
            "depth_usd": depth_usd,
        }
    except Exception:
        return None


def _compute_true_range_series(ohlcv: List[List[float]]) -> List[float]:
    if not ohlcv:
        return []
    trs: List[float] = []
    prev_close = ohlcv[0][4]
    for ts, o, h, l, c, v in ohlcv:
        tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
        trs.append(tr)
        prev_close = c
    return trs


def _compute_navd_ratio(trades: List[Dict[str, Any]]) -> float:
    buy_usd = 0.0
    sell_usd = 0.0
    for t in trades:
        side = t.get("side")
        price = t.get("price") or float(t.get("info", {}).get("p", 0) or 0)
        amount = t.get("amount") or 0
        size_usd = price * amount
        if size_usd <= 0:
            continue
        if side == "buy":
            buy_usd += size_usd
        elif side == "sell":
            sell_usd += size_usd
    total = buy_usd + sell_usd
    if total <= 0:
        return 0.0
    return (buy_usd - sell_usd) / total


async def analyze_metrics(exchange, symbol: str) -> Dict[str, Any]:
    """
    Core + Early Breakout features on SPOT:
      - z_5m, z_60m
      - sigma (1h volatility)
      - rsi (1h)
      - otr (% of big trades last 60s)
      - navd_1m, navd_5m
      - compression (VCP proxy)
      - above_vwap
      - range_expansion, micro_break, velocity_3 for EBE
    """
    try:
        t_5m = exchange.fetch_ohlcv(symbol, "5m", limit=cfg.baseline_period + 5)
        t_1h = exchange.fetch_ohlcv(symbol, "1h", limit=cfg.baseline_period + 5)
        t_trades = exchange.fetch_trades(symbol, limit=200)

        data_5m, data_1h, trades = await asyncio.gather(t_5m, t_1h, t_trades)

        if len(data_5m) < cfg.baseline_period or len(data_1h) < 30:
            return {}

        closes_1h = [x[4] for x in data_1h]
        recent = closes_1h[-24:]
        if len(recent) > 2:
            sigma = statistics.stdev(recent)
        else:
            sigma = closes_1h[-1] * 0.02

        volumes_5m = [x[5] for x in data_5m]
        volumes_1h = [x[5] for x in data_1h]
        z_5m = calculate_z_score(volumes_5m)
        z_60m = calculate_z_score(volumes_1h)

        deltas = [closes_1h[i + 1] - closes_1h[i] for i in range(len(closes_1h) - 1)]
        gain = sum(x for x in deltas[-14:] if x > 0)
        loss = abs(sum(x for x in deltas[-14:] if x < 0))
        rsi = 100 - (100 / (1 + (gain / loss))) if loss != 0 else 50

        now_ms = exchange.milliseconds()
        trades_1m = [t for t in trades if (now_ms - t["timestamp"]) <= 60_000]
        trades_5m = [t for t in trades if (now_ms - t["timestamp"]) <= 300_000]

        navd_1m = _compute_navd_ratio(trades_1m)
        navd_5m = _compute_navd_ratio(trades_5m)

        # OTR
        if trades_1m:
            usd_sizes = []
            for t in trades_1m:
                price = t.get("price") or float(t.get("info", {}).get("p", 0) or 0)
                amount = t.get("amount") or 0
                size_usd = price * amount
                if size_usd > 0:
                    usd_sizes.append(size_usd)
            if usd_sizes:
                avg_size = statistics.mean(usd_sizes)
                big_trades = [s for s in usd_sizes if s > avg_size * 2]
                otr = 100.0 * len(big_trades) / max(len(usd_sizes), 1)
            else:
                otr = 0.0
        else:
            otr = 0.0

        # Compression
        trs_5m = _compute_true_range_series(data_5m)
        if len(trs_5m) >= 30:
            short = trs_5m[-12:]
            long = trs_5m[-60:] if len(trs_5m) >= 60 else trs_5m
            short_std = statistics.stdev(short) if len(short) > 1 else 0.0
            long_std = statistics.stdev(long) if len(long) > 1 else 0.0
            compression = short_std / long_std if long_std > 0 else 1.0
        else:
            compression = 1.0

        closes_5m = [x[4] for x in data_5m]
        last_price = closes_5m[-1]
        vols_1h = volumes_1h
        if sum(vols_1h) > 0:
            vwap_1h = sum(c * v for c, v in zip(closes_1h, vols_1h)) / sum(vols_1h)
        else:
            vwap_1h = closes_1h[-1]
        above_vwap = 1.0 if last_price > vwap_1h else 0.0

        # Early Breakout features (V2.4 tweaks)
        ranges_5m = [x[2] - x[3] for x in data_5m]
        if len(ranges_5m) > 21:
            avg_range20 = statistics.mean(ranges_5m[-21:-1])
            range_expansion = ranges_5m[-1] / avg_range20 if avg_range20 > 0 else 1.0
        else:
            range_expansion = 1.0

        # shorter window: last 8 bars (was 12)
        if len(closes_5m) > 9:
            recent_high = max(closes_5m[-9:-1])
            micro_break = 1.0 if last_price > recent_high else 0.0
        else:
            micro_break = 0.0

        if len(closes_5m) > 4 and closes_5m[-4] > 0:
            velocity_3 = (closes_5m[-1] - closes_5m[-4]) / closes_5m[-4]
        else:
            velocity_3 = 0.0

        return {
            "z_5m": z_5m,
            "z_60m": z_60m,
            "sigma": sigma,
            "rsi": rsi,
            "otr": otr,
            "navd_1m": navd_1m,
            "navd_5m": navd_5m,
            "compression": compression,
            "above_vwap": above_vwap,
            "range_expansion": range_expansion,
            "micro_break": micro_break,
            "velocity_3": velocity_3,
        }
    except Exception as e:
        logging.exception(f"analyze_metrics error for {symbol}: {e}")
        return {}


# ---------- BYBIT SIDE-CAR ----------

def map_mexc_to_bybit_perp(spot_symbol: str, bybit) -> Optional[str]:
    try:
        if not getattr(bybit, "markets", None):
            return None
        base, quote = spot_symbol.split("/")
        candidates = [
            m for m in bybit.markets.values()
            if m.get("base") == base and m.get("quote") == quote and m.get("swap", False)
        ]
        if not candidates:
            return None
        return candidates[0]["symbol"]
    except Exception:
        return None


async def fetch_bybit_futures_insight(bybit, spot_symbol: str) -> Optional[Dict[str, Any]]:
    try:
        if not getattr(bybit, "has", {}).get("fetchOpenInterest", False):
            return None

        perp_symbol = map_mexc_to_bybit_perp(spot_symbol, bybit)
        if not perp_symbol:
            return None

        t_trades = bybit.fetch_trades(perp_symbol, limit=200)
        t_oi = bybit.fetch_open_interest(perp_symbol)

        trades, oi_info = await asyncio.gather(t_trades, t_oi)

        now_ms = bybit.milliseconds()
        trades_5m = [t for t in trades if (now_ms - t["timestamp"]) <= 300_000]
        fut_navd_5m = _compute_navd_ratio(trades_5m)

        oi_now = float(
            oi_info.get("openInterest", oi_info.get("info", {}).get("openInterest", 0.0))
        )
        prev = state.futures_insights.get(spot_symbol, {})
        prev_oi = prev.get("oi_now", oi_now)
        oi_5m_change = (oi_now - prev_oi) / prev_oi * 100 if prev_oi > 0 else 0.0

        insight = {
            "perp_symbol": perp_symbol,
            "oi_now": oi_now,
            "oi_5m_change": oi_5m_change,
            "fut_navd_5m": fut_navd_5m,
        }
        state.futures_insights[spot_symbol] = insight
        return insight
    except Exception as e:
        logging.exception(f"fetch_bybit_futures_insight error for {spot_symbol}: {e}")
        return None


def classify_oi_cvd_divergence_v2(
    spot_navd_5m: float,
    fut_navd_5m: float,
    oi_5m_change: float,
    stage: str,
) -> (str, int, List[str]):
    """
    Futures side-car logic (v2.5):

      - SPOT+SQZ : spot-led with shorts building on perps (good)
      - SMART_LONGS : both spot and perps long (good, but watch if LATE)
      - FUT_TRAP : futures-led pump, spot weak (trap)
      - EXHAUST : late-stage, both spot demand & OI fading (end of move)
      - SQUEEZE_RISK : dangerous late squeeze
    """
    label = "NEUTRAL"
    bump = 0
    tags: List[str] = []

    spot = spot_navd_5m
    fut = fut_navd_5m
    oi = oi_5m_change

    # --- EXHAUSTION: late stage & both spot demand + OI fading ---
    if stage in ("MID", "LATE") and spot <= -0.05 and oi <= -3:
        label = "EXHAUST"
        bump = -20
        tags.append("📉EXHAUST")

    # --- FUTURES-LED TRAP: perps doing the heavy lifting, spot weak ---
    elif fut > 0.2 and oi > 5 and spot < 0.05:
        label = "FUT_TRAP"
        bump = -20
        tags.append("⛔FUT_TRAP")

    # --- SPOT-LED WITH SQUEEZE POTENTIAL: your dream scenario ---
    elif spot > 0.15 and 3 < oi <= 15 and fut < -0.1:
        label = "SPOT_PLUS_SQUEEZE"
        bump = +15
        tags.append("🧲SPOT+SQZ")

    # --- SMART LONGS ON BOTH VENUES ---
    elif spot > 0.15 and fut > 0.15 and oi > 3:
        label = "SMART_LONGS"
        bump = +10 if stage != "LATE" else 0
        tags.append("🧲SMART_LONGS")

    # --- DANGEROUS LATE SQUEEZE ---
    elif oi > 15 and spot < 0 and stage in ("MID", "LATE"):
        label = "SQUEEZE_RISK"
        bump = -10
        tags.append("🧨SQUEEZE")

    return label, bump, tags


# ---------- PUMP STAGE CLASSIFICATION ----------

def classify_pump_stage(change_24h: float) -> str:
    """
    Simple heuristic:
        < 5%   → COLD  (we generally ignore anyway)
        5–20%  → EARLY (sweet spot for first leg)
        20–60% → MID   (already moved a lot, but can continue)
        > 60%  → LATE  (be careful)
    """
    if change_24h < 5:
        return "COLD"
    if 5 <= change_24h <= 20:
        return "EARLY"
    if 20 < change_24h <= 60:
        return "MID"
    return "LATE"


# ---------- SCORING ENGINE ----------

def compute_pump_score(
    symbol: str,
    price: float,
    base_change_24h: float,
    metrics: Dict[str, Any],
    liq_info: Optional[Dict[str, Any]],
    fut_insight: Optional[Dict[str, Any]] = None,
) -> (int, List[str], str, str):
    """
    Returns:
        score, tags, fut_label, stage
    """
    score = 0
    tags: List[str] = [f"{base_change_24h:.1f}%"]
    fut_label = "NEUTRAL"
    stage = classify_pump_stage(base_change_24h)

    z_5m = metrics.get("z_5m", 0.0)
    z_60m = metrics.get("z_60m", 0.0)
    navd_5m = metrics.get("navd_5m", 0.0)
    compression = metrics.get("compression", 1.0)
    above_vwap = metrics.get("above_vwap", 0.0)
    otr = metrics.get("otr", 0.0)
    range_expansion = metrics.get("range_expansion", 1.0)
    micro_break = metrics.get("micro_break", 0.0)
    velocity_3 = metrics.get("velocity_3", 0.0)

    obi = 0.0
    depth_usd = 0.0
    if liq_info:
        obi = liq_info.get("obi", 0.0)
        depth_usd = liq_info.get("depth_usd", 0.0)

    # ----- HARD LATE-PUMP VETO -----
    if base_change_24h > 80:
        tags.append("⛔TOO_LATE")
        return 0, tags, fut_label, stage

    # ----- STAGE-BASED SHAPING -----
    tags.append(f"STAGE:{stage}")
    if stage == "EARLY":
        score += 10
    elif stage == "MID":
        score += 0
    elif stage == "LATE":
        score -= 15

    # QUALITY GATES
    if otr > cfg.max_otr_threshold:
        tags.append("⚠️WEIRD_FLOW")
        return 0, tags, fut_label, stage

    if depth_usd < cfg.min_book_depth_usdt * 1.5:
        tags.append("💧THIN")
    else:
        score += 5

    if obi > 0.1:
        score += 5
        tags.append("OBI+")
    elif obi < -0.1:
        score -= 5
        tags.append("OBI-")

    # EARLY BREAKOUT ENGINE
    early_score = 0
    if range_expansion > cfg.ebe_min_range_expansion:
        early_score += 10
    if range_expansion > cfg.ebe_strong_range_expansion:
        early_score += 5
    if micro_break >= 0.5:
        early_score += 10
    if velocity_3 > cfg.ebe_velocity_strong:
        early_score += 10
    elif velocity_3 > cfg.ebe_velocity_med:
        early_score += 5

    if early_score >= cfg.ebe_min_early_score and navd_5m > -0.1:
        score += early_score
        tags.append("EARLY_BREAKOUT")

    # ACCUMULATION
    if navd_5m > 0:
        acc_navd = min(20, max(0, int(20 * navd_5m)))
        score += acc_navd
        tags.append(f"NAVD:{navd_5m:+.2f}")

    if compression < 0.8:
        score += 10
        tags.append(f"VCP:{compression:.2f}")

    if above_vwap >= 0.5:
        score += 5
        tags.append("VWAP+")

    # IGNITION
    if z_5m > 1.5:
        ign = 10 + int(max(0.0, z_5m - 1.5) * 8.0)
        ign = min(35, ign)
        score += ign
        tags.append(f"Z5:{z_5m:.1f}")

    if z_60m > 2.0:
        score += 5
        tags.append("Z60+")

    # BYBIT SIDE-CAR v2.5
    if fut_insight is not None:
        label, bump, fut_tags = classify_oi_cvd_divergence_v2(
            spot_navd_5m=navd_5m,
            fut_navd_5m=fut_insight.get("fut_navd_5m", 0.0),
            oi_5m_change=fut_insight.get("oi_5m_change", 0.0),
            stage=stage,
        )
        fut_label = label
        score += bump
        tags.extend(fut_tags)

    score = max(0, min(score, 100))

    if score >= cfg.alert_threshold:
        tags.append("🚀IGNITION")
    elif score >= cfg.pre_ignition_threshold:
        tags.append("👀PRIMED")

    return score, tags, fut_label, stage


# ---------- MAIN PIPELINE (FULL-UNIVERSE, ROTATING) ----------

async def process_market(spot_exchange, bybit_exchange=None):
    state.status = "Fetching Tickers..."
    try:
        tickers = await spot_exchange.fetch_tickers()
    except Exception as e:
        logging.exception(f"fetch_tickers failed: {e}")
        state.status = f"fetch_tickers failed: {e}"
        return

    candidates: List[Dict[str, Any]] = []

    # FULL-UNIVERSE over all USDT pairs (no 24h % filter)
    for s, t in tickers.items():
        if "/USDT" not in s or "3L" in s or "3S" in s:
            continue
        try:
            vol = float(t["quoteVolume"])
            chg = float(t["percentage"])
            price = float(t["last"])
        except Exception:
            continue

        if vol < cfg.min_24h_volume_usdt or vol > cfg.max_24h_volume_usdt:
            continue

        candidates.append({"symbol": s, "price": price, "vol": vol, "chg": chg})

    # If nothing liquid, just clear and exit early
    if not candidates:
        state.candidates = []
        state.status = "No liquid USDT markets found."
        return

    # Sort by 24h change just for display / priority
    candidates.sort(key=lambda x: x["chg"], reverse=True)

    # -------- ROTATING BATCH (NEW) --------
    total = len(candidates)
    batch_size = min(MAX_SYMBOLS_PER_CYCLE, total)

    start = state.rotation_offset % total
    end = start + batch_size

    if end <= total:
        batch = candidates[start:end]
    else:
        # wrap around
        batch = candidates[start:] + candidates[: end - total]

    # advance pointer for next loop
    state.rotation_offset = (start + batch_size) % total

    top_candidates = batch  # only these get deep metrics this cycle

    state.candidates = []
    analyzed_batch: List[Dict[str, Any]] = []

    symbols_to_check = set([c["symbol"] for c in top_candidates] + list(state.active_trades.keys()))
    state.status = f"Vetting {len(symbols_to_check)} of {total} symbols (rotating full-universe)..."

    for symbol in symbols_to_check:
        ticker = tickers.get(symbol)
        if not ticker:
            continue
        price = float(ticker["last"])

        liq = await check_liquidity_veto(spot_exchange, symbol)
        if liq is None:
            continue
        wall_price = liq["wall_price"] if not liq.get("failed", False) else None
        if symbol not in state.active_trades and liq.get("failed", False):
            continue

        m = await analyze_metrics(spot_exchange, symbol)
        if not m:
            continue

        if symbol in state.active_trades:
            trade = state.active_trades[symbol]
            alive = trade.update(price, m.get("z_5m", 0.0), m.get("rsi", 50.0), wall_price)
            if not alive:
                del state.active_trades[symbol]

        fut_insight = None
        if bybit_exchange is not None:
            try:
                fut_insight = await fetch_bybit_futures_insight(bybit_exchange, symbol)
            except Exception:
                fut_insight = None

        cand = next((c for c in top_candidates if c["symbol"] == symbol), None)
        if cand and symbol not in state.active_trades:
            pump_score, tags, fut_label, stage = compute_pump_score(
                symbol=symbol,
                price=price,
                base_change_24h=cand["chg"],
                metrics=m,
                liq_info=liq,
                fut_insight=fut_insight,
            )

            final = {
                **cand,
                "volume_24h": cand["vol"],
                "change_24h": cand["chg"],
                "score": pump_score,
                "tags": tags,
                "z_5m": m.get("z_5m", 0.0),
                "otr": m.get("otr", 0.0),
                "navd_5m": m.get("navd_5m", 0.0),
                "compression": m.get("compression", 1.0),
                "fut_signal": fut_label,
                "sigma": m.get("sigma", price * 0.02),
                "stage": stage,
            }
            analyzed_batch.append(final)

            if pump_score >= cfg.alert_threshold and not has_been_alerted(symbol):
                sigma = final["sigma"]
                ok, reason = risk_manager.can_open_trade(symbol, price, sigma)
                if not ok:
                    logging.info(f"Risk veto for {symbol}: {reason}")
                else:
                    record_alert(final)
                    trade = ActiveTrade(
                        symbol,
                        price,
                        sigma,
                        datetime.now().strftime("%H:%M:%S"),
                    )
                    state.active_trades[symbol] = trade
                    update_trade_db(trade, "OPEN_TRADE")

        # respect rate limit to avoid bans
        try:
            await asyncio.sleep(spot_exchange.rateLimit / 1000.0)
        except Exception:
            await asyncio.sleep(0.2)

    analyzed_batch.sort(key=lambda x: x["score"], reverse=True)
    state.candidates = analyzed_batch


# ---------- DASHBOARD ----------

def generate_dashboard() -> Layout:
    layout = Layout()
    layout.split(
        Layout(name="header", size=3),
        Layout(name="top", ratio=1),
        Layout(name="middle", ratio=1),
        Layout(name="bottom", size=8),
    )

    layout["header"].update(
        Panel(
            f"🦅🔥 MEXC ORCHESTRATOR V2.6.1 | FULL-UNIVERSE (ROTATING) + EARLY BREAKOUT + BYBIT OI/CVD v2.5 | Status: {state.status}",
            style="bold white on dark_red",
        )
    )

    t_scan = Table(
        title="🚨 V2.6 EARLY BREAKOUT RADAR (FULL-UNIVERSE) 🚨",
        expand=True,
        box=box.SIMPLE_HEAVY,
        header_style="bold white",
    )

    t_scan.add_column("Symbol 💎", style="bold cyan")
    t_scan.add_column("Price 💰")
    t_scan.add_column("Score ⚡", justify="center")
    t_scan.add_column("Z5 📈", justify="center")
    t_scan.add_column("OTR 🧨", justify="center")
    t_scan.add_column("OI–CVD 🧲", justify="center")
    t_scan.add_column("Signal Tags 🧠", overflow="fold")
    t_scan.add_column("Baby View 🍼", overflow="fold")

    for c in state.candidates[:80]:  # display top 80 rows max for readability
        score = c["score"]
        otr = c["otr"]
        z5 = c["z_5m"]
        tags = c["tags"]
        fut_signal = c.get("fut_signal", "NEUTRAL")
        stage = c.get("stage", "UNKNOWN")

        if score >= cfg.alert_threshold:
            score_style = "bold white on green"
            score_icon = "🚀"
        elif score >= cfg.pre_ignition_threshold:
            score_style = "bold black on yellow"
            score_icon = "👀"
        else:
            score_style = "dim"
            score_icon = "❄️"

        if otr < 10:
            otr_style = "green"
        elif otr > cfg.max_otr_threshold:
            otr_style = "bold red"
            otr = min(otr, 999)
        else:
            otr_style = "yellow"

        if z5 >= 2:
            z5_style = "bold green"
        elif z5 <= -2:
            z5_style = "red"
        else:
            z5_style = "yellow"

        if fut_signal == "SPOT_PLUS_SQUEEZE":
            fut_style = "bold green"
            fut_text = "SPOT+SQZ"
        elif fut_signal == "SMART_LONGS":
            fut_style = "bold green"
            fut_text = "LONGS"
        elif fut_signal == "FUT_TRAP":
            fut_style = "bold red"
            fut_text = "TRAP"
        elif fut_signal == "EXHAUST":
            fut_style = "red"
            fut_text = "EXHAUST"
        elif fut_signal == "SQUEEZE_RISK":
            fut_style = "yellow"
            fut_text = "SQZ"
        else:
            fut_style = "dim"
            fut_text = "-"

        url = f"https://www.mexc.com/exchange/{c['symbol'].replace('/', '_')}"
        sym = f"[link={url}]💎 {c['symbol']}[/link]"

        tags_str = " ".join([
            tag.replace("NAVD", "📊NAVD")
               .replace("VCP", "🫀VCP")
               .replace("VWAP+", "🟢VWAP↑")
               .replace("OBI+", "📗OBI+")
               .replace("OBI-", "📕OBI-")
               .replace("THIN", "⚠️THIN")
               .replace("IGNITION", "🔥IGNITION")
               .replace("PRIMED", "👀PRIMED")
               .replace("EARLY_BREAKOUT", "⚡EARLY")
            for tag in tags
        ])

        has_thin = any("THIN" in t for t in tags)
        has_primed = any("PRIMED" in t for t in tags)
        has_ignition = any("IGNITION" in t for t in tags)
        has_early = any("EARLY_BREAKOUT" in t for t in tags)
        has_navd_up = any("NAVD:+0." in t or "NAVD:+1" in t or "NAVD:+0" in t for t in tags)
        has_vcp = any("VCP" in t for t in tags)
        has_vwap_up = any("VWAP" in t for t in tags)
        is_too_late = any("TOO_LATE" in t for t in tags)

        if is_too_late:
            baby = "⛔ Already pumped a lot today. Bot says it's too late to chase."
        elif stage == "EARLY":
            if has_ignition or score >= cfg.alert_threshold:
                baby = "⚡ EARLY stage pump. First leg is exploding now."
            elif has_early and score >= cfg.pre_ignition_threshold:
                baby = "⚡ First breakout from tight range. This is the sweet spot."
            elif has_early:
                baby = "🌱 Very early breakout signs. Watch closely."
            else:
                baby = "🙂 Early 24h move, building energy."
        elif stage == "MID":
            if has_ignition or score >= cfg.alert_threshold:
                baby = "🚀 Mid-run pump. Can continue, but risk of pullback higher."
            else:
                baby = "😎 Coin already moved a fair bit. Look for clean pullback entries."
        elif stage == "LATE":
            baby = "⚠️ Late-stage pump. High risk of nasty dump."
        else:
            baby = "❄️ Boring for now. Ignore."

        risk_bits = []
        if has_thin:
            risk_bits.append("⚠️ Thin book, easy to rug.")
        if otr > cfg.max_otr_threshold:
            risk_bits.append("🧨 Weird flow, could be fake pump.")
        elif otr > 20:
            risk_bits.append("🟡 Some weird flow. Be careful.")

        if has_navd_up and has_vcp and has_vwap_up and score >= cfg.pre_ignition_threshold:
            risk_bits.append("🔥 Strong hands buying quietly.")
        elif has_navd_up and has_vcp and score < cfg.pre_ignition_threshold:
            risk_bits.append("🌱 Smart money nibbling, still early.")

        if fut_signal == "SPOT_PLUS_SQUEEZE":
            risk_bits.append("🧲 Spot-led pump with shorts on perps. Squeeze fuel.")
        elif fut_signal == "SMART_LONGS":
            risk_bits.append("🧲 Futures also long. Good confluence.")
        elif fut_signal == "FUT_TRAP":
            risk_bits.append("⛔ Futures-led pump. High rug risk.")
        elif fut_signal == "EXHAUST":
            risk_bits.append("📉 Exhaustion: leverage and spot demand fading.")
        elif fut_signal == "SQUEEZE_RISK":
            risk_bits.append("🧨 High-risk squeeze. Take quick profits if in.")

        if risk_bits:
            baby = f"{baby} " + " ".join(risk_bits)

        t_scan.add_row(
            sym,
            f"💰 ${c['price']:.6f}",
            f"[{score_style}]{score_icon}{score}[/]",
            f"[{z5_style}]{z5:.1f}[/]",
            f"[{otr_style}]{otr:.1f}%[/]",
            f"[{fut_style}]{fut_text}[/]",
            tags_str,
            baby,
        )

    layout["top"].update(Panel(t_scan, border_style="bright_cyan"))

    t_trade = Table(
        title="⚙️🚀 ACTIVE PUMP TRADES",
        expand=True,
        box=box.HEAVY_EDGE,
        header_style="bold white",
    )

    t_trade.add_column("Symbol 🎯", style="bold yellow")
    t_trade.add_column("Status 🔥", justify="center")
    t_trade.add_column("Entry 💵")
    t_trade.add_column("Current 💸")
    t_trade.add_column("PnL % 📈")
    t_trade.add_column("TP1 🟢")
    t_trade.add_column("TP2 🟡")
    t_trade.add_column("TP3 🔴")
    t_trade.add_column("SL 🛑")

    if not state.active_trades:
        t_trade.add_row("-", "😴 No Active Trades", "-", "-", "-", "-", "-", "-", "-")
    else:
        for s, t in state.active_trades.items():
            pnl = ((t.current_price - t.entry_price) / t.entry_price) * 100
            pc = "bold green" if pnl > 0 else "bold red"

            if t.status == "ENTRY":
                st_style, st_icon = "yellow", "🟠"
            elif t.status == "GEAR_1":
                st_style, st_icon = "green", "✅"
            elif t.status == "GEAR_2":
                st_style, st_icon = "bold green", "🚀"
            elif t.status == "GEAR_3":
                st_style, st_icon = "bold magenta", "🌙"
            else:
                st_style, st_icon = "dim", "❌"

            t_trade.add_row(
                f"🎯 {s.replace('/USDT','')}",
                f"[{st_style}]{st_icon} {t.status}[/]",
                f"{t.entry_price:.5f}",
                f"{t.current_price:.5f}",
                f"[{pc}]{pnl:.2f}%[/]",
                f"{t.tp1:.5f}",
                f"{t.tp2:.5f}",
                f"{t.tp3:.5f}",
                f"{t.sl:.5f}",
            )

    layout["middle"].update(Panel(t_trade, border_style="bright_red"))

    t_log = Table(title="📜 TRADE STORY", expand=True, box=box.MINIMAL, show_header=False)
    all_logs: List[str] = []
    for t in state.active_trades.values():
        for l in t.logs[-3:]:
            all_logs.append(f"📌 {t.symbol}: {l}")

    if not all_logs:
        t_log.add_row("🧘 Waiting for pump ignition...")
    else:
        for l in all_logs[-6:]:
            t_log.add_row(l)

    layout["bottom"].update(Panel(t_log, border_style="magenta"))
    return layout


# ---------- MAIN ----------

async def main():
    init_db()

    spot = ccxt.mexc({"enableRateLimit": True, "options": {"defaultType": "spot"}})
    bybit = None

    try:
        await spot.load_markets()
    except Exception as e:
        logging.exception(f"spot.load_markets failed: {e}")

    try:
        bybit = ccxt.bybit({"enableRateLimit": True, "options": {"defaultType": "swap"}})
        await bybit.load_markets()
    except Exception as e:
        logging.exception(f"bybit.load_markets failed: {e}")
        bybit = None

    with Live(generate_dashboard(), refresh_per_second=4, screen=True) as live:
        while True:
            state.last_scan_time = datetime.now().strftime("%H:%M:%S")
            try:
                await process_market(spot, bybit)
            except Exception as e:
                logging.exception(f"process_market error: {e}")
                state.status = f"Error: {e}"
            for i in range(cfg.scan_interval_seconds, 0, -1):
                state.status = f"Scanning... {i}s"
                live.update(generate_dashboard())
                await asyncio.sleep(1)

    if bybit is not None:
        await bybit.close()
    await spot.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Exiting...")
