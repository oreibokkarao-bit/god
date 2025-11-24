import asyncio
import sqlite3
import logging
import time
import statistics
from collections import deque
from datetime import datetime
from typing import List, Dict, Any, Optional

import ccxt.async_support as ccxt
from rich import box
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

# ============================================================
#   MEXC ORCHESTRATOR V2.3
#   Spot Pump Anticipator + Early Breakout Engine + Bybit OI/CVD
# ============================================================

# --- CONFIGURATION ---
MIN_24H_VOLUME_USDT = 50_000
MAX_24H_VOLUME_USDT = 50_000_000
MIN_PRICE_CHANGE_24H = 5.0
MAX_SPREAD_PCT = 2.0
MIN_BOOK_DEPTH_USDT = 10_000
BASELINE_PERIOD = 90  # 5m and 1h lookback length

# OTR / manipulation proxy (percentage of "big trades" in last 60s)
MAX_OTR_THRESHOLD = 60.0

SCAN_INTERVAL_SECONDS = 20

# Pump Anticipator thresholds
PRE_IGNITION_THRESHOLD = 45     # "Primed" but not yet full signal
ALERT_THRESHOLD = 70            # Full PUMP_SCORE entry threshold

DB_NAME = "mexc_orchestrator.sqlite"

console = Console()


# --- STATE MANAGEMENT ---
class ScreenerState:
    def __init__(self):
        self.status = "Initializing Pump Anticipator V2.3..."
        self.last_scan_time = "N/A"
        self.candidates: List[Dict] = []
        self.alerts = deque(maxlen=10)
        self.active_trades: Dict[str, "ActiveTrade"] = {}
        # Futures intelligence cache: per-spot-symbol insights
        self.futures_insights: Dict[str, Dict[str, Any]] = {}


state = ScreenerState()


# --- THE BRAIN: DYNAMIC TP ENGINE (Enhanced) ---
class ActiveTrade:
    def __init__(self, symbol, entry_price, volatility_unit, start_time):
        self.symbol = symbol
        self.entry_price = entry_price
        self.entry_time = start_time
        self.volatility = volatility_unit  # Sigma (σ)
        self.current_price = entry_price
        self.highest_price = entry_price
        self.status = "ENTRY"

        # --- DYNAMIC TARGETS (GEARS) ---
        # TP1: Safety Lock (Quick Scalp)
        self.tp1 = entry_price + (1.0 * volatility_unit)
        # TP2: Trend Rider (The Meat)
        self.tp2 = entry_price + (2.0 * volatility_unit)
        # TP3: Moonbag (The Home Run)
        self.tp3 = entry_price + (4.0 * volatility_unit)

        # Stop Loss (Volatility Adjusted)
        self.sl = entry_price - (2.0 * volatility_unit)

        self.logs: List[str] = []
        self.pnl_locked = 0.0

    def update(self, current_price, vol_z_5m, rsi, ask_wall_price=None):
        self.current_price = current_price
        if current_price > self.highest_price:
            self.highest_price = current_price

        # --- INTELLIGENT TARGET ADJUSTMENT (Wall Front-Run) ---
        if ask_wall_price:
            if self.status == "ENTRY" and self.tp1 > ask_wall_price > current_price:
                self.tp1 = ask_wall_price * 0.999
                self.log_event(f"⚠️ TP1 adjusted below wall: {self.tp1:.5f}")
            elif self.status == "GEAR_1" and self.tp2 > ask_wall_price > current_price:
                self.tp2 = ask_wall_price * 0.999
                self.log_event(f"⚠️ TP2 adjusted below wall: {self.tp2:.5f}")

        # --- GEAR SHIFT LOGIC ---

        # GEAR 1: SAFETY LOCK (+1σ)
        if self.status == "ENTRY" and current_price >= self.tp1:
            self.status = "GEAR_1"
            self.sl = self.entry_price * 1.002  # Breakeven + Fees
            self.pnl_locked += (self.tp1 - self.entry_price) * 0.5  # Realize 50%
            self.log_event("✅ TP1 HIT: Sold 50% | SL -> Breakeven")
            update_trade_db(self, "TP1_HIT")

        # GEAR 2: TREND RIDER (+2σ)
        elif self.status == "GEAR_1" and current_price >= self.tp2:
            self.status = "GEAR_2"
            self.pnl_locked += (self.tp2 - self.entry_price) * 0.3  # Realize 30%
            self.log_event("✅ TP2 HIT: Sold 30% | Trailing Active")
            update_trade_db(self, "TP2_HIT")

        # GEAR 3: MOONBAG (+4σ)
        elif self.status == "GEAR_2" and current_price >= self.tp3:
            self.status = "GEAR_3"
            self.log_event("🚀 TP3 CROSSED: Moonbag Mode | Panic Sensor ON")
            update_trade_db(self, "TP3_CROSS")

        # --- TRAILING STOP LOGIC ---
        if self.status in ["GEAR_2", "GEAR_3"]:
            # Tighten trail if volume disappears
            trail_dist = 1.5 * self.volatility
            if vol_z_5m < 0:  # Volume dying
                trail_dist = 1.0 * self.volatility

            new_sl = self.highest_price - trail_dist
            if new_sl > self.sl:
                self.sl = new_sl

        # --- EXIT ---
        if current_price <= self.sl:
            self.status = "CLOSED"
            reason = "STOP_LOSS" if self.sl < self.entry_price else "TRAILING_PROFIT"
            self.log_event(f"🛑 TRADE CLOSED: {reason} @ {current_price}")
            update_trade_db(self, "CLOSED")
            return False

        # --- REVERSAL GUARD (Panic Sensor) ---
        if self.status == "GEAR_3":
            # "Climax Pattern": Volume > 5x AND Price Stalling OR RSI > 90
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


# --- DATABASE SETUP ---
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


# --- ANALYTICS ENGINE ---
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
        if spread_pct > MAX_SPREAD_PCT:
            return {"failed": True, "wall_price": None, "obi": 0.0, "depth_usd": 0.0}

        bid_depth_usd = sum(p * q for p, q in bids if p >= mid * 0.98)
        ask_depth_usd = sum(p * q for p, q in asks if p <= mid * 1.02)
        depth_usd = bid_depth_usd + ask_depth_usd
        if depth_usd < MIN_BOOK_DEPTH_USDT:
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
    Pump Anticipator metrics (SPOT) + Early Breakout Engine inputs:
        - z_5m (volume z-score, 5m)
        - z_60m (volume z-score, 1h)
        - sigma (1h volatility)
        - rsi (1h)
        - otr (manipulation proxy: big-trade share in last 60s)
        - navd_1m, navd_5m
        - compression
        - above_vwap
        - range_expansion (5m bar vs 20-bar avg)
        - micro_break (close breaks last 12-bar high)
        - velocity_3 (3-bar price change)
    """
    try:
        t_5m = exchange.fetch_ohlcv(symbol, "5m", limit=BASELINE_PERIOD + 5)
        t_1h = exchange.fetch_ohlcv(symbol, "1h", limit=BASELINE_PERIOD + 5)
        t_trades = exchange.fetch_trades(symbol, limit=200)

        data_5m, data_1h, trades = await asyncio.gather(t_5m, t_1h, t_trades)

        if len(data_5m) < BASELINE_PERIOD or len(data_1h) < 30:
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

        # OTR: % of big trades in last 60s
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

        # Compression (VCP-style)
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

        # --- Early Breakout Engine inputs ---
        # Range expansion: last 5m bar vs avg of previous 20 bars
        ranges_5m = [x[2] - x[3] for x in data_5m]
        if len(ranges_5m) > 21:
            avg_range20 = statistics.mean(ranges_5m[-21:-1])
            range_expansion = ranges_5m[-1] / avg_range20 if avg_range20 > 0 else 1.0
        else:
            range_expansion = 1.0

        # Micro-break of last 12 bars high
        if len(closes_5m) > 13:
            recent_high = max(closes_5m[-13:-1])
            micro_break = 1.0 if last_price > recent_high else 0.0
        else:
            micro_break = 0.0

        # 3-bar velocity (relative)
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


# --- BYBIT SIDE-CAR HELPERS ---

def map_mexc_to_bybit_perp(spot_symbol: str, bybit) -> Optional[str]:
    """
    Match MEXC spot symbol (e.g. 'TNSR/USDT') to Bybit perp
    (e.g. 'TNSR/USDT:USDT') using base/quote + swap=True.
    """
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
    """
    Optional side-car: Bybit OI + NAVD information for overlapping symbols.
    Never raises; returns None on any problem.
    """
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


def classify_oi_cvd_divergence(
    spot_navd_5m: float,
    fut_navd_5m: float,
    oi_5m_change: float,
) -> (str, int, List[str]):
    """
    Returns: (signal_label, score_adjust, extra_tags)
    """
    label = "NEUTRAL"
    bump = 0
    tags: List[str] = []

    # Smart-long accumulation
    if spot_navd_5m > 0.2 and oi_5m_change > 3 and fut_navd_5m > 0:
        label = "SMART_LONGS"
        bump = +15
        tags.append("🧲SMART_LONGS")

    # Fake pump: no OI/CVD support
    elif spot_navd_5m <= 0 and oi_5m_change <= 0:
        label = "FAKE_PUMP"
        bump = -30
        tags.append("⛔FAKE_PUMP")

    # Squeeze risk: OI mooning, CVD meh
    elif oi_5m_change > 10 and abs(spot_navd_5m) < 0.1:
        label = "SQUEEZE_RISK"
        bump = -10
        tags.append("🧨SQUEEZE")

    # Distribution: spot CVD down, OI up
    elif spot_navd_5m < -0.2 and oi_5m_change > 3:
        label = "DISTRIBUTION"
        bump = -20
        tags.append("📉DISTRIB")

    return label, bump, tags


def compute_pump_score(
    symbol: str,
    price: float,
    base_change_24h: float,
    metrics: Dict[str, Any],
    liq_info: Optional[Dict[str, Any]],
    fut_insight: Optional[Dict[str, Any]] = None,
) -> (int, List[str], str):
    """
    PUMP_SCORE (0-100) from:
        - Early Breakout Engine (range/velocity/micro-break)
        - Accumulation (NAVD + compression + VWAP)
        - Ignition (volume z-scores)
        - Quality (liquidity, OBI, OTR)
        - Optional Bybit OI/CVD divergence (dual-listed only)
    Returns: (score, tags, fut_label)
    """
    score = 0
    tags: List[str] = [f"{base_change_24h:.1f}%"]
    fut_label = "NEUTRAL"

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

    # QUALITY GATES
    if otr > MAX_OTR_THRESHOLD:
        tags.append("⚠️WEIRD_FLOW")
        return 0, tags, fut_label

    if depth_usd < MIN_BOOK_DEPTH_USDT * 1.5:
        tags.append("💧THIN")
    else:
        score += 5

    if obi > 0.1:
        score += 5
        tags.append("OBI+")
    elif obi < -0.1:
        score -= 5
        tags.append("OBI-")

    # --- EARLY BREAKOUT ENGINE (speed-first) ---
    early_score = 0
    # Range expansion: big candle relative to recent
    if range_expansion > 3.0:
        early_score += 10
    if range_expansion > 4.0:
        early_score += 5
    # Micro-break of recent highs
    if micro_break >= 0.5:
        early_score += 10
    # 3-bar velocity
    if velocity_3 > 0.05:
        early_score += 10
    elif velocity_3 > 0.03:
        early_score += 5

    # Require at least non-bearish CVD for early long bias
    if early_score >= 15 and navd_5m > -0.1:
        score += early_score
        tags.append("EARLY_BREAKOUT")

    # --- ACCUMULATION BLOCK ---
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

    # --- IGNITION BLOCK ---
    if z_5m > 1.5:
        ign = 10 + int(max(0.0, z_5m - 1.5) * 8.0)
        ign = min(35, ign)
        score += ign
        tags.append(f"Z5:{z_5m:.1f}")

    if z_60m > 2.0:
        score += 5
        tags.append("Z60+")

    # --- BYBIT SIDE-CAR ---
    if fut_insight is not None:
        label, bump, fut_tags = classify_oi_cvd_divergence(
            spot_navd_5m=navd_5m,
            fut_navd_5m=fut_insight.get("fut_navd_5m", 0.0),
            oi_5m_change=fut_insight.get("oi_5m_change", 0.0),
        )
        fut_label = label
        score += bump
        tags.extend(fut_tags)

    score = max(0, min(score, 100))

    if score >= ALERT_THRESHOLD:
        tags.append("🚀IGNITION")
    elif score >= PRE_IGNITION_THRESHOLD:
        tags.append("👀PRIMED")

    return score, tags, fut_label


# --- PIPELINE ---
async def process_market(spot_exchange, bybit_exchange=None):
    state.status = "Fetching Tickers..."
    try:
        tickers = await spot_exchange.fetch_tickers()
    except Exception as e:
        logging.exception(f"fetch_tickers failed: {e}")
        return

    candidates: List[Dict[str, Any]] = []
    for s, t in tickers.items():
        if "/USDT" not in s or "3L" in s or "3S" in s:
            continue
        try:
            vol = float(t["quoteVolume"])
            chg = float(t["percentage"])
            price = float(t["last"])
        except Exception:
            continue

        if vol < MIN_24H_VOLUME_USDT or vol > MAX_24H_VOLUME_USDT:
            continue
        if chg < MIN_PRICE_CHANGE_24H:
            continue

        candidates.append({"symbol": s, "price": price, "vol": vol, "chg": chg})

    candidates.sort(key=lambda x: x["chg"], reverse=True)
    top_candidates = candidates[:25]

    state.candidates = []
    analyzed_batch: List[Dict[str, Any]] = []

    symbols_to_check = set([c["symbol"] for c in top_candidates] + list(state.active_trades.keys()))
    state.status = f"Vetting {len(symbols_to_check)} Candidates..."

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
            pump_score, tags, fut_label = compute_pump_score(
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
            }
            analyzed_batch.append(final)

            if pump_score >= ALERT_THRESHOLD and not has_been_alerted(symbol):
                record_alert(final)
                trade = ActiveTrade(
                    symbol,
                    price,
                    m.get("sigma", price * 0.02),
                    datetime.now().strftime("%H:%M:%S"),
                )
                state.active_trades[symbol] = trade
                update_trade_db(trade, "OPEN_TRADE")

        await asyncio.sleep(spot_exchange.rateLimit / 1000.0)

    analyzed_batch.sort(key=lambda x: x["score"], reverse=True)
    state.candidates = analyzed_batch


# --- VISUAL DASHBOARD ---
def generate_dashboard() -> Layout:
    layout = Layout()
    layout.split(
        Layout(name="header", size=3),
        Layout(name="top", ratio=1),
        Layout(name="middle", ratio=1),
        Layout(name="bottom", size=8),
    )

    # HEADER
    layout["header"].update(
        Panel(
            f"🦅🔥 MEXC ORCHESTRATOR V2.3 | EARLY BREAKOUT + BYBIT OI/CVD | Status: {state.status}",
            style="bold white on dark_red",
        )
    )

    # PUMP CANDIDATES
    t_scan = Table(
        title="🚨 V2.3 EARLY BREAKOUT RADAR 🚨",
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

    for c in state.candidates:
        score = c["score"]
        otr = c["otr"]
        z5 = c["z_5m"]
        tags = c["tags"]
        fut_signal = c.get("fut_signal", "NEUTRAL")

        # SCORE COLOR
        if score >= ALERT_THRESHOLD:
            score_style = "bold white on green"
            score_icon = "🚀"
        elif score >= PRE_IGNITION_THRESHOLD:
            score_style = "bold black on yellow"
            score_icon = "👀"
        else:
            score_style = "dim"
            score_icon = "❄️"

        # OTR COLOR
        if otr < 10:
            otr_style = "green"
        elif otr > MAX_OTR_THRESHOLD:
            otr_style = "bold red"
            otr = min(otr, 999)
        else:
            otr_style = "yellow"

        # Z5 COLOR
        if z5 >= 2:
            z5_style = "bold green"
        elif z5 <= -2:
            z5_style = "red"
        else:
            z5_style = "yellow"

        # Futures signal color
        if fut_signal == "SMART_LONGS":
            fut_style = "bold green"
            fut_text = "🧲LONGS"
        elif fut_signal == "FAKE_PUMP":
            fut_style = "bold red"
            fut_text = "⛔FAKE"
        elif fut_signal == "SQUEEZE_RISK":
            fut_style = "yellow"
            fut_text = "🧨SQZ"
        elif fut_signal == "DISTRIBUTION":
            fut_style = "red"
            fut_text = "📉DIST"
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

        # BABY VIEW
        has_thin = any("THIN" in t for t in tags)
        has_primed = any("PRIMED" in t for t in tags)
        has_ignition = any("IGNITION" in t for t in tags)
        has_early = any("EARLY_BREAKOUT" in t for t in tags)
        has_navd_up = any("NAVD:+0." in t or "NAVD:+1" in t or "NAVD:+0" in t for t in tags)
        has_vcp = any("VCP" in t for t in tags)
        has_vwap_up = any("VWAP" in t for t in tags)

        if has_ignition or score >= ALERT_THRESHOLD:
            baby = "🚀 Coin is pumping now. Bot wants to ride."
        elif has_early and score >= PRE_IGNITION_THRESHOLD:
            baby = "⚡ Coin just started breakout. Early stage, watch closely."
        elif has_early:
            baby = "⚡ Early breakout signs. Still forming."
        elif has_primed or score >= PRE_IGNITION_THRESHOLD:
            baby = "👀 Coin heating up. Watch chart closely."
        elif score >= 20 and z5 >= 0:
            baby = "🙂 Mildly bullish, could wake up soon."
        elif score >= 20 and z5 < 0:
            baby = "😴 Quiet but building energy."
        else:
            baby = "❄️ Boring for now. Ignore."

        risk_bits = []
        if has_thin:
            risk_bits.append("⚠️ Thin book, easy to rug.")
        if otr > MAX_OTR_THRESHOLD:
            risk_bits.append("🧨 Weird flow, could be fake pump.")
        elif otr > 20:
            risk_bits.append("🟡 Some weird flow. Be careful.")

        if has_navd_up and has_vcp and has_vwap_up and score >= PRE_IGNITION_THRESHOLD:
            baby = "🔥 Strong hands buying quietly. May explode soon."
        elif has_navd_up and has_vcp and score < PRE_IGNITION_THRESHOLD:
            baby = "🌱 Smart money nibbling, still early."

        if fut_signal == "SMART_LONGS":
            risk_bits.append("🧲 Futures also long. Good confluence.")
        elif fut_signal == "FAKE_PUMP":
            risk_bits.append("⛔ Futures say fake pump. Avoid.")
        elif fut_signal == "SQUEEZE_RISK":
            risk_bits.append("🧨 High squeeze risk. Take quick profits.")
        elif fut_signal == "DISTRIBUTION":
            risk_bits.append("📉 Looks like distribution. Careful.")

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

    # ACTIVE TRADES
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

    # LOG PANEL
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


# --- MAIN ---
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
            for i in range(SCAN_INTERVAL_SECONDS, 0, -1):
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
