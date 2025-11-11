#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Parallel V-Reversal Trade Monitor — Public-Only
Version: v2.5 (Gradient Spark + Sentiment-Colored Guidance)

• Jitter-free Rich table (fixed column widths, gentle refresh)
• V-Rev column: fixed-width 5-block Gradient Spark
   - Bottom V (support):  red→green background + moving green spark
   - Roof   V (top):      green→red background + moving red spark
• Guidance column: BULLISH text in GREEN, BEARISH in RED, neutral in dim/yellow
• Mac sound alert (afplay) ON by default; optional TTS ("say")
"""

import asyncio, contextlib, time, math, logging, json, sqlite3, subprocess, os, urllib.parse
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, Tuple, List
from pathlib import Path
from collections import deque

import yaml
import aiohttp
import numpy as np
import pandas as pd
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich import box
from rich.panel import Panel
from rich.align import Align
from rich.layout import Layout

# ---------------- Logging / Console ----------------
console = Console()
log = logging.getLogger("vrev_monitor")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)

HERE = Path(__file__).resolve().parent
CFG = HERE / "config.yml"


# ---------------------------------------------------
# Config
# ---------------------------------------------------
def load_cfg() -> dict:
    """Load YAML config, but keep safe defaults if missing keys."""
    defaults = {
        "app": {
            "use_uvloop": False,
            "refresh_secs": 5,
            "db_path": str(HERE / "monitor_state.db"),
            "watchlist": [],  # must be provided by user
        },
        "features": {
            "weights": {
                "cvd": 0.35,
                "obi": 0.25,
                "momentum": 0.20,
                "vol": 0.15,
                "oi": 0.05,
            }
        },
        "tuning": {"adx_threshold": 25.0, "chop_threshold": 61.8},
        "orderbook": {"depth_limit": 50},
        "v_reversal": {
            "trigger_z": 0.9,
            "weights": {"oi": 0.35, "funding": 0.15, "cvd": 0.30, "orderbook": 0.20},
        },
        "alerts": {
            "enabled": True,
            "cooldown_secs": 90,
            "speak": False,
            "tts_voice": "Alex",
            "sound_enabled": True,  # ON by default
            "sound_path": "/System/Library/Sounds/Glass.aiff",
        },
        "backups": {
            "use_binance_oi_fallback": True,
            "use_binance_liq_ws": True,
            "liq_window_secs": 600,
        },
        "consensus": {"dual_exchange": False},
        "trading": {"global_side": "—", "positions": {}},
    }
    if CFG.exists():
        with open(CFG, "r", encoding="utf-8") as f:
            user = yaml.safe_load(f) or {}

        def deepmerge(a, b):
            for k, v in b.items():
                if isinstance(v, dict) and isinstance(a.get(k), dict):
                    deepmerge(a[k], v)
                else:
                    a[k] = v
            return a

        return deepmerge(defaults, user)
    return defaults


# ---------------------------------------------------
# TA helpers
# ---------------------------------------------------
def zscore(arr: np.ndarray, lookback: int) -> float:
    if len(arr) < lookback:
        return 0.0
    s = arr[-lookback:]
    return float((s[-1] - s.mean()) / (s.std() + 1e-9))


def roc(arr: np.ndarray, lookback: int) -> float:
    if len(arr) <= lookback:
        return 0.0
    prev, cur = arr[-lookback - 1], arr[-1]
    return float((cur - prev) / max(abs(prev), 1e-9))


def chop_index(
    high: np.ndarray, low: np.ndarray, close: np.ndarray, length: int = 14
) -> float:
    if len(close) < length + 1:
        return 100.0
    tr = []
    for i in range(-length, 0):
        prev = close[i - 1]
        tr.append(max(high[i] - low[i], abs(high[i] - prev), abs(low[i] - prev)))
    denom = max(high[-length:].max() - low[-length:].min(), 1e-9)
    return float(100.0 * math.log10(max(sum(tr), 1e-12) / denom) / math.log10(length))


def adx(
    high: np.ndarray, low: np.ndarray, close: np.ndarray, length: int = 14
) -> float:
    if len(close) < length + 2:
        return 0.0
    up = high[1:] - high[:-1]
    dn = low[:-1] - low[1:]
    plus_dm = np.where((up > dn) & (up > 0), up, 0.0)
    minus_dm = np.where((dn > up) & (dn > 0), dn, 0.0)
    tr = np.maximum.reduce(
        [
            high[1:] - low[1:],
            np.abs(high[1:] - close[:-1]),
            np.abs(low[1:] - close[:-1]),
        ]
    )
    tr_sum = pd.Series(tr).rolling(length).sum().to_numpy()
    pdi = (
        100.0
        * pd.Series(plus_dm).rolling(length).sum().to_numpy()
        / np.maximum(tr_sum, 1e-9)
    )
    mdi = (
        100.0
        * pd.Series(minus_dm).rolling(length).sum().to_numpy()
        / np.maximum(tr_sum, 1e-9)
    )
    dx = 100.0 * np.abs(pdi - mdi) / np.maximum(pdi + mdi, 1e-9)
    val = float(pd.Series(dx).rolling(length).mean().to_numpy()[-1])
    return 0.0 if math.isnan(val) else val


def cvd_from_ohlcv(df: pd.DataFrame, lookback: int = 50) -> float:
    if df.empty or len(df) < lookback:
        return 0.0
    signed = (
        np.sign(df["close"].to_numpy() - df["open"].to_numpy())
        * df["volume"].to_numpy()
    )
    cum = signed.cumsum()
    return zscore(cum, lookback=lookback)


# ---------------------------------------------------
# HTTP data (Public APIs)
# ---------------------------------------------------
BYBIT = "https://api.bybit.com"
BINANCE = "https://api.binance.com"
BINANCE_FUTURES = "https://fapi.binance.com"


async def http_json(
    session: aiohttp.ClientSession,
    url: str,
    params: dict | None = None,
    retries: int = 3,
) -> dict:
    for i in range(retries):
        try:
            async with session.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                r.raise_for_status()
                return json.loads(await r.text())
        except Exception:
            await asyncio.sleep(0.3 * (i + 1))
    return {}


async def bybit_klines_5m(session, symbol: str, limit: int = 200) -> pd.DataFrame:
    j = await http_json(
        session,
        f"{BYBIT}/v5/market/kline",
        {"category": "linear", "symbol": symbol, "interval": "5", "limit": str(limit)},
    )
    try:
        arr = j["result"]["list"]
        df = pd.DataFrame(
            arr, columns=["start", "open", "high", "low", "close", "volume", "turnover"]
        )
        for c in ["open", "high", "low", "close", "volume", "turnover"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df.sort_values("start", inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df
    except Exception:
        return pd.DataFrame()


async def bybit_orderbook_obi(session, symbol: str, limit: int = 50) -> float:
    j = await http_json(
        session,
        f"{BYBIT}/v5/market/orderbook",
        {"category": "linear", "symbol": symbol, "limit": str(limit)},
    )
    try:
        b = j["result"]["b"]
        a = j["result"]["a"]
        bid = sum(float(x[1]) for x in b)
        ask = sum(float(x[1]) for x in a)
        den = max(bid + ask, 1e-9)
        return float((bid - ask) / den)
    except Exception:
        return 0.0


async def bybit_open_interest_delta(session, symbol: str) -> float:
    j = await http_json(
        session,
        f"{BYBIT}/v5/market/open-interest",
        {"category": "linear", "symbol": symbol, "intervalTime": "5", "limit": "2"},
    )
    try:
        lst = j["result"]["list"]
        if len(lst) >= 2:
            a = float(lst[-2]["openInterest"])
            b = float(lst[-1]["openInterest"])
            return float((b - a) / max(a, 1e-9))
    except Exception:
        pass
    return 0.0


async def bybit_funding(session, symbol: str) -> Tuple[float, float]:
    j = await http_json(
        session,
        f"{BYBIT}/v5/market/funding/history",
        {"category": "linear", "symbol": symbol, "limit": "50"},
    )
    try:
        vals = [float(x["fundingRate"]) for x in j["result"]["list"]]
        cur = vals[-1]
        if len(vals) >= 24 * 7:
            mean7 = float(np.mean(vals[-24 * 7 :]))
            std7 = float(np.std(vals[-24 * 7 :]))
        else:
            mean7 = float(np.mean(vals))
            std7 = float(np.std(vals))
        z = (cur - mean7) / (std7 + 1e-12)
        return cur, float(z)  # fixed
    except Exception:
        return 0.0, 0.0


async def bybit_mark_index(session, symbol: str) -> Tuple[float, float]:
    j = await http_json(
        session, f"{BYBIT}/v5/market/tickers", {"category": "linear", "symbol": symbol}
    )
    try:
        row = j["result"]["list"][0]
        mark = float(row["markPrice"])
        idx = float(row.get("indexPrice", mark))
        prem = (mark - idx) / (idx + 1e-12)
        return mark, prem
    except Exception:
        return 0.0, 0.0


async def binance_mid_premium(session, symbol: str) -> float:
    j1 = await http_json(session, f"{BINANCE}/api/v3/ticker/price", {"symbol": symbol})
    j2 = await http_json(
        session, f"{BINANCE}/api/v3/ticker/bookTicker", {"symbol": symbol}
    )
    try:
        px = float(j1["price"])
        mid = 0.5 * (float(j2["bidPrice"]) + float(j2["askPrice"]))
        return (mid - px) / (px + 1e-12)
    except Exception:
        return 0.0


# ---- Binance OI fallback ----
_BINANCE_OI_CACHE: Dict[str, float] = {}  # last absolute OI per symbol (for delta)


async def binance_open_interest(
    session: aiohttp.ClientSession, symbol: str
) -> Optional[float]:
    j = await http_json(
        session, f"{BINANCE_FUTURES}/fapi/v1/openInterest", {"symbol": symbol}
    )
    try:
        return float(j["openInterest"])
    except Exception:
        return None


async def oi_delta_with_fallback(
    session: aiohttp.ClientSession,
    symbol: str,
    bybit_oi_delta: float,
    use_binance: bool,
) -> float:
    if abs(bybit_oi_delta) > 1e-9 or not use_binance:
        return bybit_oi_delta
    cur = await binance_open_interest(session, symbol)
    prev = _BINANCE_OI_CACHE.get(symbol)
    _BINANCE_OI_CACHE[symbol] = cur if cur is not None else prev
    if cur is None or prev is None or prev == 0.0:
        return 0.0
    return float((cur - prev) / max(abs(prev), 1e-9))


# ---------------------------------------------------
# Async DB (WAL)
# ---------------------------------------------------
@dataclass
class DBEvent:
    symbol: str
    kind: str
    ts_ms: int
    payload_json: str


class AsyncDB:
    def __init__(self, path: str, maxsize: int = 10000):
        self.path = path
        self.q: asyncio.Queue[DBEvent] = asyncio.Queue(maxsize=maxsize)
        self._task: Optional[asyncio.Task] = None

    def _init(self):
        con = sqlite3.connect(self.path)
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute(
            """
        CREATE TABLE IF NOT EXISTS v_reversal_events(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          score REAL NOT NULL,
          z_norm REAL NOT NULL,
          oi REAL, funding REAL, cvd REAL, ob REAL,
          details TEXT
        );"""
        )
        con.execute(
            """
        CREATE TABLE IF NOT EXISTS alerts_log(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          kind TEXT NOT NULL,
          payload_json TEXT
        );"""
        )
        con.commit()
        con.close()

    async def start(self):
        self._init()
        self._task = asyncio.create_task(self._writer())

    async def _writer(self):
        loop = asyncio.get_event_loop()
        while True:
            ev = await self.q.get()

            def _write(e: DBEvent):
                con = sqlite3.connect(self.path)
                con.execute("PRAGMA journal_mode=WAL;")
                if e.kind == "vrev":
                    try:
                        d = json.loads(e.payload_json)
                    except Exception:
                        try:
                            d = eval(e.payload_json)
                        except Exception:
                            d = {}
                    con.execute(
                        """INSERT INTO v_reversal_events(ts_ms,symbol,score,z_norm,oi,funding,cvd,ob,details)
                                   VALUES(?,?,?,?,?,?,?,?,?)""",
                        (
                            e.ts_ms,
                            e.symbol,
                            float(d.get("score", 0.0)),
                            float(d.get("z_norm", 0.0)),
                            float(d.get("oi", 0.0)),
                            float(d.get("funding", 0.0)),
                            float(d.get("cvd", 0.0)),
                            float(d.get("ob", 0.0)),
                            e.payload_json,
                        ),
                    )
                con.execute(
                    "INSERT INTO alerts_log(ts_ms,symbol,kind,payload_json) VALUES(?,?,?,?)",
                    (e.ts_ms, e.symbol, e.kind, e.payload_json),
                )
                con.commit()
                con.close()

            await loop.run_in_executor(None, _write, ev)

    async def log_vreversal(self, symbol: str, payload: Dict[str, Any]):
        ts = int(time.time() * 1000)
        await self.q.put(
            DBEvent(
                symbol=symbol, kind="vrev", ts_ms=ts, payload_json=json.dumps(payload)
            )
        )


# ---------------------------------------------------
# State + Visual Helpers + Alerts / Judge
# ---------------------------------------------------
@dataclass
class State:
    side: str = "—"
    entry: Optional[float] = None
    price: float = float("nan")
    pnl_pct: float = float("nan")
    score: float = 0.0
    guidance: str = "…"
    source: str = "Bybit"
    vrev_status: str = "—"  # forming | genuine | fading | —
    vrev_conf: float = 0.0  # 0..1
    features: Dict[str, float] = field(
        default_factory=lambda: dict(
            cvd=0.0, obi=0.0, mom=0.0, vol=0.0, oi_d=0.0, liq=0.0
        )
    )


# thresholds
VOL_SPIKE_Z = 1.0
CVD_STRONG_Z = 0.75
OBI_STRONG = 0.25
MOM_STRONG = 0.003
SCORE_GOOD = 0.15
SCORE_BAD = -0.15


def color_num(v: float, digits: int = 3, pct: bool = False) -> str:
    if v is None or v != v:
        return "—"
    s = f"{v:+.{digits}f}" + ("%" if pct else "")
    if v > 0:
        return f"[bold green]{s}[/]"
    if v < 0:
        return f"[bold red]{s}[/]"
    return s


def badge_cvd(z: float) -> str:
    if z >= CVD_STRONG_Z:
        return f"🐂 {color_num(z)}"
    if z <= -CVD_STRONG_Z:
        return f"🐻 {color_num(z)}"
    return color_num(z)


def badge_vol(z: float) -> str:
    base = color_num(z)
    return f"{base} ⚡" if z >= VOL_SPIKE_Z else base


def badge_obi(x: float) -> str:
    tip = "🟢" if x >= OBI_STRONG else ("🔴" if x <= -OBI_STRONG else "⚪")
    return f"{tip} {color_num(x)}"


# Momentum emoji swap: 👆 (up), 🔻 (down), ↔️ (flat)
def badge_mom(x: float) -> str:
    tip = "👆" if x >= MOM_STRONG else ("🔻" if x <= -MOM_STRONG else "↔️")
    return f"{tip} {color_num(x*100, digits=2, pct=True)}"


# --- Guidance colorizer: bullish=green, bearish=red, neutral=dim/amber ---
def colorize_guidance(msg: str) -> str:
    bearish_keys = (
        "de-risk",
        "reduce",
        "tighten",
        "exit/flip",
        "exit",
        "flip against",
        "warning",
        "risk",
    )
    bullish_keys = (
        "ok / add",
        "add on pullback",
        "trend intact",
        "accumulate",
        "recovering",
        "consider add",
    )

    m = msg.lower()
    is_bearish = any(k in m for k in bearish_keys) or "📉" in msg
    is_bullish = any(k in m for k in bullish_keys) or "📈" in msg

    if is_bearish:
        return f"[bold red]{msg}[/]"
    if is_bullish:
        return f"[bold green]{msg}[/]"
    if "forming" in m or "watch" in m:
        return f"[bold yellow]{msg}[/]"
    if "no edge" in m:
        return f"[dim]{msg}[/]"
    return msg


# --- Header (tiny animation, no jitter) ---
ANIM_FRAMES_POS = ["🚴‍♂️", "🚴‍♂️💨", "🚴‍♂️💨💨"]


def anim_header(tick: int, alerts_banner: str) -> Panel:
    frame = ANIM_FRAMES_POS[tick % len(ANIM_FRAMES_POS)]
    text = f"{frame}  [b]Parallel V-Reversal Trade Monitor[/b]  {alerts_banner}"
    return Panel(Align.center(text), padding=(0, 1), border_style="grey50")


@dataclass
class VRevAlert:
    symbol: str
    ts_ms: int
    z_norm: float
    confidence: float
    status: str  # forming | genuine | fading


class AlertManager:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.active: Dict[str, VRevAlert] = {}
        self.last_ts: Dict[str, int] = {}

    def _cooldown_ok(self, sym: str) -> bool:
        cd = int(self.cfg.get("alerts", {}).get("cooldown_secs", 90))
        now = int(time.time())
        return (sym not in self.last_ts) or (now - self.last_ts[sym] >= cd)

    def _beep(self):
        cfg_alerts = self.cfg.get("alerts", {})
        if not cfg_alerts.get("sound_enabled", True):
            return
        p = cfg_alerts.get("sound_path") or "/System/Library/Sounds/Glass.aiff"
        if not os.path.exists(p):
            p = "/System/Library/Sounds/Glass.aiff"
        with contextlib.suppress(Exception):
            subprocess.Popen(
                ["afplay", p], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )

    def _speak(self, text: str):
        if not self.cfg.get("alerts", {}).get("speak", False):
            return
        voice = self.cfg.get("alerts", {}).get("tts_voice", "Alex")
        with contextlib.suppress(Exception):
            subprocess.Popen(
                ["say", "-v", voice, text],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

    def trigger(self, sym: str, z_norm: float, confidence: float, status: str):
        if not self.cfg.get("alerts", {}).get("enabled", True):
            return
        if not self._cooldown_ok(sym):
            return
        self.last_ts[sym] = int(time.time())
        self.active[sym] = VRevAlert(
            symbol=sym,
            ts_ms=int(time.time() * 1000),
            z_norm=z_norm,
            confidence=max(0.0, min(1.0, confidence)),
            status=status,
        )
        self._beep()
        self._speak(
            f"{sym} V reversal {status}. Confidence {int(confidence*100)} percent"
        )

    def decay(self):
        now = int(time.time() * 1000)
        to_del = []
        for sym, a in self.active.items():
            age = (now - a.ts_ms) / 1000.0
            if age > 480 or a.confidence < 0.15:
                to_del.append(sym)
            else:
                a.confidence *= 0.98
        for s in to_del:
            self.active.pop(s, None)

    def render_banner(self) -> str:
        if not self.active:
            return ""
        top = sorted(self.active.values(), key=lambda x: x.confidence, reverse=True)[0]
        status_color = {"forming": "yellow", "genuine": "green", "fading": "red"}.get(
            top.status, "white"
        )
        return f"[{status_color}]{top.symbol} z={top.z_norm:+.2f} conf={int(top.confidence*100)}% [{top.status}]"


# ---------- Genuine vs Fake classifier ----------
@dataclass
class CheckPoint:
    ts_ms: int
    oi_d: float
    cvd: float
    vol_z: float
    obi: float


class PersistenceJudge:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.hist: Dict[str, deque[CheckPoint]] = {}

    def push(self, sym: str, oi_d: float, cvd: float, vol_z: float, obi: float):
        dq = self.hist.setdefault(sym, deque(maxlen=6))
        dq.append(CheckPoint(int(time.time() * 1000), oi_d, cvd, vol_z, obi))

    def verdict(self, sym: str, side_hint: str, z_norm: float) -> Tuple[str, float]:
        conf = 0.0
        dq = self.hist.get(sym, deque())
        if len(dq) < 2:
            return "forming", 0.35

        cfg_root = self.cfg.get("vreval", self.cfg.get("v_reversal", {}))
        g = cfg_root.get("genuine_checks", {})
        f = cfg_root.get("fade_checks", {})

        p1, p2 = dq[-2], dq[-1]
        cvd_slope = p2.cvd - p1.cvd
        obi_persist = 0
        for i in range(1, min(len(dq), 4)):
            if (dq[-i].obi >= 0 and p2.obi >= 0) or (dq[-i].obi <= 0 and p2.obi <= 0):
                obi_persist += 1
            else:
                break

        if side_hint == "long":
            if p2.oi_d >= g.get("min_oi_delta", 0.02):
                conf += 0.25
            if cvd_slope >= g.get("min_cvd_slope", 0.10):
                conf += 0.25
        else:
            if p2.oi_d <= -g.get("min_oi_delta", 0.02):
                conf += 0.25
            if cvd_slope <= -g.get("min_cvd_slope", 0.10):
                conf += 0.25

        if p2.vol_z >= g.get("min_vol_z", 0.8):
            conf += 0.20
        if obi_persist >= int(g.get("min_obi_persist", 2)):
            conf += 0.20
        conf += max(0.0, min(0.1, abs(z_norm) / 10.0))

        fading = False
        if f.get("opposite_cvd_flip", True) and (np.sign(p1.cvd) != np.sign(p2.cvd)):
            fading = True
        if f.get("vol_drops_fast", True) and (p1.vol_z >= 0.8 and p2.vol_z < 0.2):
            fading = True
        if f.get("obi_snaps_back", True) and (np.sign(p1.obi) != np.sign(p2.obi)):
            fading = True

        status = (
            "fading"
            if fading and conf < 0.6
            else "genuine" if conf >= 0.6 else "forming"
        )
        return status, max(0.0, min(1.0, conf))


# ---------------------------------------------------
# Binance Liquidations WS → LIQ heat
# ---------------------------------------------------
class LiqHeat:
    def __init__(self, window_secs: int = 600):
        self.window = window_secs
        self.buf: Dict[str, deque[Tuple[int, float]]] = {}

    def push(self, symbol: str, notional: float):
        now = int(time.time())
        dq = self.buf.setdefault(symbol, deque())
        dq.append((now, notional))
        while dq and now - dq[0][0] > self.window:
            dq.popleft()

    def heat(self, symbol: str) -> float:
        dq = self.buf.get(symbol, deque())
        if len(dq) < 3:
            return 0.0
        vals = np.array([x[1] for x in dq], dtype=float)
        mu, sd = float(vals.mean()), float(vals.std() + 1e-6)
        return float((vals[-1] - mu) / sd)


async def binance_liq_ws_runner(symbols: List[str], liq: LiqHeat, use_ws: bool):
    if not use_ws:
        return
    streams = "/".join([f"{s.lower()}@forceOrder" for s in symbols])
    url = f"wss://fstream.binance.com/stream?streams={urllib.parse.quote(streams)}"
    async with aiohttp.ClientSession() as sess:
        while True:
            try:
                async with sess.ws_connect(url, heartbeat=20) as ws:
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                                o = data.get("data", {}).get("o", {})
                                s = o.get("s")
                                q = float(o.get("q", 0))
                                p = float(o.get("p", 0))
                                if s and q and p:
                                    liq.push(s.upper(), abs(q * p))
                            except Exception:
                                pass
                        elif msg.type in (
                            aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.ERROR,
                        ):
                            break
            except Exception:
                await asyncio.sleep(3)


# ---------------------------------------------------
# Gradient Spark V-Rev visual (fixed width, color coded)
# ---------------------------------------------------
def _lerp_hex(a: str, b: str, t: float) -> str:
    a = a.lstrip("#")
    b = b.lstrip("#")
    ar, ag, ab = int(a[0:2], 16), int(a[2:4], 16), int(a[4:6], 16)
    br, bg, bb = int(b[0:2], 16), int(b[2:4], 16), int(b[4:6], 16)
    r = int(ar + (br - ar) * t)
    g = int(ag + (bg - ag) * t)
    bl = int(ab + (bb - ab) * t)
    return f"#{r:02x}{g:02x}{bl:02x}"


def vrev_spark(direction: str, conf: float, tick: int, width: int = 5) -> str:
    """
    Fixed-width 5 blocks. A bright spark sweeps across; background shows confidence gradient.
    - direction == "bottom": red -> green (support V)
    - direction == "roof":   green -> red (roof V)
    """
    pos = (tick // 2) % width  # sweep ~3 Hz at refresh_per_second=6
    t = max(0.0, min(1.0, conf))  # clamp 0..1

    if direction == "bottom":
        base_start, base_end = "#ff3b30", "#34c759"  # red -> green
        spark_color = "#00ff67"  # bright green spark
    else:
        base_start, base_end = "#34c759", "#ff3b30"  # green -> red
        spark_color = "#ff2f55"  # bright red spark

    cells = []
    for i in range(width):
        col = _lerp_hex(base_start, base_end, t)
        if i == pos:
            cells.append(f"[{spark_color}]█[/]")
        else:
            cells.append(f"[{col}]█[/]")
    return "".join(cells)


def infer_v_direction(side: str, f: Dict[str, float]) -> str:
    """Decide if the V is bottom (bear->bull) or roof (bull->bear) using quick flow cues."""
    # strong flips against your side = reversal risk
    if side == "short" and ((f["cvd"] > 0 and f["obi"] > 0) or f["mom"] > 0):
        return "bottom"
    if side == "long" and ((f["cvd"] < 0 and f["obi"] < 0) or f["mom"] < 0):
        return "roof"
    # fallback: sign of cvd+obi dominance
    flow = f["cvd"] + f["obi"]
    return "bottom" if flow >= 0 else "roof"


# ---------------------------------------------------
# Styled table (fixed widths)
# ---------------------------------------------------
def build_table(states: Dict[str, State], tick: int = 0) -> Table:
    net = 0.0
    for s in states.values():
        f = s.features
        net += f["cvd"] * 0.5 + f["obi"] * 0.3 + f["mom"] * 50.0
    mood = "🟢 Flow ↑" if net > 0 else "🔴 Flow ↓" if net < 0 else "⚪ Neutral"

    t = Table(
        title=f"[b]Parallel V-Reversal Trade Monitor[/b]  •  {mood}",
        box=box.SQUARE,
        expand=True,
        show_lines=False,
        border_style="grey50",
    )

    # fixed widths -> no reflow / jitter
    t.add_column("🪙 Symbol", style="bold", width=12, no_wrap=True)
    t.add_column("📡 Status", justify="left", width=6)
    t.add_column("🧭 Side", justify="left", width=7)
    t.add_column("🏷️ Price", justify="right", width=12)
    t.add_column("🎯 Entry", justify="right", width=12)
    t.add_column("💸 PnL %", justify="right", width=9)
    t.add_column("⚖️ Score", justify="right", width=9)
    t.add_column("🧠 Guidance", justify="left")
    t.add_column("🔔 V-Rev", justify="left", width=18)
    t.add_column("📊 CVD", justify="right", width=10)
    t.add_column("📚 OBI", justify="right", width=10)
    t.add_column("💨 MOM", justify="right", width=10)
    t.add_column("📢 VOL", justify="right", width=10)
    t.add_column("📈 OI_D", justify="right", width=10)
    t.add_column("💧 LIQ", justify="right", width=9)
    t.add_column("🏦 Source", justify="left", width=7)

    for sym, s in sorted(states.items()):
        f = s.features
        frame = (
            "🚴‍♂️" if (f["cvd"] * 0.5 + f["obi"] * 0.3 + f["mom"] * 50.0) >= 0 else "🏍️"
        )

        # Gradient Spark visual in V-Rev cell
        if s.vrev_status in ("forming", "genuine", "fading"):
            direction = infer_v_direction(s.side, f)
            spark = vrev_spark(direction, s.vrev_conf, tick)
            label = (
                "[bold green]genuine[/]"
                if s.vrev_status == "genuine"
                else (
                    "[bold red]fading[/]"
                    if s.vrev_status == "fading"
                    else "[bold yellow]forming[/]"
                )
            )
            vrevt = f"{spark} {label} ({int(s.vrev_conf*100)}%)"
        else:
            vrevt = "—"

        # score color
        if s.score >= SCORE_GOOD:
            score_txt = f"[bold green]{s.score:+.3f}[/]"
        elif s.score <= SCORE_BAD:
            score_txt = f"[bold red]{s.score:+.3f}[/]"
        else:
            score_txt = f"{s.score:+.3f}"

        t.add_row(
            f"{frame}  {sym}",
            "Live",
            (
                "[bold green]long[/]"
                if s.side == "long"
                else "[bold red]short[/]" if s.side == "short" else "—"
            ),
            color_num(s.price, digits=6),
            color_num(s.entry, digits=6) if s.entry else "—",
            (
                color_num(s.pnl_pct, digits=2, pct=True)
                if (s.pnl_pct == s.pnl_pct)
                else "—"
            ),
            score_txt,
            s.guidance,  # already colorized
            vrevt,
            badge_cvd(f["cvd"]),
            badge_obi(f["obi"]),
            badge_mom(f["mom"]),
            badge_vol(f["vol"]),
            color_num(f["oi_d"], digits=3),
            color_num(f.get("liq", 0.0), digits=3),
            s.source,
        )
    return t


# ---------------------------------------------------
# Symbol loop
# ---------------------------------------------------
async def sym_loop(
    session: aiohttp.ClientSession,
    sym: str,
    cfg: dict,
    db: AsyncDB,
    q: asyncio.Queue,
    alerts: "AlertManager",
    judge: "PersistenceJudge",
    liq_heat: "LiqHeat",
):
    feats_w = cfg["features"]["weights"]
    adx_th = cfg["tuning"]["adx_threshold"]
    chop_th = cfg["tuning"]["chop_threshold"]
    v_w = cfg["v_reversal"]["weights"]
    trigger_z = cfg.get("vreval", cfg.get("v_reversal", {})).get(
        "trigger_z", cfg["v_reversal"]["trigger_z"]
    )

    pos = cfg.get("trading", {}).get("positions", {}).get(sym, {})
    side = (
        str(pos.get("side", cfg.get("trading", {}).get("global_side", ""))).lower()
        or "—"
    )
    entry = pos.get("entry_price", None)
    state = State(side=side, entry=entry)

    use_bnb_oi = cfg.get("backups", {}).get("use_binance_oi_fallback", True)

    while True:
        df5 = await bybit_klines_5m(session, sym, limit=200)
        obi = await bybit_orderbook_obi(
            session, sym, limit=cfg["orderbook"]["depth_limit"]
        )
        oi_d_bybit = await bybit_open_interest_delta(session, sym)
        _, funding_z = await bybit_funding(session, sym)
        _, prem_bybit = await bybit_mark_index(session, sym)

        oi_d = await oi_delta_with_fallback(session, sym, oi_d_bybit, use_bnb_oi)

        if not df5.empty:
            px = float(df5["close"].iloc[-1])
            state.price = px
            if state.entry:
                state.pnl_pct = (
                    (px - state.entry)
                    / max(abs(state.entry), 1e-9)
                    * (100.0 if side == "long" else -100.0)
                )

        try:
            c = df5["close"].to_numpy(dtype=float)
            h = df5["high"].to_numpy(dtype=float)
            l = df5["low"].to_numpy(dtype=float)
            v = df5["volume"].to_numpy(dtype=float)
            mom = roc(c, lookback=3)
            volz = zscore(v, lookback=30)
            cvd = cvd_from_ohlcv(df5, lookback=50)
            a = adx(h, l, c, length=14)
            chop = chop_index(h, l, c, length=14)
        except Exception:
            mom = volz = cvd = a = chop = 0.0

        gate = 0.5 if (a < adx_th or chop > chop_th) else 1.0

        score = (
            feats_w.get("cvd", 0) * cvd
            + feats_w.get("obi", 0) * obi
            + feats_w.get("momentum", 0) * mom
            + feats_w.get("vol", 0) * volz
            + feats_w.get("oi", 0) * oi_d
        ) * gate

        vola_norm = max(np.std(c[-30:]), 1e-6) if len(c) >= 30 else 1.0
        raw = (
            v_w.get("oi", 0) * oi_d
            + v_w.get("funding", 0) * funding_z
            + v_w.get("cvd", 0) * cvd
            + v_w.get("orderbook", 0) * obi
        )
        z_norm = raw / max(vola_norm, 1e-6)
        predictive_fire = z_norm >= trigger_z

        # optional cross-exchange sanity (premium disagreement)
        if cfg.get("consensus", {}).get("dual_exchange", False):
            try:
                prem_bin = await binance_mid_premium(session, sym)
                if prem_bin * prem_bybit < 0:
                    predictive_fire = False
            except Exception:
                pass

        # Track features for persistence judge
        judge.push(sym, oi_d=oi_d, cvd=cvd, vol_z=volz, obi=obi)
        side_hint = "long" if side == "long" else "short"
        status, conf = judge.verdict(sym, side_hint=side_hint, z_norm=z_norm)

        liq_val = liq_heat.heat(sym) if liq_heat is not None else 0.0

        if predictive_fire:
            alerts.trigger(sym, z_norm=z_norm, confidence=conf, status=status)

            # Decide if this V is bearish or bullish for *your current side*
            v_dir = infer_v_direction(side, dict(cvd=cvd, obi=obi, mom=mom))
            is_bearish_for_position = (side == "long" and v_dir == "roof") or (
                side == "short" and v_dir == "bottom"
            )
            icon = "📉" if is_bearish_for_position else "📈"

            if status == "genuine":
                base = f"{icon} V-reversal (5m) [genuine] — exit/flip"
            elif status == "fading":
                base = f"{icon} V-reversal (5m) [fading] — de-risk"
            else:
                base = f"{icon} V-reversal (5m) forming — consider add/flip"

            guidance = colorize_guidance(base)

            await db.log_vreversal(
                sym,
                {
                    "score": float(raw),
                    "z_norm": float(z_norm),
                    "oi": float(oi_d),
                    "funding": float(funding_z),
                    "cvd": float(cvd),
                    "ob": float(obi),
                    "status": status,
                    "conf": conf,
                },
            )
        else:
            if abs(score) < 0.02:
                guidance = colorize_guidance("🤫 no edge")
            elif (side == "long" and score < 0) or (side == "short" and score > 0):
                guidance = colorize_guidance("📉 tighten / reduce")
            else:
                guidance = colorize_guidance("📈 ok / add on pullback")

        state.score = float(score)
        state.guidance = guidance
        state.vrev_status = status if predictive_fire else "—"
        state.vrev_conf = conf if predictive_fire else 0.0
        state.features = dict(
            cvd=float(cvd),
            obi=float(obi),
            mom=float(mom),
            vol=float(volz),
            oi_d=float(oi_d),
            liq=float(liq_val),
        )
        await q.put((sym, state))

        await asyncio.sleep(max(5, int(cfg["app"]["refresh_secs"])))


# ---------------------------------------------------
# Main (gentle redraw with animated header + V-Rev spark)
# ---------------------------------------------------
async def main():
    cfg = load_cfg()
    if cfg.get("app", {}).get("use_uvloop", False):
        with contextlib.suppress(Exception):
            import uvloop

            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

    db = AsyncDB(path=cfg["app"]["db_path"])
    await db.start()

    symbols: List[str] = cfg["app"]["watchlist"]
    if not symbols:
        raise SystemExit("No symbols in app.watchlist")

    timeout = aiohttp.ClientTimeout(total=15)
    connector = aiohttp.TCPConnector(limit=20, ssl=False)
    q: asyncio.Queue = asyncio.Queue()
    states: Dict[str, State] = {}

    alerts = AlertManager(cfg)
    judge = PersistenceJudge(cfg)
    liq_heat = LiqHeat(
        window_secs=int(cfg.get("backups", {}).get("liq_window_secs", 600))
    )
    use_liq_ws = cfg.get("backups", {}).get("use_binance_liq_ws", True)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        liq_task = asyncio.create_task(
            binance_liq_ws_runner(symbols, liq_heat, use_liq_ws)
        )
        tasks = [
            asyncio.create_task(
                sym_loop(session, s, cfg, db, q, alerts, judge, liq_heat)
            )
            for s in symbols
        ]

        # We repaint at ~6 fps; widths fixed -> no strobe.
        with Live(console=console, refresh_per_second=6) as live:
            tick = 0
            cached_table = None
            dirty = True

            while True:
                try:
                    sym, st = await asyncio.wait_for(
                        q.get(), timeout=0.3
                    )  # ~3.3 Hz UI cycle
                    states[sym] = st
                    dirty = True
                except asyncio.TimeoutError:
                    pass

                # Rebuild table on data change OR every second tick to animate the spark smoothly
                if dirty or tick % 2 == 0:
                    cached_table = build_table(states, tick=tick)
                    dirty = False

                alerts.decay()
                header = anim_header(tick, alerts.render_banner())

                layout = Layout()
                layout.split(
                    Layout(header, name="header", size=3),
                    Layout(cached_table or Table(), name="table"),
                )
                live.update(layout)
                tick += 1

        for t in tasks:
            t.cancel()
        liq_task.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
