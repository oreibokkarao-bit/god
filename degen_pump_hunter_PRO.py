"""
Degen Pump Hunter – Majors + BANANAS-style pump radar (with FlowEngine)
+ high R:R trade trigger plans + ignition heatmap + confidence score
+ hybrid visual dashboard (color-coded + emojis via Rich)
+ pullback-based Entry / ATR+Structure SL / dynamic TP ladder.

Run:
    python degen_pump_hunter.py
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import aiohttp
from rich.console import Console, Group
from rich.live import Live
from rich.table import Table

from flow_engine import FlowEngine, FlowSnapshot, adjust_score_with_flow  # type: ignore

BINANCE_FAPI_BASE = "https://fapi.binance.com"

# -------------------------- CONFIG --------------------------

MICROCAP_MODE: bool = True

MAJOR_MIN_QV_USDT: float = 1_000_000.0  # 24h quote volume >= 1M
DEGEN_MIN_QV_USDT: float = 50_000.0     # allow smaller names

DEGEN_MIN_24H_PCT: float = 5.0
DEGEN_MIN_RANGE_PCT: float = 12.0

TOP_N_MAJORS: int = 15
TOP_N_DEGEN: int = 15

REFRESH_SECONDS: float = 15.0

TRIGGER_MIN_SCORE: float = 24.0
MAX_TRADE_PLANS: int = 8

RISK_SPAN_FRACTION: float = 0.40  # kept for compat
MIN_RISK_PCT: float = 0.03        # minimum 3% price risk


# ---------------------- Safe HTTP Client --------------------


class SafeHTTPClient:
    def __init__(self, min_interval_sec: float = 0.12, max_retries: int = 5) -> None:
        self._min_interval = float(min_interval_sec)
        self._max_retries = int(max_retries)
        self._last_call_ts: float = 0.0
        self._session: Optional[aiohttp.ClientSession] = None
        self._lock = asyncio.Lock()

    async def __aenter__(self) -> "SafeHTTPClient":
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._session:
            await self._session.close()
        self._session = None

    async def get_json(self, url: str, params: Optional[Dict[str, str]] = None) -> Optional[object]:
        assert self._session is not None, "SafeHTTPClient must be used as async context manager"

        async with self._lock:
            to_wait = self._min_interval - (time.time() - self._last_call_ts)
            if to_wait > 0:
                await asyncio.sleep(to_wait)
            self._last_call_ts = time.time()

        attempt = 0
        backoff = 0.5
        while True:
            attempt += 1
            try:
                async with self._session.get(url, params=params, timeout=10) as resp:
                    status = resp.status
                    if status == 200:
                        return await resp.json()
                    if status in (418, 429, 500, 502, 503, 504):
                        if attempt >= self._max_retries:
                            return None
                        await asyncio.sleep(backoff + 0.2 * attempt)
                        backoff *= 2
                        continue
                    return None
            except (aiohttp.ClientError, asyncio.TimeoutError):
                if attempt >= self._max_retries:
                    return None
                await asyncio.sleep(backoff + 0.2 * attempt)
                backoff *= 2


# ------------------------- Data types ------------------------


@dataclass
class MajorRow:
    symbol: str
    last_price: float
    change_24h_pct: float
    dist_from_high_pct: float
    strength_score: float
    ignition_score: float
    confidence: float
    heat_cvd: bool
    heat_vol: bool
    heat_oi: bool
    heat_ob: bool
    reasons: List[str]


@dataclass
class DegenRow:
    symbol: str
    last_price: float
    change_24h_pct: float
    range_24h_pct: float
    dist_from_low_pct: float
    degen_score: float
    ignition_score: float
    confidence: float
    heat_cvd: bool
    heat_vol: bool
    heat_oi: bool
    heat_ob: bool
    high_24h: float
    low_24h: float
    reasons: List[str] = field(default_factory=list)


@dataclass
class TradePlanRow:
    symbol: str
    price: float
    score: float
    ignition_score: float
    confidence: float
    direction: str
    entry: float
    sl: float
    tp1: float
    tp2: float
    tp3: float
    rr1: float
    rr2: float
    rr3: float
    heat_cvd: bool
    heat_vol: bool
    heat_oi: bool
    heat_ob: bool
    reasons: List[str]


# ----------------------- Fetch helpers -----------------------


async def fetch_futures_universe(http: SafeHTTPClient) -> List[str]:
    url = f"{BINANCE_FAPI_BASE}/fapi/v1/exchangeInfo"
    raw = await http.get_json(url)
    if not isinstance(raw, dict):
        return []
    symbols: List[str] = []
    for s in raw.get("symbols", []):
        try:
            if s.get("contractType") != "PERPETUAL":
                continue
            if s.get("quoteAsset") != "USDT":
                continue
            if s.get("status") != "TRADING":
                continue
            sym = s.get("symbol")
            if sym:
                symbols.append(sym)
        except Exception:
            continue
    return sorted(set(symbols))


async def fetch_24h_tickers(http: SafeHTTPClient) -> Dict[str, dict]:
    url = f"{BINANCE_FAPI_BASE}/fapi/v1/ticker/24hr"
    raw = await http.get_json(url)
    out: Dict[str, dict] = {}
    if isinstance(raw, list):
        for item in raw:
            sym = item.get("symbol")
            if sym:
                out[str(sym)] = item
    return out


# ----------------------- Visual helpers ----------------------


def _safe_float(d: dict, key: str, default: float = 0.0) -> float:
    try:
        return float(d.get(key, default))
    except (TypeError, ValueError):
        return default


def _build_heat_flags(flow: Optional[FlowSnapshot]) -> tuple[bool, bool, bool, bool]:
    if flow is None or not flow.ignition_flags:
        return False, False, False, False

    cvd = any(flag.startswith("CVD kick") for flag in flow.ignition_flags)
    vol = any(flag.startswith("Vol burst") for flag in flow.ignition_flags)
    oi = any(flag.startswith("OI uptick") for flag in flow.ignition_flags)
    ob = any(flag.startswith("OB bid/ask") for flag in flow.ignition_flags)
    return cvd, vol, oi, ob


def _heat_str(cvd: bool, vol: bool, oi: bool, ob: bool) -> str:
    return "".join(
        [
            "🟩" if cvd else "⬛",
            "🟩" if vol else "⬛",
            "🟩" if oi else "⬛",
            "🟩" if ob else "⬛",
        ]
    ) + "  "


def _confidence_from_base_and_ignition(base_score: float, ignition_score: float) -> float:
    base_norm = min(max(base_score, 0.0), 40.0) / 40.0
    ign_norm = min(max(ignition_score, 0.0), 4.0) / 4.0
    return 60.0 * base_norm + 40.0 * ign_norm


def fmt_pct(value: float, plus: bool = True) -> str:
    sign_fmt = f"{value:+.1f}" if plus else f"{value:.1f}"
    if value >= 20:
        return f"[bold bright_green]{sign_fmt}[/]"
    if value >= 10:
        return f"[green]{sign_fmt}[/]"
    if value >= 0:
        return f"[yellow]{sign_fmt}[/]"
    if value <= -10:
        return f"[bold red]{sign_fmt}[/]"
    return f"[red]{sign_fmt}[/]"


def fmt_distance(value: float) -> str:
    if value <= 3:
        return f"[bold bright_green]{value:.1f}[/]"
    if value <= 10:
        return f"[green]{value:.1f}[/]"
    if value <= 20:
        return f"[yellow]{value:.1f}[/]"
    return f"[red]{value:.1f}[/]"


def fmt_score(score: float) -> str:
    if score >= 30:
        return f"[bold bright_green]{score:.1f} 🚀[/]"
    if score >= 24:
        return f"[green]{score:.1f} 🔥[/]"
    if score >= 18:
        return f"[yellow]{score:.1f}[/]"
    return f"[dim]{score:.1f}[/]"


def fmt_conf(conf: float) -> str:
    c = int(round(conf))
    if c >= 85:
        return f"[bold bright_green]{c}⚡🚀[/]"
    if c >= 70:
        return f"[bold green]{c}⚡[/]"
    if c >= 50:
        return f"[yellow]{c}[/]"
    return f"[dim]{c}[/]"


def fmt_rr(rr1: float, rr2: float, rr3: float) -> str:
    tag = ""
    if rr3 >= 4.0 or rr2 >= 3.0:
        tag = " 💎"
    return f"{rr1:.1f}/{rr2:.1f}/{rr3:.1f}{tag}"


# ----------------------- Scoring logic -----------------------


def compute_major_row(
    symbol: str,
    t: dict,
    flow: Optional[FlowSnapshot],
) -> Optional[MajorRow]:
    last_price = _safe_float(t, "lastPrice")
    high_24h = _safe_float(t, "highPrice")
    low_24h = _safe_float(t, "lowPrice")
    change_24h_pct = _safe_float(t, "priceChangePercent")

    if last_price <= 0 or high_24h <= 0 or low_24h <= 0:
        return None

    dist_from_high_pct = (high_24h - last_price) / high_24h * 100.0

    pump_score = max(change_24h_pct, 0.0)
    proximity_score = max(0.0, 5.0 - dist_from_high_pct) * 2.0

    range_span = max(high_24h - low_24h, high_24h * 0.001)
    dist_from_low = (last_price - low_24h) / range_span
    burnt_out_penalty = 0.0
    if dist_from_low > 0.95 and change_24h_pct > 20:
        burnt_out_penalty = 10.0

    base_score = pump_score * 0.5 + proximity_score * 0.5 - burnt_out_penalty

    ignition_score = flow.ignition_score if flow is not None else 0.0
    confidence = _confidence_from_base_and_ignition(base_score, ignition_score)
    heat_cvd, heat_vol, heat_oi, heat_ob = _build_heat_flags(flow)

    adj_score, flow_reasons = adjust_score_with_flow(
        base_score=base_score,
        flow=flow,
        oi_change_pct=None,
        direction="long",
    )

    reasons: List[str] = []
    if pump_score > 10:
        reasons.append(f"24h pump {pump_score:.1f}%")
    if dist_from_high_pct < 3:
        reasons.append(f"{dist_from_high_pct:.1f}% from 24h high")
    if burnt_out_penalty > 0:
        reasons.append("Possible blow-off (very extended)")
    reasons.extend(flow_reasons)

    return MajorRow(
        symbol=symbol,
        last_price=last_price,
        change_24h_pct=change_24h_pct,
        dist_from_high_pct=dist_from_high_pct,
        strength_score=adj_score,
        ignition_score=ignition_score,
        confidence=confidence,
        heat_cvd=heat_cvd,
        heat_vol=heat_vol,
        heat_oi=heat_oi,
        heat_ob=heat_ob,
        reasons=reasons,
    )


def compute_degen_row(
    symbol: str,
    t: dict,
    flow: Optional[FlowSnapshot],
) -> Optional[DegenRow]:
    last_price = _safe_float(t, "lastPrice")
    high_24h = _safe_float(t, "highPrice")
    low_24h = _safe_float(t, "lowPrice")
    change_24h_pct = _safe_float(t, "priceChangePercent")

    if last_price <= 0 or high_24h <= 0 or low_24h <= 0:
        return None

    if change_24h_pct < DEGEN_MIN_24H_PCT:
        return None

    range_24h_pct = (high_24h - low_24h) / low_24h * 100.0
    if range_24h_pct < DEGEN_MIN_RANGE_PCT:
        return None

    dist_from_low_abs = (last_price - low_24h) / low_24h * 100.0

    vol_score = min(range_24h_pct, 200.0) * 0.25
    off_floor_score = min(max(dist_from_low_abs, 0.0), 150.0) * 0.25
    pump_score = max(min(change_24h_pct, 200.0), 0.0) * 0.5

    base_score = pump_score + vol_score + off_floor_score

    ignition_score = flow.ignition_score if flow is not None else 0.0

    adj_score, flow_reasons = adjust_score_with_flow(
        base_score=base_score,
        flow=flow,
        oi_change_pct=None,
        direction="long",
    )

    confidence = _confidence_from_base_and_ignition(base_score, ignition_score)
    heat_cvd, heat_vol, heat_oi, heat_ob = _build_heat_flags(flow)

    reasons: List[str] = [
        f"24h %+{change_24h_pct:.1f}",
        f"Range {range_24h_pct:.1f}%",
        f"+{dist_from_low_abs:.1f}% off 24h low",
    ]
    reasons.extend(flow_reasons)

    return DegenRow(
        symbol=symbol,
        last_price=last_price,
        change_24h_pct=change_24h_pct,
        range_24h_pct=range_24h_pct,
        dist_from_low_pct=dist_from_low_abs,
        degen_score=adj_score,
        ignition_score=ignition_score,
        confidence=confidence,
        heat_cvd=heat_cvd,
        heat_vol=heat_vol,
        heat_oi=heat_oi,
        heat_ob=heat_ob,
        high_24h=high_24h,
        low_24h=low_24h,
        reasons=reasons,
    )


# -------- Pullback / SL / TP engine helpers for degen --------


def build_pullback_plan_for_degen(
    row: DegenRow,
    atr_short: float,
) -> Optional[Tuple[float, float, float, float, float]]:
    last = row.last_price
    low = row.low_24h
    high = row.high_24h

    if low <= 0 or high <= 0 or last <= 0:
        return None

    impulse_range = high - low
    if impulse_range <= 0:
        return None

    pullback_top = high - 0.25 * impulse_range
    pullback_bottom = high - 0.45 * impulse_range
    pullback_bottom = max(pullback_bottom, low * 1.01)

    entry = (pullback_top + pullback_bottom) / 2.0

    if atr_short <= 0:
        atr_short = impulse_range / 20.0

    struct_sl = low * 0.997
    atr_sl = entry - 1.5 * atr_short

    sl_candidates = [x for x in (struct_sl, atr_sl) if x < entry]
    if not sl_candidates:
        return None
    sl = max(sl_candidates)

    risk = entry - sl
    if risk <= 0:
        return None

    tp1_raw = entry + 2.0 * risk
    tp1 = min(max(tp1_raw, entry + 1.5 * risk), high * 1.02)

    tp2_raw = entry + 3.0 * risk
    tp2_cap = high * 1.25
    tp2 = min(tp2_raw, tp2_cap)

    tp3_raw = entry + 4.0 * risk
    measured = entry + impulse_range
    tp3_candidate = max(tp3_raw, measured)
    tp3_cap = high * 1.5
    tp3 = min(tp3_candidate, tp3_cap)

    if tp1 <= entry or tp2 <= tp1 or tp3 <= tp2:
        return None

    return entry, sl, tp1, tp2, tp3


# ---------------------- Table builders -----------------------


def build_major_table(rows: List[MajorRow]) -> Table:
    table = Table(
        title="Majors – Trend Breakout Radar (Flow-confirmed)",
        expand=True,
    )
    table.add_column("#", justify="right", width=3)
    table.add_column("Symbol", justify="left", width=10)
    table.add_column("Price", justify="right", width=10)
    table.add_column("24h %", justify="right", width=9)
    table.add_column("↓ from High %", justify="right", width=14)
    table.add_column("Score", justify="right", width=8)
    table.add_column("Conf", justify="right", width=8)
    table.add_column("Heat", justify="center", width=9)
    table.add_column("Why", justify="left", overflow="fold")

    for idx, r in enumerate(rows[:TOP_N_MAJORS], start=1):
        table.add_row(
            str(idx),
            r.symbol,
            f"{r.last_price:.4g}",
            fmt_pct(r.change_24h_pct),
            fmt_distance(r.dist_from_high_pct),
            fmt_score(r.strength_score),
            fmt_conf(r.confidence),
            _heat_str(r.heat_cvd, r.heat_vol, r.heat_oi, r.heat_ob),
            "; ".join(r.reasons),
        )

    for idx in range(len(rows) + 1, TOP_N_MAJORS + 1):
        table.add_row(str(idx), "", "", "", "", "", "", "", "")

    return table


def build_degen_table(rows: List[DegenRow]) -> Table:
    title = "Degen – Pump Hunter (BANANAS mode, Flow-confirmed)" if MICROCAP_MODE else "Degen – Pump Hunter (disabled)"
    table = Table(
        title=title,
        expand=True,
    )
    table.add_column("#", justify="right", width=3)
    table.add_column("Symbol", justify="left", width=9)
    table.add_column("Price", justify="right", width=9)
    table.add_column("24h %", justify="right", width=8)
    table.add_column("Range %", justify="right", width=9)
    table.add_column("↑ from Low %", justify="right", width=11)
    table.add_column("Score", justify="right", width=9)
    table.add_column("Conf", justify="right", width=8)
    table.add_column("Heat", justify="center", width=9)
    table.add_column("Why", justify="left", overflow="fold")

    for idx, r in enumerate(rows[:TOP_N_DEGEN], start=1):
        table.add_row(
            str(idx),
            r.symbol,
            f"{r.last_price:.4g}",
            fmt_pct(r.change_24h_pct),
            fmt_pct(r.range_24h_pct, plus=False),
            fmt_distance(r.dist_from_low_pct),
            fmt_score(r.degen_score),
            fmt_conf(r.confidence),
            _heat_str(r.heat_cvd, r.heat_vol, r.heat_oi, r.heat_ob),
            "; ".join(r.reasons),
        )

    for idx in range(len(rows) + 1, TOP_N_DEGEN + 1):
        table.add_row(str(idx), "", "", "", "", "", "", "", "", "")

    return table


def build_trade_plans_table(rows: List[DegenRow]) -> Table:
    table = Table(
        title="Trade Trigger Plan – Long (Degen)",
        expand=True,
    )
    table.add_column("#", justify="right", width=3)
    table.add_column("Symbol", justify="left", width=9)
    table.add_column("Score", justify="right", width=7)
    table.add_column("Conf", justify="right", width=7)
    table.add_column("Ign", justify="right", width=4)
    table.add_column("Heat", justify="center", width=9)
    table.add_column("Entry", justify="right", width=9)
    table.add_column("SL", justify="right", width=9)
    table.add_column("TP1", justify="right", width=9)
    table.add_column("TP2", justify="right", width=9)
    table.add_column("TP3", justify="right", width=9)
    table.add_column("R1/R2/R3", justify="right", width=13)
    table.add_column("Why", justify="left", overflow="fold")

    candidates = sorted(
        [r for r in rows if r.degen_score >= TRIGGER_MIN_SCORE],
        key=lambda r: (r.confidence, r.ignition_score, r.degen_score),
        reverse=True,
    )

    plans: List[TradePlanRow] = []

    for r in candidates:
        atr_short_estimate = max((r.high_24h - r.low_24h) / 20.0, r.last_price * MIN_RISK_PCT * 0.5)

        plan = build_pullback_plan_for_degen(r, atr_short_estimate)
        if plan is None:
            continue

        entry, sl, tp1, tp2, tp3 = plan
        risk_per_unit = entry - sl
        if risk_per_unit <= 0:
            continue

        rr1 = (tp1 - entry) / risk_per_unit
        rr2 = (tp2 - entry) / risk_per_unit
        rr3 = (tp3 - entry) / risk_per_unit

        plans.append(
            TradePlanRow(
                symbol=r.symbol,
                price=r.last_price,
                score=r.degen_score,
                ignition_score=r.ignition_score,
                confidence=r.confidence,
                direction="LONG",
                entry=entry,
                sl=sl,
                tp1=tp1,
                tp2=tp2,
                tp3=tp3,
                rr1=rr1,
                rr2=rr2,
                rr3=rr3,
                heat_cvd=r.heat_cvd,
                heat_vol=r.heat_vol,
                heat_oi=r.heat_oi,
                heat_ob=r.heat_ob,
                reasons=r.reasons,
            )
        )

        if len(plans) >= MAX_TRADE_PLANS:
            break

    for idx, p in enumerate(plans, start=1):
        table.add_row(
            str(idx),
            p.symbol,
            fmt_score(p.score),
            fmt_conf(p.confidence),
            f"{p.ignition_score:.0f}",
            _heat_str(p.heat_cvd, p.heat_vol, p.heat_oi, p.heat_ob),
            f"{p.entry:.4g}",
            f"{p.sl:.4g}",
            f"{p.tp1:.4g}",
            f"{p.tp2:.4g}",
            f"{p.tp3:.4g}",
            fmt_rr(p.rr1, p.rr2, p.rr3),
            "; ".join(p.reasons),
        )

    for idx in range(len(plans) + 1, MAX_TRADE_PLANS + 1):
        table.add_row(
            str(idx),
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
        )

    return table


# -------------------------- Main loop ------------------------


async def scanner_main() -> None:
    console = Console()
    console.print("[bold cyan]Degen Pump Hunter starting (Binance futures + Bybit FlowEngine)...[/]")
    if MICROCAP_MODE:
        console.print("[magenta]MICROCAP_MODE = True → degen panel ENABLED[/]")
    else:
        console.print("[magenta]MICROCAP_MODE = False → degen panel DISABLED[/]")

    async with SafeHTTPClient() as http:
        symbols = await fetch_futures_universe(http)
        if not symbols:
            console.print("[red]Failed to fetch Binance futures universe.[/]")
            return

        console.print(f"[green]Tracking {len(symbols)} USDT-perp symbols.[/]")

        flow_engine = FlowEngine(symbols)
        stop_event = asyncio.Event()
        flow_task = asyncio.create_task(flow_engine.run(stop_event))

        try:
            with Live(console=console, refresh_per_second=0.5, screen=False) as live:
                while True:
                    tickers = await fetch_24h_tickers(http)
                    now_ts = time.time()

                    major_rows: List[MajorRow] = []
                    degen_rows: List[DegenRow] = []

                    for sym, t in tickers.items():
                        if sym not in symbols:
                            continue

                        quote_vol = _safe_float(t, "quoteVolume")
                        flow = await flow_engine.get_snapshot(sym, now_ts)

                        if quote_vol >= MAJOR_MIN_QV_USDT:
                            mr = compute_major_row(sym, t, flow)
                            if mr is not None:
                                major_rows.append(mr)

                        if MICROCAP_MODE and quote_vol >= DEGEN_MIN_QV_USDT:
                            dr = compute_degen_row(sym, t, flow)
                            if dr is not None:
                                degen_rows.append(dr)

                    major_rows.sort(key=lambda r: r.strength_score, reverse=True)
                    degen_rows.sort(key=lambda r: r.degen_score, reverse=True)

                    group = Group(
                        build_major_table(major_rows),
                        build_degen_table(degen_rows),
                        build_trade_plans_table(degen_rows),
                    )
                    live.update(group)

                    await asyncio.sleep(REFRESH_SECONDS)
        finally:
            stop_event.set()
            await asyncio.sleep(0.1)
            flow_task.cancel()
            import contextlib
            with contextlib.suppress(asyncio.CancelledError):
                await flow_task


def main() -> None:
    try:
        asyncio.run(scanner_main())
    except KeyboardInterrupt:
        print("Exiting...")


if __name__ == "__main__":
    main()
