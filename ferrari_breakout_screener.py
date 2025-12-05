#!/usr/bin/env python3
"""
Ferrari Breakout Screener – Coinalyze Docteja Edition (Futures Core, Auto-Universe)

- Auto-discovers Binance USDT perpetual futures from Coinalyze /future-markets.
- Pulls OHLCV, OI history, Funding & Predicted Funding history.
- Computes Docteja-style futures-core metrics and ranks symbols into:

    1) 🏁 Top 5 coins with “100% Chance” of a 20% Pump (heuristic)
    2) 🏆 Top 5 coins with highest chance of being top gainers next 24h
    3) 💥 Top 5 coins with highest chances of a violent breakout explosion
    4) ⚰️ Top 5 coins with highest chance of being biggest losers next 24h

YOU ONLY NEED TO:
- Put your Coinalyze API key in COINALYZE_API_KEY.
"""

import asyncio
import os
import time
from dataclasses import dataclass, field
from typing import Dict, List, Any

import httpx
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.align import Align
from rich import box


# ======================================================================
#  CONFIG – EDIT JUST YOUR API KEY
# ======================================================================

COINALYZE_API_KEY = ""  # 🔑 <= CHANGE THIS

COINALYZE_BASE_URL = "https://api.coinalyze.net/v1"
REFRESH_SECONDS = 30.0          # data refresh cadence
MAX_UNIVERSE_SYMBOLS = 80       # cap how many futures symbols we track

console = Console()


# ======================================================================
#  UTILS
# ======================================================================

def pct_change(new: float, old: float) -> float:
    if old == 0:
        return 0.0
    return (new - old) / old * 100.0


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


# ======================================================================
#  DATA MODEL
# ======================================================================

@dataclass
class SymbolState:
    symbol: str

    price: float = 0.0
    rvol_5m: float = 0.0

    mom_15m: float = 0.0
    mom_60m: float = 0.0
    mom_24h: float = 0.0

    oi_ch_1h: float = 0.0
    oi_ch_6h: float = 0.0
    oi_ch_24h: float = 0.0

    fr_1h: float = 0.0
    pfr_1h: float = 0.0

    explosion_score: float = 0.0
    pump20_score: float = 0.0
    gainer24_score: float = 0.0
    loser24_score: float = 0.0

    last_update_ts: float = field(default_factory=time.time)

    def update_from_metrics(self, m: Dict[str, float]):
        self.price = float(m.get("price", self.price))
        self.rvol_5m = float(m.get("rvol_5m", self.rvol_5m))
        self.mom_15m = float(m.get("mom_15m", self.mom_15m))
        self.mom_60m = float(m.get("mom_60m", self.mom_60m))
        self.mom_24h = float(m.get("mom_24h", self.mom_24h))

        self.oi_ch_1h = float(m.get("oi_ch_1h", self.oi_ch_1h))
        self.oi_ch_6h = float(m.get("oi_ch_6h", self.oi_ch_6h))
        self.oi_ch_24h = float(m.get("oi_ch_24h", self.oi_ch_24h))

        self.fr_1h = float(m.get("fr_1h", self.fr_1h))
        self.pfr_1h = float(m.get("pfr_1h", self.pfr_1h))

        self.last_update_ts = time.time()
        self._update_scores()

    def _update_scores(self):
        # Futures-side approximation of your filter brain
        rv = clamp(self.rvol_5m, 0.0, 20.0)
        m15 = self.mom_15m
        m60 = self.mom_60m
        m24 = self.mom_24h
        oi1 = self.oi_ch_1h
        oi6 = self.oi_ch_6h
        oi24 = self.oi_ch_24h
        fr = self.fr_1h
        pfr = self.pfr_1h

        # --- Explosion: short-term violence
        self.explosion_score = (
            rv * 1.4
            + max(0.0, m15) * 1.2
            + max(0.0, m60) * 0.9
            + max(0.0, oi1) * 0.7
            + max(0.0, oi6) * 0.5
        )

        # --- Pump20: big-move potential from now
        base_pump = (
            max(0.0, oi6) * 1.2
            + max(0.0, oi24) * 0.8
            + rv * 0.6
            + max(0.0, m15) * 0.5
        )
        if m24 > 25:
            base_pump *= 0.6
        elif m24 > 15:
            base_pump *= 0.75

        if oi6 > 0 and (fr < 0 or pfr < 0):
            base_pump *= 1.15

        self.pump20_score = base_pump

        # --- Gainer24: next 24h top gainers
        funding_penalty = 0.0
        if fr > 0.15 or pfr > 0.15:
            funding_penalty = (fr + pfr) * 20.0

        self.gainer24_score = (
            max(0.0, m24) * 1.5
            + max(0.0, m60) * 1.0
            + max(0.0, oi24) * 1.2
            + max(0.0, oi6) * 0.8
            - max(0.0, funding_penalty)
        )

        # --- Loser24: biggest losers next 24h
        down24 = max(0.0, -m24)
        down60 = max(0.0, -m60)

        oi_unwind = max(0.0, -oi24)
        oi_trap = 0.0
        if oi24 > 0 and m24 < 0:
            oi_trap = oi24 * down24 * 0.05

        funding_bias = 0.0
        if fr < 0 or pfr < 0:
            funding_bias = (-fr - pfr) * 30.0

        self.loser24_score = (
            down24 * 1.4
            + down60 * 1.0
            + oi_unwind
            + oi_trap
            + funding_bias
        )


class ScreenerState:
    def __init__(self):
        self.symbols: Dict[str, SymbolState] = {}

    def get_or_create(self, symbol: str) -> SymbolState:
        s = self.symbols.get(symbol)
        if s is None:
            s = SymbolState(symbol=symbol)
            self.symbols[symbol] = s
        return s

    def update_symbol_metrics(self, symbol: str, metrics: Dict[str, float]):
        s = self.get_or_create(symbol)
        s.update_from_metrics(metrics)

    def top_by(self, key, limit: int = 5) -> List[SymbolState]:
        vals = [s for s in self.symbols.values() if s.price > 0]
        ranked = sorted(vals, key=key, reverse=True)
        return ranked[:limit]


screener = ScreenerState()


# ======================================================================
#  COINALYZE CLIENT
# ======================================================================

def coinalyze_client() -> httpx.AsyncClient:
    headers = {
        "Accept": "application/json",
        "User-Agent": "FerrariBreakoutScreener/1.0",
        "api_key": COINALYZE_API_KEY,
    }
    return httpx.AsyncClient(base_url=COINALYZE_BASE_URL, headers=headers, timeout=20.0)


async def discover_futures_universe() -> List[str]:
    """
    Use /future-markets to auto-discover Binance USDT perps with OHLCV data.
    """
    async with coinalyze_client() as client:
        resp = await client.get("/future-markets")
        try:
            resp.raise_for_status()
        except httpx.HTTPError as e:
            console.print(f"[red bold]ERROR:[/red bold] Failed to fetch /future-markets: {e}")
            return []

        data = resp.json()
        symbols: List[str] = []
        for m in data:
            try:
                if (
                    m.get("exchange") == "BINANCE"
                    and m.get("quote_asset") == "USDT"
                    and m.get("is_perpetual", False)
                    and m.get("has_ohlcv_data", False)
                ):
                    symbols.append(m["symbol"])
            except Exception:
                continue

        symbols = sorted(set(symbols))
        if len(symbols) > MAX_UNIVERSE_SYMBOLS:
            symbols = symbols[:MAX_UNIVERSE_SYMBOLS]

        console.print(
            f"[green]Universe built from /future-markets:[/green] "
            f"{len(symbols)} Binance USDT perps"
        )
        if not symbols:
            console.print(
                "[red]No symbols found. Check API key / permissions.[/red]"
            )
        return symbols


async def fetch_ohlcv_history(
    client: httpx.AsyncClient,
    symbols: List[str],
    interval: str,
    lookback_seconds: int,
) -> Dict[str, List[Dict[str, Any]]]:
    now_sec = int(time.time())
    from_sec = now_sec - lookback_seconds

    sym_param = ",".join(symbols)
    params = {
        "symbols": sym_param,
        "interval": interval,
        "from": from_sec,
        "to": now_sec,
    }

    resp = await client.get("/ohlcv-history", params=params)
    resp.raise_for_status()
    raw = resp.json()

    out: Dict[str, List[Dict[str, Any]]] = {}
    for item in raw:
        sym = item["symbol"]
        out[sym] = item.get("history", [])
    return out


async def fetch_oi_history(
    client: httpx.AsyncClient,
    symbols: List[str],
    interval: str,
    lookback_seconds: int,
) -> Dict[str, List[Dict[str, Any]]]:
    now_sec = int(time.time())
    from_sec = now_sec - lookback_seconds

    sym_param = ",".join(symbols)
    params = {
        "symbols": sym_param,
        "interval": interval,
        "from": from_sec,
        "to": now_sec,
        "convert_to_usd": "true",
    }

    resp = await client.get("/open-interest-history", params=params)
    resp.raise_for_status()
    raw = resp.json()
    out: Dict[str, List[Dict[str, Any]]] = {}
    for item in raw:
        sym = item["symbol"]
        out[sym] = item.get("history", [])
    return out


async def fetch_fr_history(
    client: httpx.AsyncClient,
    symbols: List[str],
    interval: str,
    lookback_seconds: int,
) -> Dict[str, List[Dict[str, Any]]]:
    now_sec = int(time.time())
    from_sec = now_sec - lookback_seconds

    sym_param = ",".join(symbols)
    params = {
        "symbols": sym_param,
        "interval": interval,
        "from": from_sec,
        "to": now_sec,
    }

    resp = await client.get("/funding-rate-history", params=params)
    resp.raise_for_status()
    raw = resp.json()
    out: Dict[str, List[Dict[str, Any]]] = {}
    for item in raw:
        sym = item["symbol"]
        out[sym] = item.get("history", [])
    return out


async def fetch_pfr_history(
    client: httpx.AsyncClient,
    symbols: List[str],
    interval: str,
    lookback_seconds: int,
) -> Dict[str, List[Dict[str, Any]]]:
    now_sec = int(time.time())
    from_sec = now_sec - lookback_seconds

    sym_param = ",".join(symbols)
    params = {
        "symbols": sym_param,
        "interval": interval,
        "from": from_sec,
        "to": now_sec,
    }

    resp = await client.get("/predicted-funding-rate-history", params=params)
    resp.raise_for_status()
    raw = resp.json()
    out: Dict[str, List[Dict[str, Any]]] = {}
    for item in raw:
        sym = item["symbol"]
        out[sym] = item.get("history", [])
    return out


# ======================================================================
#  METRIC EXTRACTION
# ======================================================================

def compute_rvol_5m(history_5m: List[Dict[str, Any]]) -> float:
    if len(history_5m) < 5:
        return 0.0
    vols = [float(c["v"]) for c in history_5m]
    last_vol = vols[-1]
    base = vols[:-1][-20:]
    if not base:
        return 0.0
    avg_vol = sum(base) / len(base)
    if avg_vol <= 0:
        return 0.0
    return last_vol / avg_vol


def compute_mom_from_closes(closes: List[float], bars_back: int) -> float:
    if not closes:
        return 0.0
    last = closes[-1]
    if len(closes) > bars_back:
        ref = closes[-1 - bars_back]
    else:
        ref = closes[0]
    return pct_change(last, ref)


def compute_mom_24h_from_1h(closes_1h: List[float]) -> float:
    if not closes_1h:
        return 0.0
    last = closes_1h[-1]
    if len(closes_1h) > 24:
        ref = closes_1h[-1 - 24]
    else:
        ref = closes_1h[0]
    return pct_change(last, ref)


def compute_oi_change(history_1h_oi: List[Dict[str, Any]], hours_back: int) -> float:
    if not history_1h_oi:
        return 0.0
    cvals = [float(c["c"]) for c in history_1h_oi]
    last = cvals[-1]
    if len(cvals) > hours_back:
        ref = cvals[-1 - hours_back]
    else:
        ref = cvals[0]
    return pct_change(last, ref)


def last_c_from_history(history: List[Dict[str, Any]]) -> float:
    if not history:
        return 0.0
    return float(history[-1]["c"])


async def update_metrics_for_batch(symbols_batch: List[str]):
    if not symbols_batch:
        return

    async with coinalyze_client() as client:
        lookback_5m = 24 * 3600
        lookback_1h = 7 * 24 * 3600

        ohlcv5_task = asyncio.create_task(
            fetch_ohlcv_history(client, symbols_batch, "5min", lookback_5m)
        )
        ohlcv1h_task = asyncio.create_task(
            fetch_ohlcv_history(client, symbols_batch, "1hour", lookback_1h)
        )
        oi1h_task = asyncio.create_task(
            fetch_oi_history(client, symbols_batch, "1hour", lookback_1h)
        )
        fr1h_task = asyncio.create_task(
            fetch_fr_history(client, symbols_batch, "1hour", lookback_1h)
        )
        pfr1h_task = asyncio.create_task(
            fetch_pfr_history(client, symbols_batch, "1hour", lookback_1h)
        )

        try:
            ohlcv_5m = await ohlcv5_task
            ohlcv_1h = await ohlcv1h_task
            oi_1h = await oi1h_task
            fr_1h_hist = await fr1h_task
            pfr_1h_hist = await pfr1h_task
        except httpx.HTTPError as e:
            console.print(f"[red]HTTP error while updating batch:[/] {e}")
            return

        updated_count = 0

        for sym in symbols_batch:
            h5 = ohlcv_5m.get(sym, [])
            h1 = ohlcv_1h.get(sym, [])
            hoi = oi_1h.get(sym, [])
            hfr = fr_1h_hist.get(sym, [])
            hpfr = pfr_1h_hist.get(sym, [])

            if not h5 or not h1:
                continue

            closes_5 = [float(c["c"]) for c in h5]
            closes_1 = [float(c["c"]) for c in h1]

            price = closes_5[-1]
            rvol_5m = compute_rvol_5m(h5)
            mom_15m = compute_mom_from_closes(closes_5, bars_back=3)
            mom_60m = compute_mom_from_closes(closes_5, bars_back=12)
            mom_24h = compute_mom_24h_from_1h(closes_1)

            oi_ch_1h = compute_oi_change(hoi, hours_back=1)
            oi_ch_6h = compute_oi_change(hoi, hours_back=6)
            oi_ch_24h = compute_oi_change(hoi, hours_back=24)

            fr_last = last_c_from_history(hfr)
            pfr_last = last_c_from_history(hpfr)

            metrics = {
                "price": price,
                "rvol_5m": rvol_5m,
                "mom_15m": mom_15m,
                "mom_60m": mom_60m,
                "mom_24h": mom_24h,
                "oi_ch_1h": oi_ch_1h,
                "oi_ch_6h": oi_ch_6h,
                "oi_ch_24h": oi_ch_24h,
                "fr_1h": fr_last,
                "pfr_1h": pfr_last,
            }

            screener.update_symbol_metrics(sym, metrics)
            updated_count += 1

        console.log(
            f"[cyan]Updated metrics for {updated_count} symbols in batch "
            f"({len(symbols_batch)} requested)[/cyan]"
        )


async def engine_loop(universe_symbols: List[str]):
    if not universe_symbols:
        console.print("[red bold]No universe symbols – engine not started.[/red bold]")
        return

    batch_size = 20

    while True:
        start = time.time()
        console.log(
            f"[magenta]Coinalyze refresh round: {len(universe_symbols)} symbols[/magenta]"
        )

        batches = [
            universe_symbols[i : i + batch_size]
            for i in range(0, len(universe_symbols), batch_size)
        ]

        tasks = [update_metrics_for_batch(b) for b in batches]
        await asyncio.gather(*tasks)

        total_ready = len([s for s in screener.symbols.values() if s.price > 0])
        console.log(f"[green]Total symbols with data:[/green] {total_ready}")

        elapsed = time.time() - start
        sleep_for = max(5.0, REFRESH_SECONDS - elapsed)
        await asyncio.sleep(sleep_for)


# ======================================================================
#  UI HELPERS
# ======================================================================

def heat_bar(score: float, max_score: float, length: int, mode: str) -> str:
    if max_score <= 0:
        max_score = 1.0
    frac = clamp(score / max_score, 0.0, 1.0)
    filled = int(round(frac * length))
    empty = length - filled

    if mode == "green":
        fill_char = "🟩"
    elif mode == "gold":
        fill_char = "🟨"
    elif mode == "blue":
        fill_char = "🟦"
    else:
        fill_char = "🟥"

    empty_char = "⬛"
    return fill_char * filled + empty_char * empty


def style_pct(val: float) -> str:
    color = "white"
    if val > 0:
        color = "green" if val < 10 else "bold bright_green"
    elif val < 0:
        color = "red" if val > -10 else "bold bright_red"
    return f"[{color}]{val:>7.2f}%[/{color}]"


def style_x(val: float) -> str:
    color = "white"
    if val > 1:
        color = "green" if val < 5 else "bold bright_green"
    elif val < 1:
        color = "dim"
    return f"[{color}]{val:>5.2f}x[/{color}]"


def style_plain(val: float) -> str:
    return f"{val:>8.2f}"


def style_symbol(sym: str) -> str:
    return f"[bold white]{sym}[/bold white]"


# ======================================================================
#  TABLE BUILDERS (4 TABLES)
# ======================================================================

def build_table_pump20(frame: int) -> Table:
    table = Table(
        title="🏁 Top 5 Coins with “100% Chance” of a 20% Pump from Now (Heuristic)",
        box=box.SQUARE,
        expand=True,
        show_lines=True,
        style="bright_white",
        header_style="bold red",
    )
    table.add_column("Rank", justify="right", width=4)
    table.add_column("Symbol", width=18)
    table.add_column("Price", justify="right", width=12)
    table.add_column("Pump20 Score", justify="right", width=14)
    table.add_column("15m %", justify="right", width=10)
    table.add_column("24h %", justify="right", width=10)
    table.add_column("RVOL 5m", justify="right", width=10)
    table.add_column("Heat", width=16)
    table.add_column("Engine", width=12)

    top = screener.top_by(lambda s: s.pump20_score, limit=5)

    for i, s in enumerate(top, 1):
        blink = (frame // 4) % 2 == 0
        engine = "🟢 REV" if blink else "⚪ idle"
        heat = heat_bar(s.pump20_score, max_score=120, length=8, mode="green")

        table.add_row(
            str(i),
            style_symbol(s.symbol),
            f"{s.price:>11.5f}",
            style_plain(s.pump20_score),
            style_pct(s.mom_15m),
            style_pct(s.mom_24h),
            style_x(s.rvol_5m),
            heat,
            f"[bold green]{engine}[/bold green]" if blink else f"[dim]{engine}[/dim]",
        )
    return table


def build_table_gainer24(frame: int) -> Table:
    table = Table(
        title="🏆 Top 5 Coins with Highest Chance of Being Top Gainers (Next 24h)",
        box=box.SQUARE,
        expand=True,
        show_lines=True,
        style="bright_white",
        header_style="bold yellow",
    )
    table.add_column("Rank", justify="right", width=4)
    table.add_column("Symbol", width=18)
    table.add_column("Price", justify="right", width=12)
    table.add_column("Gainer24 Score", justify="right", width=16)
    table.add_column("24h %", justify="right", width=10)
    table.add_column("60m %", justify="right", width=10)
    table.add_column("OI 24h %", justify="right", width=10)
    table.add_column("Heat", width=16)

    top = screener.top_by(lambda s: s.gainer24_score, limit=5)

    for i, s in enumerate(top, 1):
        heat = heat_bar(s.gainer24_score, max_score=200, length=8, mode="gold")
        blink = (frame // 6) % 2 == 0
        rank_label = f"[bold]{i}[/bold]" if blink else f"{i}"

        table.add_row(
            rank_label,
            style_symbol(s.symbol),
            f"{s.price:>11.5f}",
            style_plain(s.gainer24_score),
            style_pct(s.mom_24h),
            style_pct(s.mom_60m),
            style_pct(s.oi_ch_24h),
            heat,
        )
    return table


def build_table_explosion(frame: int) -> Table:
    table = Table(
        title="💥 Top 5 Coins with Highest Chances of a Violent Breakout Explosion",
        box=box.SQUARE,
        expand=True,
        show_lines=True,
        style="bright_white",
        header_style="bold bright_red",
    )
    table.add_column("Rank", justify="right", width=4)
    table.add_column("Symbol", width=18)
    table.add_column("Price", justify="right", width=12)
    table.add_column("Explosion Score", justify="right", width=16)
    table.add_column("15m %", justify="right", width=10)
    table.add_column("60m %", justify="right", width=10)
    table.add_column("OI 1h %", justify="right", width=10)
    table.add_column("RVOL 5m", justify="right", width=10)
    table.add_column("Nitro", width=12)

    top = screener.top_by(lambda s: s.explosion_score, limit=5)

    for i, s in enumerate(top, 1):
        heat = heat_bar(s.explosion_score, max_score=150, length=8, mode="red")
        blink = (frame // 3) % 2 == 0
        nitro = "🔥 NITRO" if blink else "💤 sleeping"

        table.add_row(
            str(i),
            style_symbol(s.symbol),
            f"{s.price:>11.5f}",
            style_plain(s.explosion_score),
            style_pct(s.mom_15m),
            style_pct(s.mom_60m),
            style_pct(s.oi_ch_1h),
            style_x(s.rvol_5m),
            f"[bold red]{nitro}[/bold red]" if blink else f"[dim]{nitro}[/dim]",
        )
    return table


def build_table_losers24(frame: int) -> Table:
    table = Table(
        title="⚰️ Top 5 Coins with Highest Chance of Being Biggest Losers (Next 24h)",
        box=box.SQUARE,
        expand=True,
        show_lines=True,
        style="bright_white",
        header_style="bold blue",
    )
    table.add_column("Rank", justify="right", width=4)
    table.add_column("Symbol", width=18)
    table.add_column("Price", justify="right", width=12)
    table.add_column("Loser24 Score", justify="right", width=16)
    table.add_column("24h %", justify="right", width=10)
    table.add_column("60m %", justify="right", width=10)
    table.add_column("OI 24h %", justify="right", width=10)
    table.add_column("FR 1h", justify="right", width=10)
    table.add_column("Heat", width=16)

    top = screener.top_by(lambda s: s.loser24_score, limit=5)

    for i, s in enumerate(top, 1):
        heat = heat_bar(s.loser24_score, max_score=150, length=8, mode="blue")
        blink = (frame // 5) % 2 == 0
        rank_label = f"[bold]{i}[/bold]" if blink else f"{i}"

        table.add_row(
            rank_label,
            style_symbol(s.symbol),
            f"{s.price:>11.5f}",
            style_plain(s.loser24_score),
            style_pct(s.mom_24h),
            style_pct(s.mom_60m),
            style_pct(s.oi_ch_24h),
            f"{s.fr_1h:>8.4f}",
            heat,
        )
    return table


def build_layout(frame: int):
    header = Panel(
        Align.center(
            "[bold red]🏎️ Ferrari Breakout Screener[/bold red]  "
            "[yellow]|[/yellow]  [white]Coinalyze Futures • Docteja Core • LIVE[/white]",
            vertical="middle",
        ),
        style="bold red",
        padding=(0, 1),
        box=box.SQUARE_DOUBLE_HEAD,
    )

    t1 = build_table_pump20(frame)
    t2 = build_table_gainer24(frame)
    t3 = build_table_explosion(frame)
    t4 = build_table_losers24(frame)

    grid = Table.grid(padding=1)
    grid.add_row(t1)
    grid.add_row(t2)
    grid.add_row(t3)
    grid.add_row(t4)

    root = Table.grid(expand=True)
    root.add_row(header)
    root.add_row(grid)
    return root


# ======================================================================
#  RENDER LOOP
# ======================================================================

async def ui_loop():
    frame = 0
    layout = build_layout(frame)
    with Live(layout, console=console, refresh_per_second=4, screen=True) as live:
        while True:
            frame += 1
            live.update(build_layout(frame))
            await asyncio.sleep(0.25)


async def main():
    if COINALYZE_API_KEY == "PUT_YOUR_COINALYZE_KEY_HERE":
        console.print(
            "[red bold]ERROR:[/red bold] Please edit COINALYZE_API_KEY at the top of the script!"
        )
        return

    console.print("[cyan]Discovering Binance USDT perpetual futures from Coinalyze...[/cyan]")
    universe = await discover_futures_universe()

    engine = asyncio.create_task(engine_loop(universe))
    ui = asyncio.create_task(ui_loop())
    await asyncio.gather(engine, ui)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[red]Exiting Ferrari Coinalyze Docteja Screener...[/red]")
