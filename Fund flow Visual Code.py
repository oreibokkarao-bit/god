#!/usr/bin/env python3
# Segment Flow Tracker (CoinPaprika, no key)
# Groups + Sub-Segments + Color/Emojis + 24–48h Predictor
# FINAL ROTATION BOARD and PREDICTOR now include Top Sub-segments and Top-5 coins
#
# REFACTORED with `rich` to fix formatting (replaces tabulate + colorama)

import argparse, time, re, sys
from typing import Dict, List, Tuple
import requests
import pandas as pd

# --- NEW: Rich Imports ---
from rich.console import Console
from rich.table import Table
from rich.text import Text
from rich import box

# (All data fetching and computation functions are unchanged)

# ---------- Color helpers (NEW: `rich` version) ----------
# These functions now return `rich.text.Text` objects


def color_pct_rich(x) -> Text:
    """x is a fraction -> percent string with color."""
    try:
        v = float(x)
    except Exception:
        return Text(str(x), style="dim")
    s = f"{v*100:.2f}%"
    if v > 0.005:
        return Text(s, style="bright green")
    if v < -0.005:
        return Text(s, style="bright red")
    return Text(s, style="yellow")


def color_bp_rich(x) -> Text:
    """x is a fraction -> basis points text with color."""
    try:
        bp = float(x) * 100
    except Exception:
        return Text(str(x), style="dim")
    s = f"{bp:.2f} bp"
    if bp >= 5.0:
        return Text(s, style="bright green")
    if bp >= 2.0:
        return Text(s, style="bright yellow")
    if bp <= -2.0:
        return Text(s, style="bright red")
    return Text(s, style="cyan")


def _human_usd(x):
    try:
        x = float(x)
    except Exception:
        return "-"
    neg = x < 0
    x = abs(x)
    if x >= 1e12:
        s = f"{x/1e12:.2f}T"
    elif x >= 1e9:
        s = f"{x/1e9:.2f}B"
    elif x >= 1e6:
        s = f"{x/1e6:.2f}M"
    elif x >= 1e3:
        s = f"{x/1e3:.0f}K"
    else:
        s = f"{x:.0f}"
    return f"-{s}" if neg else s


def money_color_rich(val: float) -> Text:
    s = _human_usd(val)
    try:
        neg = float(val) < 0
    except Exception:
        neg = False
    style = "bright red" if neg else "bright green"
    return Text(s, style=style)


def signal_emoji(pctl, thrust, breadth, overheat=False):
    if overheat:
        return "⚠️"
    if pctl >= 0.80 and thrust > 0 and breadth >= 0.55:
        return "🚀"
    if pctl >= 0.65:
        return "📈"
    return "🧊"


def _percentile(series: pd.Series) -> pd.Series:
    return (
        (series.rank(method="average", pct=True)).fillna(0.0)
        if not series.empty
        else series
    )


# ---------- Formatters for Top-5 coins & Sub lists (NEW: `rich` version) ----------
def format_top5_rich(drivers: List[dict]) -> Text:
    """
    Returns a rich Text object, e.g.,
    'SOL:+120M (+0.82%), APT:+45M (+0.61%), ...'
    """
    if not drivers:
        return Text("—", style="dim")

    parts = Text()
    for i, d in enumerate(drivers[:5]):
        sym = d.get("sym", "?")
        d1 = d.get("d1", 0.0)
        p1 = d.get("p1", None)

        d1t = money_color_rich(d1)
        p1t = color_pct_rich((p1 / 100.0) if isinstance(p1, (int, float)) else 0.0)

        parts.append(f"{sym}:", style="bold")
        parts.append(d1t)
        parts.append(" (")
        parts.append(p1t)
        parts.append(")")
        if i < len(drivers[:5]) - 1:
            parts.append(", ")
    return parts


def format_top_subs_with_drivers_rich(
    sub_df: pd.DataFrame, k: int
) -> Tuple[Text, Text]:
    """
    Returns two multi-line Text objects:
      1) sub-lines: '🚀 AI Agents (83.1 pctl)', ...
      2) driver-lines: one line per sub with its Top-5 list
    """
    lines_subs = Text()
    lines_drv = Text()

    subs_to_show = sub_df.head(k)
    if subs_to_show.empty:
        return Text("—", style="dim"), Text("—", style="dim")

    for i, (_, sr) in enumerate(subs_to_show.iterrows()):
        emj = signal_emoji(
            sr.get("pred_pctl_sub", 0.0),
            sr.get("1h_pct_of_mcap", 0.0),
            sr.get("breadth_1h", 0.0),
        )
        pctl = sr.get("pred_pctl_sub", 0.0) * 100.0
        sub_label = f"{emj} {sr.get('subsegment','?')} ({pctl:.1f} pctl)"

        lines_subs.append(sub_label)
        lines_drv.append(format_top5_rich(sr.get("top5", [])))

        if i < len(subs_to_show) - 1:
            lines_subs.append("\n")
            lines_drv.append("\n")

    return lines_subs, lines_drv


# ---------- Data (CoinPaprika, keyless) ----------
# (This section is unchanged)
BASE = "https://api.coinpaprika.com/v1"
HEADERS = {"Accept": "application/json"}
REQ_INTERVAL = 0.6
MAX_RETRIES = 4


def _get(path: str, params: dict | None = None):
    url = f"{BASE}{path}"
    backoff = REQ_INTERVAL
    for _ in range(MAX_RETRIES):
        try:
            r = requests.get(url, params=params or {}, headers=HEADERS, timeout=40)
            if r.status_code == 429:
                time.sleep(backoff)
                backoff *= 1.8
                continue
            r.raise_for_status()
            if "json" not in (r.headers.get("content-type") or "").lower():
                time.sleep(backoff)
                backoff *= 1.4
                continue
            return r.json()
        except Exception:
            time.sleep(backoff)
            backoff *= 1.6
    return None


def fetch_all_tickers(quotes="USD") -> pd.DataFrame:
    data = _get("/tickers", params={"quotes": quotes})  # %change 1h/24h/7d + mcap/vol
    if not isinstance(data, list):
        return pd.DataFrame()
    rows = []
    for c in data:
        q = (c.get("quotes") or {}).get(quotes, {})
        rows.append(
            {
                "id": c.get("id"),
                "name": c.get("name"),
                "symbol": c.get("symbol"),
                "mcap": q.get("market_cap"),
                "vol24h": q.get("volume_24h"),
                "pct_1h": q.get("percent_change_1h"),
                "pct_24h": q.get("percent_change_24h"),
                "pct_7d": q.get("percent_change_7d"),
            }
        )
    return pd.DataFrame(rows).dropna(subset=["id", "mcap"]).reset_index(drop=True)


def fetch_tags_with_coins() -> pd.DataFrame:
    data = _get(
        "/tags", params={"additional_fields": "coins"}
    )  # include coin IDs per tag
    if not isinstance(data, list):
        return pd.DataFrame()
    rows = []
    for t in data:
        rows.append(
            {
                "tag_id": t.get("id"),
                "tag_name": t.get("name"),
                "type": t.get("type"),
                "coin_ids": t.get("coins") or [],
                "coin_count": len(t.get("coins") or []),
                "description": t.get("description"),
            }
        )
    return pd.DataFrame(rows)


# ---------- Tag → (Group, Sub) classifier ----------
# (This section is unchanged)
_PATTERNS = {
    "Tech": {
        "AI": [
            r"\bartificial intelligence\b",
            r"\bai\b",
            r"\bai[- ]agents?\b",
            r"\bagents?\b",
            r"\bmachine learning\b",
        ],
        "Data/Storage": [
            r"big data",
            r"data storage",
            r"decentralized storage",
            r"\bfile\b",
            r"ipfs",
            r"arweave",
            r"storage",
        ],
        "Privacy/Security": [
            r"privacy",
            r"privacy & security",
            r"\bsecurity\b",
            r"\bzkp\b",
            r"\bzero[- ]knowledge\b",
            r"mixers?",
        ],
        "Oracles/Middleware": [
            r"oracle",
            r"middleware",
            r"indexing",
            r"\bsubgraph",
            r"the graph",
        ],
        "Compute": [r"distributed computing", r"comput(?:e|ing)", r"cloud"],
    },
    "Scaling": {
        "Layer 2 (L2)": [
            r"\blayer[ -]?2\b",
            r"\bl2\b",
            r"rollups?",
            r"arbitrum",
            r"optimism",
            r"\bbase\b",
            r"stark",
            r"linea",
            r"\bzks?",
        ],
        "Layer 0 / Interop": [
            r"\blayer[ -]?0\b",
            r"\bl0\b",
            r"interoperability",
            r"cosmos",
            r"ibc",
            r"polkadot",
            r"relay",
        ],
        "Sidechains": [r"side[- ]?chain"],
    },
    "DeFi": {
        "DEX/Perps": [r"\bdex\b", r"exchange", r"\bamm\b", r"perp", r"perpetual"],
        "Lending/Money Markets": [r"lending", r"money market", r"aave", r"compound"],
        "Stablecoins": [r"stable ?coin"],
        "Yield/Derivatives": [
            r"yield",
            r"farming",
            r"structured",
            r"options?",
            r"derivatives?",
        ],
    },
    "RWA": {"Real World Assets": [r"real world assets", r"\brwa\b"]},
    "Web3/Media": {
        "Social/Comms": [
            r"social",
            r"communication",
            r"comms",
            r"media",
            r"publishing",
        ],
        "Metaverse/Gaming": [
            r"metaverse",
            r"gaming",
            r"gamefi",
            r"play[- ]?to[- ]?earn",
        ],
    },
    "Memes": {"Memecoins": [r"\bmeme\b"]},
    "Infrastructure": {
        "Chain Infra": [
            r"infrastructure",
            r"blockchain service",
            r"smart contract",
            r"platform",
        ]
    },
}


def classify_tag(name: str) -> Tuple[str, str]:
    n = (name or "").lower()
    for group, submap in _PATTERNS.items():
        for sub, pats in submap.items():
            if any(re.search(p, n) for p in pats):
                return group, sub
    return "Other", "Other"


# ---------- Core compute ----------
# (This section is unchanged)
def compute(all_functional_only=True, min_coins=6):
    tick = fetch_all_tickers("USD")
    tags = fetch_tags_with_coins()
    if tick.empty or tags.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    if all_functional_only:
        tags = tags[tags["type"] == "functional"].copy()

    tick = tick.set_index("id")
    mcap = tick["mcap"].to_dict()
    vol24 = tick["vol24h"].to_dict()
    p1h = tick["pct_1h"].to_dict()
    p24 = tick["pct_24h"].to_dict()
    p7d = tick["pct_7d"].to_dict()

    sub_rows = []
    for _, row in tags.iterrows():
        ids = [cid for cid in (row["coin_ids"] or []) if cid in mcap]
        if len(ids) < min_coins:
            continue

        total_mcap = sum(float(mcap[i] or 0) for i in ids)
        if total_mcap <= 0:
            continue
        total_vol = sum(float(vol24.get(i) or 0) for i in ids)

        flows = {"1h": 0.0, "24h": 0.0, "7d": 0.0}
        pos1h = pos24 = 0
        mcaps_list = []
        drivers = []

        for i in ids:
            mc = float(mcap.get(i) or 0.0)
            mcaps_list.append(mc)

            if isinstance(p1h.get(i), (int, float)):
                flows["1h"] += mc * (p1h[i] / 100.0)
            if isinstance(p24.get(i), (int, float)):
                flows["24h"] += mc * (p24[i] / 100.0)
            if isinstance(p7d.get(i), (int, float)):
                flows["7d"] += mc * (p7d[i] / 100.0)

            pos1h += 1 if isinstance(p1h.get(i), (int, float)) and p1h[i] > 0 else 0
            pos24 += 1 if isinstance(p24.get(i), (int, float)) and p24[i] > 0 else 0

            p = p1h.get(i)
            d1 = mc * (float(p) / 100.0) if isinstance(p, (int, float)) else 0.0
            sym = tick.loc[i, "symbol"] if i in tick.index else i
            drivers.append(
                {"sym": sym, "d1": d1, "p1": p if isinstance(p, (int, float)) else None}
            )

        drivers.sort(key=lambda x: abs(x["d1"]), reverse=True)
        top5 = drivers[:5]

        breadth_1h = pos1h / len(ids)
        breadth_24h = pos24 / len(ids)
        liq = (total_vol / total_mcap) if total_mcap > 0 else 0.0
        mcaps_list.sort(reverse=True)
        top5_share = (sum(mcaps_list[:5]) / total_mcap) if total_mcap > 0 else 1.0

        grp, subgrp = classify_tag(row["tag_name"])
        sub_rows.append(
            {
                "group": grp,
                "subsegment": row["tag_name"],
                "subgroup": subgrp,
                "mcap": total_mcap,
                "vol24h": total_vol,
                "flow_1h": flows["1h"],
                "flow_24h": flows["24h"],
                "flow_7d": flows["7d"],
                "1h_pct_of_mcap": flows["1h"] / total_mcap,
                "24h_pct_of_mcap": flows["24h"] / total_mcap,
                "7d_pct_of_mcap": flows["7d"] / total_mcap,
                "breadth_1h": breadth_1h,
                "breadth_24h": breadth_24h,
                "liq_vmc": liq,
                "top5_share": top5_share,
                "top5": top5,
            }
        )

    subs = pd.DataFrame(sub_rows)
    if subs.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    subs["flow_score"] = 0.6 * subs["1h_pct_of_mcap"] + 0.4 * subs["24h_pct_of_mcap"]

    # predictor per sub (percentile-blend)
    subs["accel_ratio"] = subs["1h_pct_of_mcap"] / (
        subs["24h_pct_of_mcap"].abs() + 1e-9
    )
    subs["recovery_pos"] = subs["7d_pct_of_mcap"].clip(upper=0).abs()
    subs["overheat_pos"] = subs["7d_pct_of_mcap"].clip(lower=0)

    thrust_p = _percentile(subs["1h_pct_of_mcap"])
    persist_p = _percentile(subs["24h_pct_of_mcap"])
    accel_p = _percentile(subs["accel_ratio"])
    breadth_p = _percentile(subs["breadth_1h"])
    liq_p = _percentile(subs["liq_vmc"])
    recover_p = _percentile(subs["recovery_pos"])
    overheat_p = _percentile(subs["overheat_pos"])

    subs["pred_score"] = (
        0.35 * thrust_p
        + 0.25 * persist_p
        + 0.15 * accel_p
        + 0.10 * breadth_p
        + 0.08 * liq_p
        + 0.10 * recover_p
        - 0.13 * overheat_p
    )
    subs["pred_pctl_sub"] = _percentile(subs["pred_score"])

    # group aggregates (flows)
    groups = subs.groupby("group", as_index=False).agg(
        {
            "mcap": "sum",
            "vol24h": "sum",
            "flow_1h": "sum",
            "flow_24h": "sum",
            "flow_7d": "sum",
        }
    )
    for tf in ["1h", "24h", "7d"]:
        groups[f"{tf}_pct_of_mcap"] = groups[f"flow_{tf}"] / groups["mcap"]
    groups["flow_score"] = (
        0.6 * groups["1h_pct_of_mcap"] + 0.4 * groups["24h_pct_of_mcap"]
    )

    # group predictor via weighted sums
    gp = subs.copy()
    gp["w"] = gp["mcap"].clip(lower=1.0)
    gp["w_pred"] = gp["pred_score"] * gp["w"]
    gp["w_thru"] = gp["1h_pct_of_mcap"] * gp["w"]
    gp["w_pers"] = gp["24h_pct_of_mcap"] * gp["w"]
    gp["w_br"] = gp["breadth_1h"] * gp["w"]
    gp["w_liq"] = gp["liq_vmc"] * gp["w"]

    sums = gp.groupby("group", as_index=False)[
        ["mcap", "vol24h", "w", "w_pred", "w_thru", "w_pers", "w_br", "w_liq"]
    ].sum()

    pred_group = sums.copy()
    pred_group["pred_score"] = pred_group["w_pred"] / pred_group["w"]
    pred_group["thrust"] = pred_group["w_thru"] / pred_group["w"]
    pred_group["persist"] = pred_group["w_pers"] / pred_group["w"]
    pred_group["breadth"] = pred_group["w_br"] / pred_group["w"]
    pred_group["liq_vmc"] = pred_group["w_liq"] / pred_group["w"]
    pred_group = pred_group[
        [
            "group",
            "mcap",
            "vol24h",
            "pred_score",
            "thrust",
            "persist",
            "breadth",
            "liq_vmc",
        ]
    ]
    pred_group["pred_pctl_grp"] = _percentile(pred_group["pred_score"])

    # sort for presentation
    groups = groups.sort_values("flow_score", ascending=False).reset_index(drop=True)
    subs = subs.sort_values(
        ["group", "flow_score"], ascending=[True, False]
    ).reset_index(drop=True)
    pred_group = pred_group.sort_values("pred_score", ascending=False).reset_index(
        drop=True
    )

    return groups, subs, pred_group, subs  # return subs twice for reuse


# ---------- Printers (NEW: `rich` version) ----------

# --- NEW: Helper functions for rich table cell styling ---


def color_pred_score(pctl) -> Text:
    """Colors the percentile score"""
    s = f"{pctl*100:.1f} pctl"
    if pctl >= 0.65:
        return Text(s, style="bright green")
    if pctl >= 0.45:
        return Text(s, style="bright yellow")
    return Text(s, style="bright red")


def color_breadth(breadth) -> Text:
    """Colors the breadth percentage"""
    s = f"{breadth*100:.1f}%"
    if breadth >= 0.60:
        return Text(s, style="bright green")
    if breadth >= 0.40:
        return Text(s, style="bright yellow")
    return Text(s, style="bright red")


def color_liq(liq) -> Text:
    """Colors the liquidity"""
    return Text(f"{liq:.3f}", style="magenta")


def print_boards_rich(
    console: Console,
    groups,
    subs,
    pred_group,
    pred_subs,
    top_groups=8,
    top_subs=5,
    pred_subs_per_group=3,
):
    """
    New print function using rich.console.Console and rich.table.Table
    """

    TABLE_BOX = box.SQUARE  # Use box.SQUARE for a grid-like look

    # A) GROUP LEADERBOARD
    console.print("\n=== GROUP LEADERBOARD (by Flow Score) ===", style="bold cyan")
    g_table = Table(
        show_header=True, header_style="bold magenta", box=TABLE_BOX, padding=(0, 1)
    )
    g_table.add_column("Group", style="bold white", no_wrap=True)
    g_table.add_column("MCap", justify="right")
    g_table.add_column("Vol24h", justify="right")
    g_table.add_column("Score", justify="right")
    g_table.add_column("1h Δ($)", justify="right")
    g_table.add_column("1h Δ%Mc", justify="right")
    g_table.add_column("24h Δ($)", justify="right")
    g_table.add_column("24h Δ%Mc", justify="right")
    g_table.add_column("7d Δ($)", justify="right")
    g_table.add_column("7d Δ%Mc", justify="right")

    g = groups.head(top_groups).copy()
    for _, r in g.iterrows():
        g_table.add_row(
            r["group"],
            _human_usd(r["mcap"]),
            _human_usd(r["vol24h"]),
            color_bp_rich(r["flow_score"]),
            money_color_rich(r["flow_1h"]),
            color_pct_rich(r["1h_pct_of_mcap"]),
            money_color_rich(r["flow_24h"]),
            color_pct_rich(r["24h_pct_of_mcap"]),
            money_color_rich(r["flow_7d"]),
            color_pct_rich(r["7d_pct_of_mcap"]),
        )
    console.print(g_table)

    # B) TOP SUB-SEGMENTS (per Group)
    console.print("\n=== TOP SUB-SEGMENTS (per Group) ===", style="bold cyan")
    s_table = Table(
        show_header=True, header_style="bold magenta", box=TABLE_BOX, padding=(0, 1)
    )
    s_table.add_column("Group", style="bold white", no_wrap=True)
    s_table.add_column("Sub-segment", no_wrap=True)
    s_table.add_column("MCap", justify="right")
    s_table.add_column("Vol24h", justify="right")
    s_table.add_column("Score", justify="right")
    s_table.add_column("1h Δ($)", justify="right")
    s_table.add_column("1h Δ%Mc", justify="right")
    s_table.add_column("24h Δ($)", justify="right")
    s_table.add_column("24h Δ%Mc", justify="right")
    s_table.add_column("7d Δ($)", justify="right")
    s_table.add_column("7d Δ%Mc", justify="right")

    has_subs = False
    for gname in g["group"].tolist():
        topk = subs[subs["group"] == gname].head(top_subs)
        for _, r in topk.iterrows():
            has_subs = True
            s_table.add_row(
                gname,
                r["subsegment"],
                _human_usd(r["mcap"]),
                _human_usd(r["vol24h"]),
                color_bp_rich(r["flow_score"]),
                money_color_rich(r["flow_1h"]),
                color_pct_rich(r["1h_pct_of_mcap"]),
                money_color_rich(r["flow_24h"]),
                color_pct_rich(r["24h_pct_of_mcap"]),
                money_color_rich(r["flow_7d"]),
                color_pct_rich(r["7d_pct_of_mcap"]),
            )
    if has_subs:
        console.print(s_table)

    # C) NEXT–24–48h PREDICTOR (Group-level)
    console.print("\n=== NEXT–24–48h PREDICTOR (Group-level) ===", style="bold cyan")
    p_table = Table(
        show_header=True, header_style="bold magenta", box=TABLE_BOX, padding=(0, 1)
    )
    p_table.add_column("Group (Signal)", style="bold white")
    p_table.add_column("MCap", justify="right")
    p_table.add_column("Vol24h", justify="right")
    p_table.add_column("PredScore", justify="right")
    p_table.add_column("Thrust(1h)", justify="right")
    p_table.add_column("Persist(24h)", justify="right")
    p_table.add_column("Breadth(1h)", justify="right")
    p_table.add_column("Liq V/Mc", justify="right")
    p_table.add_column(
        "Top Sub-segments (PredScore)"
    )  # This will align left by default
    p_table.add_column("Drivers: Top-5 coins / sub")  # This will align left by default

    pg = pred_group.copy()
    pg["pred_pctl"] = _percentile(pg["pred_score"])

    for _, r in pg.iterrows():
        gname = r["group"]
        emj = signal_emoji(r["pred_pctl"], r["thrust"], r["breadth"])

        # Top sub-segments for this group by sub PredScore
        sub_slice = (
            pred_subs[pred_subs["group"] == gname]
            .sort_values("pred_score", ascending=False)
            .copy()
        )

        # THIS IS THE KEY FIX:
        # These functions return rich.text.Text objects.
        # The Table will handle their multi-line alignment perfectly.
        sub_names_block, drivers_block = format_top_subs_with_drivers_rich(
            sub_slice, pred_subs_per_group
        )

        p_table.add_row(
            f"{emj} {gname}",
            _human_usd(r["mcap"]),
            _human_usd(r["vol24h"]),
            color_pred_score(r["pred_pctl"]),
            color_pct_rich(r["thrust"]),
            color_pct_rich(r["persist"]),
            color_breadth(r["breadth"]),
            color_liq(r["liq_vmc"]),
            sub_names_block,
            drivers_block,
        )
    console.print(p_table)

    # D) FINAL ROTATION BOARD (Groups with Sub-segments + Top-5 coins)
    console.print(
        "\n=== FINAL ROTATION BOARD (Groups with Sub-segments) ===", style="bold cyan"
    )
    f_table = Table(
        show_header=True, header_style="bold magenta", box=TABLE_BOX, padding=(0, 1)
    )
    f_table.add_column("Level", style="dim")
    f_table.add_column("Name (Signal)", style="bold white")
    f_table.add_column("MCap", justify="right")
    f_table.add_column("Vol24h", justify="right")
    f_table.add_column("FlowScore", justify="right")
    f_table.add_column("PredScore(24–48h)", justify="right")
    f_table.add_column("Thrust(1h)", justify="right")
    f_table.add_column("Persist(24h)", justify="right")
    f_table.add_column("Breadth(1h)", justify="right")
    f_table.add_column("Liq V/Mc", justify="right")
    f_table.add_column("Top 5 Coins (1h Δ / %)")

    pred_subs = pred_subs.copy()
    pred_subs["pred_pctl_sub"] = _percentile(pred_subs["pred_score"])

    for _, gr in pg.iterrows():  # Using same sorted group df 'pg' from above
        gname = gr["group"]
        g_emj = signal_emoji(gr["pred_pctl"], gr["thrust"], gr["breadth"])
        flow_sc = groups.loc[groups["group"] == gname, "flow_score"].item()

        # Add GROUP row
        f_table.add_row(
            "GROUP",
            f"{g_emj} {gname}",
            _human_usd(gr["mcap"]),
            _human_usd(gr["vol24h"]),
            color_bp_rich(flow_sc),
            color_pred_score(gr["pred_pctl"]),
            color_pct_rich(gr["thrust"]),
            color_pct_rich(gr["persist"]),
            color_breadth(gr["breadth"]),
            color_liq(gr["liq_vmc"]),
            Text("—", style="dim"),
            style="on grey19",  # Highlight the group row
        )

        # Add TOP-K SUB rows
        topk_subs = (
            pred_subs[pred_subs["group"] == gname]
            .sort_values("pred_score", ascending=False)
            .head(top_subs)
        )
        for _, sr in topk_subs.iterrows():
            s_emj = signal_emoji(
                sr["pred_pctl_sub"], sr["1h_pct_of_mcap"], sr["breadth_1h"]
            )
            f_table.add_row(
                "↳ SUB",
                f"{s_emj} {sr['subsegment']}",
                _human_usd(sr["mcap"]),
                _human_usd(sr["vol24h"]),
                color_bp_rich(sr["flow_score"]),
                color_pred_score(sr["pred_pctl_sub"]),
                color_pct_rich(sr["1h_pct_of_mcap"]),
                color_pct_rich(sr["24h_pct_of_mcap"]),
                color_breadth(sr["breadth_1h"]),
                color_liq(sr["liq_vmc"]),
                format_top5_rich(sr.get("top5", [])),
            )
    console.print(f_table)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--top_groups", type=int, default=8)
    ap.add_argument("--top_subs", type=int, default=5)
    ap.add_argument("--min_coins", type=int, default=6)
    ap.add_argument("--include_technical", action="store_true")
    ap.add_argument(
        "--pred_subs_per_group",
        type=int,
        default=3,
        help="#sub-segments to show per group in PREDICTOR table",
    )
    ap.add_argument("--no_color", action="store_true")
    args = ap.parse_args()

    # --- NEW: `rich` Console setup ---
    # This automatically handles color detection AND respects the --no_color flag.
    console = Console(no_color=args.no_color)

    groups, subs, pred_group, pred_subs = compute(
        all_functional_only=(not args.include_technical), min_coins=args.min_coins
    )
    if groups.empty:
        console.print(
            "No data (network or rate-limit). Re-run in a minute.", style="bold red"
        )
        return

    # --- NEW: Call the rich printer function ---
    print_boards_rich(
        console,
        groups,
        subs,
        pred_group,
        pred_subs,
        top_groups=args.top_groups,
        top_subs=args.top_subs,
        pred_subs_per_group=args.pred_subs_per_group,
    )


if __name__ == "__main__":
    main()
