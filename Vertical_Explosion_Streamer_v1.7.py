#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================================
# Vertical_Explosion_Streamer_v1.7.py
#
# v1.7 (Safe Mode) Changelog:
# - [NEW] Added OI Delta Gate: `passes_gate` now requires `oi_delta > 0`
#   to prevent longing into an OI dump (longs closing).
# - [NEW] Added Bollinger Band Expansion Filter: Added `MAX_BB_EXPANSION_PCT`
#   tunable and `calc_bollinger_expansion_pct` function to block entries
#   if volatility is already too high (prevents V-reversal traps).
# - [FIX] Added `bb_exp` to `self.top` dict for debugging.
#
# v1.6 Changelog:
# - [CRITICAL FIX] Solved long-term stability issues (ping-pong timeouts).
# - [FIX] Enhanced `init_exchanges` with explicit keepalive, retry,
#   and data-reduction options for `ccxt.pro` to stabilize connections.
# - [FIX] Increased `SUBSCRIPTION_DELAY` from 0.20s to 1.0s to prevent
#   rate-limiting by exchanges like Bitget (error 30006).
# - [FIX] Added `aiohttp` import, a core dependency for `ccxt.pro`.
# ============================================================================

import os, asyncio, json, random, time, subprocess, sys
from collections import deque
from pathlib import Path
from typing import Optional, Dict, Any

import ccxt.pro as ccxt
import pandas as pd
import numpy as np
from tabulate import tabulate
from colorama import Fore, Style, init as colorama_init
import aiohttp # <--- v1.6: Explicitly import ccxt's dependency

colorama_init(autoreset=True)

# --- Numeric sanitizers ---
def safe_float(x, default=None):
    try:
        if x is None: return default
        return float(x)
    except (TypeError, ValueError):
        return default

def valid_ohlcv_tuple(tup):
    if not isinstance(tup, (list, tuple)) or len(tup) < 6: return False
    ts, o, h, l, c, v = tup[:6]
    return (ts is not None
            and safe_float(o) is not None and safe_float(h) is not None
            and safe_float(l) is not None and safe_float(c) is not None
            and safe_float(v) is not None)

# --- Tunables ---
# v1.6: Increased delay to 1.0s to avoid rate-limits
SUBSCRIPTION_DELAY = 1.0
PRINT_INTERVAL = 12

# --- [NEW v1.7] Volatility Trap Filter ---
# Blocks entry if BB % width is wider than this.
# 8.0 = 8% wide. This prevents longing a "V-reversal" candle.
MAX_BB_EXPANSION_PCT = 8.0

# --- Hysteresis & Cooldown ---
OPEN_THR_15M = 2.2
CLOSE_THR_15M = 1.6
OPEN_THR_1H  = 2.0
CLOSE_THR_1H = 1.4
OPEN_THR_4H  = 2.4
CLOSE_THR_4H = 1.8
MIN_HOLD_SEC = 60
COOLDOWN_SEC = 180

# --- Mac Ding ---
ENABLE_SOUND = True
SOUND_PATH = "/System/Library/Sounds/Glass.aiff"
SOUND_RATE_LIMIT_SEC = 2.0
_LAST_SOUND_TS = 0.0

def _mac_ding():
    global _LAST_SOUND_TS
    if not ENABLE_SOUND or not os.path.exists(SOUND_PATH): return
    now = time.time()
    if now - _LAST_SOUND_TS < SOUND_RATE_LIMIT_SEC: return
    try:
        subprocess.Popen(["afplay", SOUND_PATH], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        _LAST_SOUND_TS = now
    except Exception: pass

def _fingerprint_top_dual(rows: list) -> tuple:
    return tuple(
        (r["symbol"], round(r["score"], 3), round(r["entry"], 6), round(r["tp1"], 6), round(r.get("tp2", 0.0), 6), round(r.get("tp3", 0.0), 6))
        for r in rows[:5]
    )

# --- Screener Configuration Class ---
class ScreenerConfig:
    def __init__(
        self, name, tf_ccxt, tf_pandas, backfill, z_period, fast_ma, slow_ma,
        min_quote_vol, gate_z,
        price_atr_w=0.0, oi_w=0.0, cvd_w=0.0, vol_w=0.0,
        risk_mult=0.6, sl_buf=0.2, tp2_mul=2.0, tp3_mul=3.0,
    ):
        self.name = name
        self.tf_ccxt = tf_ccxt     # Timeframe for ccxt (e.g., '15m')
        self.tf_pandas = tf_pandas # Timeframe for pandas (e.g., '15min')
        self.backfill = backfill
        self.z_period = z_period
        self.fast_ma = fast_ma
        self.slow_ma = slow_ma
        self.min_quote_vol = min_quote_vol
        self.gate_z = gate_z
        self.price_atr_w = price_atr_w
        self.oi_w = oi_w
        self.cvd_w = cvd_w
        self.vol_w = vol_w
        self.risk_mult = risk_mult
        self.sl_buf = sl_buf
        self.tp2_mul = tp2_mul
        self.tp3_mul = tp3_mul

# *** VEC v16 Strategy Config (on 15m TF) ***
CFG_VEC_15M = ScreenerConfig(
    "VEC 15m Stream", "15m", "15min", 120, 20, 5, 20,
    min_quote_vol=500_000,
    gate_z=0.5,
    price_atr_w=0.4, # VEC's "Price/ATR" score
    oi_w=0.3,        # VEC's "OI Spike" score
    cvd_w=0.3,       # VEC's "CVD" score (using True Taker)
    vol_w=0.0,
    risk_mult=0.8, sl_buf=0.25, tp2_mul=2.2, tp3_mul=3.3
)

# *** VEC v16 Strategy Config (on 1h TF) ***
CFG_VEC_1H = ScreenerConfig(
    "VEC 1h Stream", "1h", "1H", 100, 20, 5, 20,
    min_quote_vol=1_000_000,
    gate_z=0.5,
    price_atr_w=0.4, # VEC's "Price/ATR" score
    oi_w=0.3,        # VEC's "OI Spike" score
    cvd_w=0.3,       # VEC's "CVD" score (using True Taker)
    vol_w=0.0,
    risk_mult=0.8, sl_buf=0.25, tp2_mul=2.2, tp3_mul=3.3
)


# --- Core Technical Analysis Functions ---

def calc_bollinger_expansion_pct(df: pd.DataFrame, period: int = 20, mult: float = 2.0) -> float:
    """ [NEW v1.7] Calculates the BB Width as a % of the SMA."""
    if len(df) < period:
        return 999.0  # Return a high number if not enough data
    
    close_series = df["close"].tail(period)
    sma = close_series.mean()
    std = close_series.std()
    
    if sma == 0 or std == 0 or not np.isfinite(sma) or not np.isfinite(std):
        return 0.0 # Not enough data or bad data
        
    upper = sma + mult * std
    lower = sma - mult * std
    width_pct = ((upper - lower) / sma) * 100
    return float(width_pct)

def calc_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    if df.empty: return pd.Series(dtype="float64")
    d = df[["high", "low", "close"]].copy()
    d["h-l"] = d["high"] - d["low"]
    d["h-pc"] = (d["high"] - d["close"].shift(1)).abs()
    d["l-pc"] = (d["low"] - d["close"].shift(1)).abs()
    tr = d[["h-l", "h-pc", "l-pc"]].max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()

def vol_profile(df: pd.DataFrame, bins: int = 50):
    if df.empty: return None
    pmin = float(df["low"].min()); pmax = float(df["high"].max())
    if not np.isfinite(pmin) or not np.isfinite(pmax) or pmax <= pmin: return None
    step = (pmax - pmin) / bins
    edges = np.arange(pmin, pmax + step, step)
    vols = np.zeros(len(edges))
    for _, r in df.iterrows():
        lo, hi, v = float(r["low"]), float(r["high"]), float(r["volume"])
        if not np.isfinite(lo) or not np.isfinite(hi) or hi <= lo: continue
        i0 = np.searchsorted(edges, lo, "left"); i1 = np.searchsorted(edges, hi, "right") - 1
        i0 = max(0, min(i0, len(edges) - 1)); i1 = max(0, min(i1, len(edges) - 1))
        span = max(1, i1 - i0 + 1)
        vols[i0 : i1 + 1] += v / span
    poc_idx = int(np.argmax(vols)); poc = float(edges[poc_idx])
    tgt = float(vols.sum() * 0.7); L = R = poc_idx; acc = float(vols[poc_idx])
    while acc < tgt:
        lv = vols[L - 1] if L - 1 >= 0 else -np.inf
        rv = vols[R + 1] if R + 1 < len(vols) else -np.inf
        if lv >= rv and np.isfinite(lv): L -= 1; acc += lv
        elif np.isfinite(rv): R += 1; acc += rv
        else: break
    val = float(edges[max(0, L)]); vah = float(edges[min(R, len(edges) - 1)])
    avg = float(vols.sum() / bins) if bins else 0.0
    hvns = [float(edges[i]) for i in range(1, len(vols)-1) if vols[i] > vols[i-1] and vols[i] > vols[i+1] and vols[i] > 1.5 * avg]
    hvns = sorted([x for x in hvns if x > vah or x < val])
    return {"poc": poc, "vah": vah, "val": val, "hvns": hvns}


# --- Screener Class ---
class Screener:
    def __init__(self, cfg: ScreenerConfig, exchanges: dict, symbols: list):
        self.cfg = cfg
        self.exchanges = exchanges
        self.symbols = symbols
        self.state = {}
        self.top = []
        self.lock = asyncio.Lock()
        self.last_sig = None
        self.log_prefix = f"{Fore.BLUE}[{self.cfg.name}]{Style.RESET_ALL}"

    async def backfill_symbol(self, ex, symbol: str):
        try:
            ohlcv = await ex.fetch_ohlcv(symbol, timeframe=self.cfg.tf_ccxt, limit=self.cfg.backfill + 2)
            if not ohlcv or len(ohlcv) < 10: return None
            df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
            df.set_index("timestamp", inplace=True)
            oi_series = None
            if getattr(ex, "has", {}).get("fetchOpenInterestHistory", False):
                try:
                    oi_data = await ex.fetch_open_interest_history(symbol, timeframe=self.cfg.tf_ccxt, limit=self.cfg.backfill + 2)
                    if oi_data:
                        oi_df = pd.DataFrame(oi_data)
                        if "timestamp" in oi_df and "openInterestValue" in oi_df:
                            oi_df["timestamp"] = pd.to_datetime(oi_df["timestamp"], unit="ms")
                            oi_df.set_index("timestamp", inplace=True)
                            oi_series = (oi_df["openInterestValue"].astype(float)
                                         .resample(self.cfg.tf_pandas, label="right", closed="right").last())
                except Exception: oi_series = None
            if oi_series is None: oi_series = pd.Series(index=df.index, data=np.nan, dtype="float64")
            df["openInterestValue"] = oi_series.reindex(df.index).ffill()
            df["cvd"] = 0.0
            df = df[["open", "high", "low", "close", "volume", "openInterestValue", "cvd"]].dropna()
            return df
        except Exception as e:
            if "RateLimitExceeded" not in str(e) and "Invalid interval" not in str(e) and "invalid literal" not in str(e):
                print(f"{Fore.RED}Backfill error {ex.id} {symbol}: {e}{Style.RESET_ALL}")
            return None

    async def initialize(self):
        print(f"{self.log_prefix} Backfilling {self.cfg.backfill} bars per venue…")
        sem = asyncio.Semaphore(10)
        async def agg_one(symbol: str):
            async with sem:
                dfs = []
                for ex_id in EXCHANGES_CFG.keys():
                    ex = self.exchanges[ex_id]
                    df = await self.backfill_symbol(ex, symbol)
                    if df is not None and not df.empty:
                        df = df.copy(); df[f"vol_{ex_id}"] = df["volume"]; dfs.append(df)
                if not dfs: return
                base = dfs[0].copy()
                for add in dfs[1:]:
                    base = base.join(add[["volume", "openInterestValue"]], how="outer", rsuffix="_r")
                    base["volume"] = base[["volume", "volume_r"]].sum(axis=1, skipna=True)
                    base.drop(columns=["volume_r"], inplace=True)
                    base["openInterestValue"] = base[["openInterestValue", "openInterestValue_r"]].sum(axis=1, skipna=True)
                    base.drop(columns=["openInterestValue_r"], inplace=True)
                base = base[["open","high","low","close","volume","openInterestValue","cvd"]].sort_index().ffill().dropna()
                if base["volume"].iloc[-1] < self.cfg.min_quote_vol: return
                self.state[symbol] = {
                    "lock": asyncio.Lock(),
                    "last_update": int(base.index[-1].value // 1_000_000),
                    "df_deque": deque(
                        [{
                            "timestamp": int(ts.value // 1_000_000), "open": float(row["open"]),
                            "high": float(row["high"]), "low": float(row["low"]),
                            "close": float(row["close"]), "volume": float(row["volume"]),
                            "openInterestValue": float(row["openInterestValue"]),
                            "cvd": float(row.get("cvd", 0.0)),
                        } for ts, row in base.tail(self.cfg.backfill).iterrows()],
                        maxlen=self.cfg.backfill + 5,
                    ),
                    "signal": {"active": False, "since": 0, "cooldown_until": 0}
                }
        await asyncio.gather(*[agg_one(s) for s in self.symbols])
        kept = len(self.state)
        print(f"{self.log_prefix} Initialized {kept} symbols")
        try: await asyncio.gather(*[self.update_and_rank(s) for s in self.state.keys()])
        except Exception: pass
        return kept > 0

    async def update_and_rank(self, symbol: str):
        try:
            async with self.state[symbol]["lock"]:
                dq = self.state[symbol]["df_deque"]
                df = pd.DataFrame(dq)
                if df.empty or len(df) < max(self.cfg.z_period, self.cfg.slow_ma) + 2: # Need +2 for oi_delta
                    return

                # --- 1. Calculate base Z-scores ---
                vol_mean = df["volume"].rolling(self.cfg.z_period).mean().iloc[-1]
                vol_std  = df["volume"].rolling(self.cfg.z_period).std().iloc[-1]
                vol_z = (df["volume"].iloc[-1] - vol_mean) / (vol_std if vol_std > 1e-9 else 1.0)

                oi_mean = df["openInterestValue"].rolling(self.cfg.z_period).mean().iloc[-1]
                oi_std  = df["openInterestValue"].rolling(self.cfg.z_period).std().iloc[-1]
                oi_z = (df["openInterestValue"].iloc[-1] - oi_mean) / (oi_std if oi_std > 1e-9 else 1.0)

                # --- 2. Calculate VEC "Vertical Explosion" Score ---
                atr_series = calc_atr(df, 14).tail(self.cfg.z_period)
                price_pct_change = df['close'].pct_change().tail(self.cfg.z_period)
                price_atr_ratio = (price_pct_change / atr_series).replace([np.inf, -np.inf], np.nan).dropna()
                
                if len(price_atr_ratio) >= 5:
                    price_atr_mean = price_atr_ratio.mean()
                    price_atr_std = price_atr_ratio.std()
                    current_ratio = price_atr_ratio.iloc[-1]
                    price_atr_z = (current_ratio - price_atr_mean) / (price_atr_std if price_atr_std > 1e-9 else 1.0)
                else:
                    price_atr_z = 0.0

                # --- 3. Calculate CVD Acceleration Z-Score (FIXED) ---
                cvd_fast_series = df["cvd"].rolling(self.cfg.fast_ma).mean()
                cvd_slow_series = df["cvd"].rolling(self.cfg.slow_ma).mean()
                cvd_accel_series = (cvd_fast_series - cvd_slow_series).dropna()

                # --- [NEW v1.7] Calculate OI Delta ---
                # Get the last 2 OI values from the dataframe
                oi_now = df["openInterestValue"].iloc[-1]
                oi_prev = df["openInterestValue"].iloc[-2]
                oi_delta = oi_now - oi_prev
                # --- [END NEW v1.7] ---

                if len(cvd_accel_series) < self.cfg.z_period:
                    cvd_acc_z = 0.0
                    passes_gate = False
                else:
                    accel_history = cvd_accel_series.tail(self.cfg.z_period)
                    cvd_accel = accel_history.iloc[-1]
                    mu = float(accel_history.mean())
                    sd = float(accel_history.std())
                    cvd_acc_z = (cvd_accel - mu) / (sd if sd > 1e-9 else 1.0)

                    # --- [MODIFIED v1.7] Add oi_delta > 0 to the gate ---
                    passes_gate = (
                        (oi_z > self.cfg.gate_z) 
                        and (cvd_acc_z > self.cfg.gate_z) 
                        and (oi_delta > 0)  # <-- BLOCKS ENTRY IF OI IS DUMPING
                    )

                # --- 4. Final Scoring & Gating ---
                score = (
                    (self.cfg.price_atr_w * price_atr_z) +
                    (self.cfg.oi_w * oi_z) +
                    (self.cfg.cvd_w * cvd_acc_z) +
                    (self.cfg.vol_w * vol_z)
                )

                if not np.isfinite(score):
                    score = -1e9

                # --- 5. Trade Plan ---
                trade = None
                vp = vol_profile(df.tail(self.cfg.z_period))
                if (passes_gate or self.cfg.gate_z == 0.0) and vp:
                    atr = calc_atr(df, 14)
                    if len(atr) and np.isfinite(atr.iloc[-1]) and atr.iloc[-1] > 0:
                        atr_v = float(atr.iloc[-1])
                        poc, vah, val = vp["poc"], vp["vah"], vp["val"]
                        entry = float(poc)
                        risk = atr_v * self.cfg.risk_mult
                        sl = max(0.0, float(val) - self.cfg.sl_buf * atr_v)
                        tp1 = float(vah)
                        hvn_up = [h for h in vp["hvns"] if h > tp1]
                        tp2 = float(hvn_up[0]) if hvn_up else entry + self.cfg.tp2_mul * risk
                        tp3 = float(hvn_up[1]) if len(hvn_up) > 1 else entry + self.cfg.tp3_mul * risk
                        if tp1 > entry > sl > 0:
                            trade = {
                                "price": float(df["close"].iloc[-1]),
                                "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3,
                            }
            
            # --- 6. Hysteresis Filtering ---
            async with self.lock:
                now = int(time.time())
                sig = self.state[symbol]["signal"]
                
                if "15m" in self.cfg.tf_ccxt:
                    open_thr, close_thr = OPEN_THR_15M, CLOSE_THR_15M
                elif "1h" in self.cfg.tf_ccxt:
                    open_thr, close_thr = OPEN_THR_1H, CLOSE_THR_1H
                else:
                    open_thr, close_thr = OPEN_THR_4H, CLOSE_THR_4H

                is_gated = self.cfg.gate_z > 0.0
                passes_final = passes_gate if is_gated else True

                if now < sig.get("cooldown_until", 0):
                    passes_final = False
                if not sig["active"]:
                    if passes_final and score >= open_thr:
                        sig["active"] = True
                        sig["since"] = now
                    else:
                        passes_final = False
                else:
                    if now - sig.get("since", now) < MIN_HOLD_SEC:
                        passes_final = True
                    elif score < close_thr or (is_gated and not passes_gate):
                        sig["active"] = False
                        sig["cooldown_until"] = now + COOLDOWN_SEC
                        passes_final = False

                # --- [NEW v1.7] Volatility Filter ---
                bb_exp_pct = calc_bollinger_expansion_pct(df, 20, 2)
                if bb_exp_pct > MAX_BB_EXPANSION_PCT:
                    passes_final = False # Block entry, volatility is too high
                # --- [END NEW v1.7] ---

                self.top = [x for x in self.top if x["symbol"] != symbol]
                if passes_final and trade:
                    self.top.append({
                        "symbol": symbol, "score": float(score),
                        "price_z": float(price_atr_z), # VEC Score
                        "oi_z": float(oi_z),
                        "cvd_z": float(cvd_acc_z),
                        "vol_z": float(vol_z),
                        "bb_exp": bb_exp_pct, # Add for debugging
                        **trade
                    })
                self.top.sort(key=lambda x: (x["score"], x.get("vol_z", 0.0)), reverse=True)
        except Exception as e:
            print(f"{Fore.RED}{self.log_prefix} update_and_rank {symbol}: {e}{Style.RESET_ALL}")
            # import traceback; traceback.print_exc() # Uncomment for deep debug


    # --- Stream Watchers ---

    async def watch_candles(self, ex, symbol: str):
        retry = 5.0
        while True:
            try:
                candles = await ex.watch_ohlcv(symbol, self.cfg.tf_ccxt)
                if symbol not in self.state: continue
                async with self.state[symbol]["lock"]:
                    dq = self.state[symbol]["df_deque"]
                    if not dq or len(candles) < 2: continue
                    raw = candles[-2]
                    if not valid_ohlcv_tuple(raw): continue
                    t, o, h, l, c, v = raw
                    last_ts = dq[-1]["timestamp"]
                    if t > last_ts:
                        last = dq[-1] if dq else {}
                        dq.append({
                            "timestamp": int(t),
                            "open":   safe_float(o, default=last.get("close", 0.0)),
                            "high":   safe_float(h, default=last.get("high", 0.0)),
                            "low":    safe_float(l, default=last.get("low", 0.0)),
                            "close":  safe_float(c, default=last.get("close", 0.0)),
                            "volume": safe_float(v, default=0.0),
                            "openInterestValue": last.get("openInterestValue", 0.0),
                            "cvd": last.get("cvd", 0.0),
                        })
                        self.state[symbol]["last_update"] = int(t)
                        asyncio.create_task(self.update_and_rank(symbol))
                    else:
                        dq[-1]["volume"] = safe_float(v, default=dq[-1].get("volume", 0.0))
                retry = 5.0
            except Exception as e:
                if "ping-pong" in str(e):
                    print(f"{Fore.YELLOW}{self.log_prefix} {ex.id} candles {symbol}: Ping-pong timeout, reconnecting...{Style.RESET_ALL}")
                elif "1006" in str(e):
                    print(f"{Fore.YELLOW}{self.log_prefix} {ex.id} candles {symbol}: Connection closed (1006), reconnecting...{Style.RESET_ALL}")
                else:
                    print(f"{Fore.RED}{self.log_prefix} {ex.id} candles {symbol}: {e}{Style.RESET_ALL}")
                await asyncio.sleep(retry + random.uniform(0, 3))
                retry = min(retry * 2, 300)

    async def watch_oi(self, ex, symbol: str):
        if not getattr(ex, "has", {}).get("watchOpenInterest", False): return
        retry = 5.0
        while True:
            try:
                oi = await ex.watch_open_interest(symbol)
                if symbol not in self.state: continue
                async with self.state[symbol]["lock"]:
                    dq = self.state[symbol]["df_deque"]
                    if dq:
                        val = float(oi.get("openInterestValue") or oi.get("openInterestUsd", 0.0) or 0.0)
                        dq[-1]["openInterestValue"] = max(dq[-1].get("openInterestValue", 0.0), val)
                retry = 5.0
            except Exception as e:
                if "ping-pong" in str(e):
                    print(f"{Fore.YELLOW}{self.log_prefix} {ex.id} OI {symbol}: Ping-pong timeout, reconnecting...{Style.RESET_ALL}")
                elif "1006" in str(e):
                    print(f"{Fore.YELLOW}{self.log_prefix} {ex.id} OI {symbol}: Connection closed (1006), reconnecting...{Style.RESET_ALL}")
                else:
                    print(f"{Fore.RED}{self.log_prefix} {ex.id} OI {symbol}: {e}{Style.RESET_ALL}")
                await asyncio.sleep(retry + random.uniform(0, 3))
                retry = min(retry * 2, 300)

    # *** THIS IS THE "TRUE TAKER" CVD FUNCTION ***
    async def watch_trades(self, ex, symbol: str):
        if not getattr(ex, "has", {}).get("watchTrades", False): return
        retry = 5.0
        while True:
            try:
                trades = await ex.watch_trades(symbol)
                if symbol not in self.state: continue
                async with self.state[symbol]["lock"]:
                    dq = self.state[symbol]["df_deque"]
                    if not dq: continue
                base = float(dq[-1].get("cvd", 0.0))
                for t in trades:
                    amt = float(t.get("amount", 0.0) or 0.0)
                    side = (t.get("side") or "").lower()
                    info = t.get("info") or {}
                    taker_signed = None
                    if "m" in info or "isBuyerMaker" in info:
                        is_buyer_maker = info.get("m", info.get("isBuyerMaker"))
                        if isinstance(is_buyer_maker, str): is_buyer_maker = is_buyer_maker.lower() in ("true","1","yes")
                        if is_buyer_maker is True: taker_signed = -amt
                        elif is_buyer_maker is False: taker_signed = +amt
                    if taker_signed is None:
                        bybit_side = info.get("S") or info.get("side") or info.get("s")
                        if isinstance(bybit_side, str):
                            bl = bybit_side.lower()
                            if bl == "buy": taker_signed = +amt
                            elif bl == "sell": taker_signed = -amt
                    if taker_signed is None:
                        if side == "buy": taker_signed = +amt
                        elif side == "sell": taker_signed = -amt
                        else: taker_signed = 0.0
                    base += taker_signed
                dq[-1]["cvd"] = base
                if trades and trades[-1].get("timestamp", 0) > self.state[symbol]["last_update"]:
                    asyncio.create_task(self.update_and_rank(symbol))
                retry = 5.0
            except Exception as e:
                if "ping-pong" in str(e):
                    print(f"{Fore.YELLOW}{self.log_prefix} {ex.id} trades {symbol}: Ping-pong timeout, reconnecting...{Style.RESET_ALL}")
                elif "1006" in str(e):
                    print(f"{Fore.YELLOW}{self.log_prefix} {ex.id} trades {symbol}: Connection closed (1006), reconnecting...{Style.RESET_ALL}")
                else:
                    print(f"{Fore.RED}{self.log_prefix} {ex.id} trades {symbol}: {e}{Style.RESET_ALL}")
                await asyncio.sleep(retry + random.uniform(0, 3))
                retry = min(retry * 2, 300)

    async def subscribe_all(self):
        tasks = []
        syms = list(self.symbols); random.shuffle(syms)
        print(f"{self.log_prefix} Subscribing to {len(syms)} symbols with a {SUBSCRIPTION_DELAY}s delay...")
        for symbol in syms:
            if symbol not in self.state: continue
            for ex_id, cfg in EXCHANGES_CFG.items():
                ex = self.exchanges[ex_id]
                if getattr(ex, "has", {}).get("watchOHLCV", False):
                    tasks.append(asyncio.create_task(self.watch_candles(ex, symbol)))
                if cfg["oi_stream"] and getattr(ex, "has", {}).get("watchOpenInterest", False):
                    tasks.append(asyncio.create_task(self.watch_oi(ex, symbol)))
                if cfg["cvd_stream"] and getattr(ex, "has", {}).get("watchTrades", False):
                    tasks.append(asyncio.create_task(self.watch_trades(ex, symbol)))
            await asyncio.sleep(SUBSCRIPTION_DELAY) # <--- This is now 1.0s
        print(f"{Fore.GREEN}{self.log_prefix} Subscriptions complete. Streams running: {len(tasks)}{Style.RESET_ALL}")
        await asyncio.gather(*tasks)

    # --- Printer ---
    async def printer(self):
        while True:
            await asyncio.sleep(PRINT_INTERVAL)
            try:
                async with self.lock:
                    sig = _fingerprint_top_dual(self.top)
                    if sig == self.last_sig: continue
                    self.last_sig = sig
                    rows = []
                    for i, r in enumerate(self.top[:5], 1):
                        
                        # --- STALE TRADE FILTER ---
                        if r['price'] > r['tp1'] or r['price'] < r['sl']:
                            continue
                        # --- END FILTER ---

                        pr = 2 if r["price"] >= 10 else (4 if r["price"] >= 0.1 else (6 if r["price"] >= 0.001 else 8))
                        rr_den = max(r["entry"] - r["sl"], 1e-12)
                        rr = (r["tp1"] - r["entry"]) / rr_den if rr_den > 0 else 0.0
                        rr_txt = f"{rr:.2f}"
                        if rr >= 1.8: rr_txt = f"{Fore.GREEN}{Style.BRIGHT}{rr:.2f}{Style.RESET_ALL}"
                        elif rr >= 1.5: rr_txt = f"{Fore.YELLOW}{rr:.2f}{Style.RESET_ALL}"
                        else: rr_txt = f"{Fore.RED}{rr:.2f}{Style.RESET_ALL}"
                        
                        if "15m" in self.cfg.tf_ccxt:
                            open_thr = OPEN_THR_15M
                        elif "1h" in self.cfg.tf_ccxt:
                            open_thr = OPEN_THR_1H
                        else:
                            open_thr = OPEN_THR_4H
                        hot = f"{Fore.MAGENTA}HOT🔥{Style.RESET_ALL}" if r.get("score", 0) >= open_thr else ""

                        rows.append([
                            i, f"{Fore.YELLOW}{Style.BRIGHT}{r['symbol']}{Style.RESET_ALL}", hot,
                            f"{Fore.MAGENTA}{r['score']:.2f}{Style.RESET_ALL}",
                            f"{Fore.GREEN if r['price_z']>1 else Fore.WHITE}{r['price_z']:.2f}{Style.RESET_ALL}",
                            f"{Fore.GREEN if r['oi_z']>1 else Fore.WHITE}{r['oi_z']:.2f}{Style.RESET_ALL}",
                            f"{Fore.GREEN if r['cvd_z']>1 else Fore.WHITE}{r['cvd_z']:.2f}{Style.RESET_ALL}",
                            f"{r['price']:.{pr}f}", f"{Fore.CYAN}{r['entry']:.{pr}f}{Style.RESET_ALL}",
                            f"{Fore.RED}{r['sl']:.{pr}f}{Style.RESET_ALL}", rr_txt,
                            f"{Fore.GREEN}{r['tp1']:.{pr}f}{Style.RESET_ALL}",
                            f"{r.get('bb_exp', 0.0):.2f}%", # <-- [NEW v1.7] Show BB %
                        ])
                    
                    headers = [
                        "#","Symbol","Heat","Score 🔥","Price/ATR Z 💥", "OI Z 📈","CVD Z 💨",
                        "Price","Entry (POC) 🔵","SL 🔴","RR ▶︎ TP1","TP1 🟢", "BB% 📊", # <-- [NEW v1.7]
                    ]
                    print("\n" + Style.BRIGHT + Fore.CYAN + f"=== {self.cfg.name} (UTC {time.strftime('%Y-%m-%d %H:%M:%S')}) ===" + Style.RESET_ALL)
                    if rows:
                        print(tabulate(rows, headers=headers, tablefmt="fancy_grid"))
                        _mac_ding()
                    else:
                        print("⏳ Warming up… awaiting qualifying signals or filtering stale trades.")
            except Exception as e:
                print(f"{Fore.RED}{self.log_prefix} printer: {e}{Style.RESET_ALL}")

# --- Exchange Wiring ---
EXCHANGES_CFG = {
    "binanceusdm": {"oi_stream": False, "vol_stream": True,  "cvd_stream": True, "defaultType": "future"},
    "bybit":       {"oi_stream": True,  "vol_stream": True,  "cvd_stream": True,  "defaultType": "future"},
    "bitget":      {"oi_stream": True,  "vol_stream": True,  "cvd_stream": True, "defaultType": "swap"},
    "mexc":        {"oi_stream": True,  "vol_stream": True,  "cvd_stream": True, "defaultType": "swap"},
}

# ====================================================================
# *** v1.6: Enhanced Exchange Constructor ***
# ====================================================================
async def init_exchanges():
    exchanges = {}
    for ex_id, c in EXCHANGES_CFG.items():
        klass = getattr(ccxt, ex_id)
        
        # NEW: Add robust connection settings
        config = {
            "enableRateLimit": True,
            "options": {
                "defaultType": c["defaultType"],
                "watch_ohlcv_limit": 5, # Get only 5 candles on stream updates
            },
            "newUpdates": False, # Drastically reduces data load
            "keepalive": 30000, # 30s TCP keepalive
            "enable_close_watcher": True, # ccxt-native auto-reconnect
        }
        
        ex = klass(config)
        
        try:
            await ex.load_markets()
            exchanges[ex_id] = ex
            print(f"Loaded markets: {ex_id} → {len(ex.markets)}")
        except Exception as e:
            print(f"{Fore.RED}Could not load {ex_id}: {e}{Style.RESET_ALL}")
            await ex.close()
    return exchanges

def common_usdt_symbols(exchanges: dict) -> list:
    universe_per_ex = []
    for ex_id, ex in exchanges.items():
        syms = [s for s, m in ex.markets.items() if isinstance(s, str) and "/USDT" in s and m.get("active", True)]
        universe_per_ex.append(set(syms))
    common = set.intersection(*universe_per_ex) if universe_per_ex else set()
    print(f"Common /USDT symbols across {len(exchanges)} venues: {len(common)}")
    return sorted(common)

# --- Main Execution ---
async def main():
    print(f"{Fore.GREEN}{Style.BRIGHT}--- Vertical Explosion Streamer v1.7 (Safe Mode) ---{Style.RESET_ALL}")
    exchanges = await init_exchanges()
    if len(exchanges) < 2:
        print(f"{Fore.RED}Need at least 2 exchanges for aggregation. Exiting.{Style.RESET_ALL}")
        for ex in exchanges.values(): await ex.close()
        return

    symbols = common_usdt_symbols(exchanges)
    
    if not symbols:
        print(f"{Fore.RED}No common symbols found. Exiting.{Style.RESET_ALL}")
        for ex in exchanges.values(): await ex.close()
        return

    # --- Running the 15m VEC Screener by default ---
    vec_screener = Screener(CFG_VEC_15M, exchanges, symbols)
    ok_vec = await vec_screener.initialize()

    if not ok_vec or not vec_screener.state:
        print(f"{Fore.RED}Initialization failed, no symbols loaded. Exiting.{Style.RESET_ALL}")
        for ex in exchanges.values(): await ex.close()
        return

    asyncio.create_task(vec_screener.printer())

    try:
        await vec_screener.subscribe_all()
    finally:
        for ex in exchanges.values():
            try: await ex.close()
            except Exception: pass

if __name__ == "__main__":
    while True: # <--- v1.6: Add a simple outer loop
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            print("\nStopped by user.")
            break # Exit loop on Ctrl+C
        except Exception as e:
            # This catches the `AttributeError` and other critical loop-killing exceptions
            print(f"{Fore.RED}CRITICAL MAIN LOOP ERROR: {e}{Style.RESET_ALL}")
            print("Restarting main loop in 30 seconds...")
            time.sleep(30)