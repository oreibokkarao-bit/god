#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fresh Money Multi Screener — 15m SAFE (FIXED) + CHANGE-TRIGGER PRINT + MAC DING
(derived from the user's 4h version) — v2 offset-resample
"""
# --- Begin auto-injected SQLite cache + WS keepalive (inlined) ---
try:
    import os
    from pathlib import Path as _P
    _db_path = _P(__file__).with_name("fresh_money_cache.sqlite")
    _ttl = int(os.environ.get("FRESH_MONEY_CACHE_TTL", "30"))
    _ping = int(os.environ.get("FRESH_MONEY_WS_PING", "20"))
    _db_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        enable_sqlite_cache_patched(str(_db_path), _ttl)
    except NameError:
        pass
    try:
        enable_ws_keepalive_patched()
    except NameError:
        pass
    try:
        import sqlite3 as _sqlite3
        _sqlite3.connect(str(_db_path)).close()
    except Exception:
        pass
    print(f"[CACHE] SQLite pinned to {_db_path} | ttl={_ttl}s | WS ping={_ping}s")
except Exception as _e:
    print("[WARN] Auto-enable cache/keepalive failed:", _e)
# --- End auto-injected block ---

import asyncio
import time
import random
import subprocess, os
from collections import deque

import ccxt.pro as ccxt
import pandas as pd
import numpy as np
import re
from tabulate import tabulate
from colorama import Fore, Style, init

# --- Resample offset helper (uses pandas Timedelta, avoiding deprecated string aliases) ---
def _tf_as_offset(tf: str):
    """
    Return a pandas Timedelta/DateOffset for resampling without using string aliases.
    Supports: 'Xm', 'Xh', 'Xd' where X is int.
    """
    tf = (tf or "").strip()
    m = re.fullmatch(r"(\\d+)m", tf, flags=re.IGNORECASE)
    if m:
        return pd.to_timedelta(int(m.group(1)), unit="m")
    h = re.fullmatch(r"(\\d+)h", tf, flags=re.IGNORECASE)
    if h:
        return pd.to_timedelta(int(h.group(1)), unit="h")
    d = re.fullmatch(r"(\\d+)d", tf, flags=re.IGNORECASE)
    if d:
        return pd.to_timedelta(int(d.group(1)), unit="d")
    try:
        return pd.to_timedelta(int(tf), unit="m")
    except Exception:
        return pd.to_timedelta(1, unit="h")

# ------------ Constants ------------
TIME_FRAME = '15m'
DATA_FETCH_LIMIT = 320
Z_SCORE_PERIOD = 20
FAST_MA_PERIOD = 5
SLOW_MA_PERIOD = 20

MIN_QUOTE_VOLUME = 200_000
GATE_THRESHOLD_ZSCORE = 0.65
WEIGHT_OI = 0.4
WEIGHT_CVD = 0.4
WEIGHT_VOL = 0.2

SUBSCRIPTION_DELAY = 0.25
PRINT_INTERVAL = 10

EXCHANGES_TO_AGGREGATE = {
    'binanceusdm': {'oi_stream': False, 'vol_stream': True,  'cvd_stream': False, 'defaultType': 'future'},
    'bybit':       {'oi_stream': True,  'vol_stream': True,  'cvd_stream': True,  'defaultType': 'future'},
    'bitget':      {'oi_stream': True,  'vol_stream': True,  'cvd_stream': False, 'defaultType': 'swap'},
    'mexc':        {'oi_stream': True,  'vol_stream': True,  'cvd_stream': False, 'defaultType': 'swap'},
}

# ------------ Globals ------------
init(autoreset=True)
STATE: dict[str, dict] = {}
TOP_5_LIST: list[dict] = []
GLOBAL_LOCK = asyncio.Lock()

LAST_SIG = None
ENABLE_SOUND = True
SOUND_PATH = "/System/Library/Sounds/Glass.aiff"
SOUND_RATE_LIMIT_SEC = 2.0
_LAST_SOUND_TS = 0.0

def _mac_ding():
    global _LAST_SOUND_TS
    if not ENABLE_SOUND or not os.path.exists(SOUND_PATH):
        return
    now = time.time()
    if now - _LAST_SOUND_TS < SOUND_RATE_LIMIT_SEC:
        return
    try:
        subprocess.Popen(["afplay", SOUND_PATH], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        _LAST_SOUND_TS = now
    except Exception:
        pass

def _fingerprint_top(rows: list[dict]) -> tuple:
    return tuple(
        (r['symbol'], round(r.get('composite_score', r.get('score', 0.0)), 3),
         round(r.get('pullback_entry', r.get('entry', 0.0)), 6),
         round(r.get('tp1', 0.0), 6),
         round(r.get('tp2', 0.0), 6),
         round(r.get('tp3', 0.0), 6))
        for r in rows[:5]
    )

def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    if df.empty:
        return pd.Series(dtype='float64')
    df_atr = df[['high', 'low', 'close']].copy()
    df_atr['h-l'] = df_atr['high'] - df_atr['low']
    df_atr['h-pc'] = (df_atr['high'] - df_atr['close'].shift(1)).abs()
    df_atr['l-pc'] = (df_atr['low'] - df_atr['close'].shift(1)).abs()
    tr = df_atr[['h-l', 'h-pc', 'l-pc']].max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()

def calculate_volume_profile(df: pd.DataFrame, bins: int = 50):
    if df.empty:
        return None
    price_min = float(df['low'].min())
    price_max = float(df['high'].max())
    if not np.isfinite(price_min) or not np.isfinite(price_max) or price_max <= price_min:
        return None
    bin_size = (price_max - price_min) / bins
    price_bins = np.arange(price_min, price_max + bin_size, bin_size)
    bin_volumes = np.zeros(len(price_bins))
    for _, row in df.iterrows():
        low, high, vol = float(row['low']), float(row['high']), float(row['volume'])
        if not np.isfinite(low) or not np.isfinite(high) or high <= low:
            continue
        idx_low = np.searchsorted(price_bins, low, side='left')
        idx_high = np.searchsorted(price_bins, high, side='right') - 1
        idx_low = max(0, min(idx_low, len(price_bins) - 1))
        idx_high = max(0, min(idx_high, len(price_bins) - 1))
        spread = max(1, idx_high - idx_low + 1)
        per = vol / spread
        bin_volumes[idx_low:idx_high + 1] += per
    poc_index = int(np.argmax(bin_volumes))
    poc = float(price_bins[poc_index])
    total_volume = float(bin_volumes.sum())
    target = total_volume * 0.7
    low_i = high_i = poc_index
    acc = float(bin_volumes[poc_index])
    while acc < target:
        left_v = bin_volumes[low_i - 1] if low_i - 1 >= 0 else -np.inf
        right_v = bin_volumes[high_i + 1] if high_i + 1 < len(bin_volumes) else -np.inf
        if left_v >= right_v and np.isfinite(left_v):
            low_i -= 1
            acc += left_v
        elif np.isfinite(right_v):
            high_i += 1
            acc += right_v
        else:
            break
    val = float(price_bins[max(0, low_i)])
    vah = float(price_bins[min(high_i, len(price_bins) - 1)])
    avg_per_bin = total_volume / bins if bins else 0.0
    hvns = []
    for i in range(1, len(bin_volumes) - 1):
        if bin_volumes[i] > bin_volumes[i - 1] and bin_volumes[i] > bin_volumes[i + 1] and bin_volumes[i] > avg_per_bin * 1.5:
            hvns.append(float(price_bins[i]))
    hvns_outside = sorted([x for x in hvns if x > vah or x < val])
    return {'poc': poc, 'vah': vah, 'val': val, 'hvns': hvns_outside}

async def initialize_exchange(exchange_id: str):
    klass = getattr(ccxt, exchange_id)
    opts = {'enableRateLimit': True, 'options': {'defaultType': EXCHANGES_TO_AGGREGATE[exchange_id]['defaultType']}}
    ex = klass(opts)
    await ex.load_markets()
    return ex

async def backfill_symbol_data(ex, symbol: str):
    try:
        ohlcv = await ex.fetch_ohlcv(symbol, timeframe=TIME_FRAME, limit=DATA_FETCH_LIMIT + 2)
        if not ohlcv or len(ohlcv) < 10:
            return None
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)

        oi_series = None
        if getattr(ex, 'has', {}).get('fetchOpenInterestHistory', False):
            try:
                oi_data = await ex.fetch_open_interest_history(symbol, timeframe=TIME_FRAME, limit=DATA_FETCH_LIMIT + 2)
                if oi_data:
                    oi_df = pd.DataFrame(oi_data)
                    if 'timestamp' in oi_df and 'openInterestValue' in oi_df:
                        oi_df['timestamp'] = pd.to_datetime(oi_df['timestamp'], unit='ms')
                        oi_df.set_index('timestamp', inplace=True)
                        oi_series = oi_df['openInterestValue'].astype(float).resample(_tf_as_offset(TIME_FRAME), label='right', closed='right').last()
            except Exception:
                oi_series = None

        if oi_series is None:
            oi_series = pd.Series(index=df.index, data=np.nan, dtype='float64')

        df['openInterestValue'] = oi_series.reindex(df.index).ffill()
        df['cvd'] = 0.0
        df = df[['open', 'high', 'low', 'close', 'volume', 'openInterestValue', 'cvd']].dropna(subset=['open', 'high', 'low', 'close'])
        return df

    except ccxt.RateLimitExceeded:
        print(f"{Fore.RED}Rate limit exceeded during backfill on {ex.id}. Sleeping 60s...{Style.RESET_ALL}")
        await asyncio.sleep(60)
        return None
    except Exception as e:
        print(f"{Fore.RED}Backfill error on {ex.id} {symbol}: {e}{Style.RESET_ALL}")
        return None

async def initialize_state():
    exchanges = {}
    all_markets = {}
    for ex_id in EXCHANGES_TO_AGGREGATE.keys():
        try:
            exchanges[ex_id] = await initialize_exchange(ex_id)
            mkts = exchanges[ex_id].markets
            symbols = [s for s, m in mkts.items() if isinstance(s, str) and '/USDT' in s and m.get('active', True)]
            all_markets[ex_id] = {s: mkts[s] for s in symbols}
            print(f"Loaded {len(all_markets[ex_id])} markets from {ex_id}")
        except Exception as e:
            print(f"Could not load {ex_id}: {e}")
    if set(all_markets.keys()) != set(EXCHANGES_TO_AGGREGATE.keys()):
        print(f"{Fore.RED}Critical error: Could not load all exchanges. Exiting.{Style.RESET_ALL}")
        return None, None

    common_symbols = set.intersection(*[set(d.keys()) for d in all_markets.values()])
    print(f"Found {len(common_symbols)} common symbols across all exchanges.")
    print(f"Backfilling initial {DATA_FETCH_LIMIT} bars for {len(common_symbols)} symbols...")

    sem = asyncio.Semaphore(10)
    async def backfill_and_aggregate(symbol: str):
        async with sem:
            dfs = []
            for ex_id in EXCHANGES_TO_AGGREGATE.keys():
                df = await backfill_symbol_data(exchanges[ex_id], symbol)
                if df is not None and not df.empty:
                    df = df.copy()
                    df[f'vol_{ex_id}'] = df['volume']
                    dfs.append(df)
            if not dfs:
                return
            base = dfs[0].copy()
            for add_df in dfs[1:]:
                base = base.join(add_df[['volume', 'openInterestValue']], how='outer', rsuffix='_r')
                base['volume'] = base[['volume', 'volume_r']].sum(axis=1, skipna=True)
                base.drop(columns=['volume_r'], inplace=True)
                base['openInterestValue'] = base[['openInterestValue', 'openInterestValue_r']].sum(axis=1, skipna=True)
                base.drop(columns=['openInterestValue_r'], inplace=True)
            base = base[['open', 'high', 'low', 'close', 'volume', 'openInterestValue', 'cvd']].sort_index()
            base = base.ffill().dropna()
            if base['volume'].iloc[-1] < MIN_QUOTE_VOLUME:
                return
            STATE[symbol] = {
                'lock': asyncio.Lock(),
                'last_update': int(base.index[-1].value // 1_000_000),
                'df_deque': deque(
                    [
                        {
                            'timestamp': int(ts.value // 1_000_000),
                            'open': float(row['open']),
                            'high': float(row['high']),
                            'low': float(row['low']),
                            'close': float(row['close']),
                            'volume': float(row['volume']),
                            'openInterestValue': float(row['openInterestValue']),
                            'cvd': float(row.get('cvd', 0.0)),
                        }
                        for ts, row in base.tail(DATA_FETCH_LIMIT).iterrows()
                    ],
                    maxlen=DATA_FETCH_LIMIT + 5,
                ),
            }
    await asyncio.gather(*[backfill_and_aggregate(sym) for sym in common_symbols])
    print("State initialization complete.")
    initialized = [s for s in common_symbols if s in STATE]
    if not initialized:
        print(f"{Fore.RED}Initialization failed. Exiting.{Style.RESET_ALL}")
        return None, None
    return exchanges, initialized

async def update_and_rank(symbol: str):
    global TOP_5_LIST
    try:
        async with STATE[symbol]['lock']:
            dq = STATE[symbol]['df_deque']
            df = pd.DataFrame(dq)
            if df.empty or len(df) < max(Z_SCORE_PERIOD, SLOW_MA_PERIOD) + 1:
                return
            vol_mean = df['volume'].rolling(Z_SCORE_PERIOD).mean().iloc[-1]
            vol_std = df['volume'].rolling(Z_SCORE_PERIOD).std().iloc[-1]
            vol_z = (df['volume'].iloc[-1] - vol_mean) / (vol_std if vol_std else 1.0)
            oi_mean = df['openInterestValue'].rolling(Z_SCORE_PERIOD).mean().iloc[-1]
            oi_std = df['openInterestValue'].rolling(Z_SCORE_PERIOD).std().iloc[-1]
            oi_z = (df['openInterestValue'].iloc[-1] - oi_mean) / (oi_std if oi_std else 1.0)
            cvd_fast = df['cvd'].rolling(FAST_MA_PERIOD).mean().iloc[-1]
            cvd_slow = df['cvd'].rolling(SLOW_MA_PERIOD).mean().iloc[-1]
            cvd_accel = cvd_fast - cvd_slow
            cvd_acc_mean = df['cvd'].rolling(Z_SCORE_PERIOD).apply(
                lambda x: (pd.Series(x).rolling(FAST_MA_PERIOD).mean().iloc[-1] - pd.Series(x).rolling(SLOW_MA_PERIOD).mean().iloc[-1]),
                raw=False
            ).dropna()
            mu = float(cvd_acc_mean.mean()) if len(cvd_acc_mean) else 0.0
            sd = float(cvd_acc_mean.std()) if len(cvd_acc_mean) else 1.0
            cvd_acc_z = (cvd_accel - mu) / (sd if sd else 1.0)
            passes_gate = (oi_z > GATE_THRESHOLD_ZSCORE) and (cvd_acc_z > GATE_THRESHOLD_ZSCORE)
            score = WEIGHT_OI * oi_z + WEIGHT_CVD * cvd_acc_z + WEIGHT_VOL * vol_z
            trade_plan = None
            vp = calculate_volume_profile(df.tail(Z_SCORE_PERIOD))
            if passes_gate and vp:
                atr = calculate_atr(df, 14)
                if len(atr) and np.isfinite(atr.iloc[-1]) and atr.iloc[-1] > 0:
                    atr_val = float(atr.iloc[-1])
                    cur = float(df['close'].iloc[-1])
                    poc, vah, val = vp['poc'], vp['vah'], vp['val']
                    entry = float(poc)
                    risk = atr_val * 0.8
                    sl = max(0.0, float(val) - 0.25 * atr_val)
                    tp1 = float(vah)
                    hvns_above = [h for h in vp['hvns'] if h > tp1]
                    tp2 = float(hvns_above[0]) if hvns_above else entry + 2.2 * risk
                    tp3 = float(hvns_above[1]) if len(hvns_above) > 1 else entry + 3.3 * risk
                    if tp1 > entry > sl > 0:
                        trade_plan = {
                            'current_price': cur,
                            'pullback_entry': entry,
                            'stop_loss': sl,
                            'tp1': tp1, 'tp2': tp2, 'tp3': tp3
                        }
        async with GLOBAL_LOCK:
            TOP_5_LIST = [x for x in TOP_5_LIST if x['symbol'] != symbol]
            if passes_gate and trade_plan:
                TOP_5_LIST.append({
                    'symbol': symbol,
                    'composite_score': float(score),
                    'oi_zscore': float(oi_z),
                    'cvd_accel_zscore': float(cvd_acc_z),
                    'vol_zscore': float(vol_z),
                    **trade_plan
                })
            TOP_5_LIST.sort(key=lambda x: x['composite_score'], reverse=True)
    except Exception as e:
        print(f"{Fore.RED}update_and_rank error {symbol}: {e}{Style.RESET_ALL}")

def _precision(price: float) -> int:
    if price >= 10: return 2
    if price >= 0.1: return 4
    if price >= 0.001: return 6
    return 8

async def print_top_loop():
    global LAST_SIG
    while True:
        await asyncio.sleep(PRINT_INTERVAL)
        try:
            async with GLOBAL_LOCK:
                current = TOP_5_LIST[:5]
                sig = _fingerprint_top(current)
                if sig == LAST_SIG:
                    continue
                LAST_SIG = sig
                rows = []
                for i, res in enumerate(current, 1):
                    pr = _precision(res['current_price'])
                    rows.append([
                        f"{i}",
                        f"{Fore.YELLOW}{Style.BRIGHT}{res['symbol']}{Style.RESET_ALL}",
                        f"{Fore.MAGENTA}{res['composite_score']:.2f}{Style.RESET_ALL}",
                        f"{Fore.GREEN if res['oi_zscore'] > 1.0 else Fore.WHITE}{res['oi_zscore']:.2f}{Style.RESET_ALL}",
                        f"{Fore.GREEN if res['cvd_accel_zscore'] > 1.0 else Fore.WHITE}{res['cvd_accel_zscore']:.2f}{Style.RESET_ALL}",
                        f"{Fore.GREEN if res['vol_zscore'] > 1.0 else Fore.WHITE}{res['vol_zscore']:.2f}{Style.RESET_ALL}",
                        f"{res['current_price']:.{pr}f}",
                        f"{Fore.CYAN}{res['pullback_entry']:.{pr}f}{Style.RESET_ALL}",
                        f"{Fore.RED}{res['stop_loss']:.{pr}f}{Style.RESET_ALL}",
                        f"{Fore.GREEN}{res['tp1']:.{pr}f}{Style.RESET_ALL}",
                        f"{Fore.GREEN}{res['tp2']:.{pr}f}{Style.RESET_ALL}",
                        f"{Fore.GREEN}{res['tp3']:.{pr}f}{Style.RESET_ALL}",
                    ])
                print("\n" + Style.BRIGHT + Fore.CYAN + f"--- ⚡️ Top 5 'Fresh Money' 15m SAFE (UTC {time.strftime('%Y-%m-%d %H:%M:%S')}) ---" + Style.RESET_ALL)
                if rows:
                    print(tabulate(rows, headers=[
                        "Rank", "Symbol", "Score 🔥", "OI Z 📈", "CVD Z 💨", "Vol Z 📊",
                        "Price", "Entry (POC) 🔵", "Stop (VAL-Buf) 🔴", "TP1 (VAH) 🟢", "TP2 (HVN) 🟢🟢", "TP3 (HVN) 🟢🟢🟢"
                    ], tablefmt="fancy_grid"))
                    _mac_ding()
                else:
                    print("⏳ Warming up… waiting for qualifying signals.")
        except Exception as e:
            print(f"{Fore.RED}print_top_loop error: {e}{Style.RESET_ALL}")

async def watch_candles_loop(ex, symbol: str, retry_delay: float = 5.0):
    while True:
        try:
            candles = await ex.watch_ohlcv(symbol, TIME_FRAME)
            if symbol not in STATE or STATE[symbol].get('df_deque') is None:
                continue
            async with STATE[symbol]['lock']:
                dq = STATE[symbol]['df_deque']
                if not dq or len(candles) < 2:
                    continue
                t, o, h, l, c, v = candles[-2]
                last_ts = dq[-1]['timestamp']
                if t > last_ts:
                    new_row = {
                        'timestamp': int(t),
                        'open': float(o),
                        'high': float(h),
                        'low': float(l),
                        'close': float(c),
                        'volume': float(v),
                        'openInterestValue': dq[-1].get('openInterestValue', 0.0),
                        'cvd': dq[-1].get('cvd', 0.0),
                    }
                    dq.append(new_row)
                    STATE[symbol]['last_update'] = int(t)
                    asyncio.create_task(update_and_rank(symbol))
                else:
                    last = dq[-1]
                    last['volume'] = float(v)
            retry_delay = 5.0
        except Exception as e:
            print(f"{Fore.RED}Error in {ex.id} watch_candles_loop for {symbol}: {e}{Style.RESET_ALL}")
            await asyncio.sleep(retry_delay + random.uniform(0, 3))
            retry_delay = min(retry_delay * 2, 300)

async def watch_oi_loop(ex, symbol: str, retry_delay: float = 5.0):
    while True:
        try:
            if not getattr(ex, 'has', {}).get('watchOpenInterest', False):
                return
            oi = await ex.watch_open_interest(symbol)
            if symbol not in STATE or STATE[symbol].get('df_deque') is None:
                continue
            async with STATE[symbol]['lock']:
                dq = STATE[symbol]['df_deque']
                if dq:
                    last = dq[-1]
                    val = float(oi.get('openInterestValue') or oi.get('openInterestUsd', 0.0) or 0.0)
                    last['openInterestValue'] = max(last.get('openInterestValue', 0.0), val)
            retry_delay = 5.0
        except Exception:
            await asyncio.sleep(retry_delay + random.uniform(0, 3))
            retry_delay = min(retry_delay * 2, 300)

async def watch_trades_loop(ex, symbol: str, retry_delay: float = 5.0):
    while True:
        try:
            trades = await ex.watch_trades(symbol)
            if symbol not in STATE or STATE[symbol].get('df_deque') is None:
                continue
            async with STATE[symbol]['lock']:
                dq = STATE[symbol]['df_deque']
                if not dq:
                    continue
                last = dq[-1]
                base = float(last.get('cvd', 0.0))
                for t in trades:
                    side = t.get('side')
                    amt = float(t.get('amount', 0.0) or 0.0)
                    signed = amt if side == 'buy' else (-amt if side == 'sell' else 0.0)
                    base += signed
                last['cvd'] = base
            if trades and trades[-1].get('timestamp', 0) > STATE[symbol]['last_update']:
                asyncio.create_task(update_and_rank(symbol))
            retry_delay = 5.0
        except Exception:
            await asyncio.sleep(retry_delay + random.uniform(0, 3))
            retry_delay = min(retry_delay * 2, 300)

async def subscribe_to_all_symbols(exchanges: dict, symbols: list[str]):
    tasks = []
    for symbol in symbols:
        for ex_id, cfg in EXCHANGES_TO_AGGREGATE.items():
            ex = exchanges[ex_id]
            if getattr(ex, 'has', {}).get('watchOHLCV', False):
                tasks.append(asyncio.create_task(watch_candles_loop(ex, symbol)))
            if cfg['oi_stream'] and getattr(ex, 'has', {}).get('watchOpenInterest', False):
                tasks.append(asyncio.create_task(watch_oi_loop(ex, symbol)))
            if cfg['cvd_stream'] and ex_id == 'bybit' and getattr(ex, 'has', {}).get('watchTrades', False):
                tasks.append(asyncio.create_task(watch_trades_loop(ex, symbol)))
        await asyncio.sleep(SUBSCRIPTION_DELAY)
    print(f"{Fore.GREEN}--- Paced subscription complete. {len(tasks)} streams running. ---{Style.RESET_ALL}")
    print("--- Live dashboard will update when Top-5 CHANGES. ---")
    await asyncio.gather(*tasks)

async def main_loop():
    print(f"{Fore.GREEN}{Style.BRIGHT}--- Initializing Multi-Exchange Aggregator (15m SAFE) ---{Style.RESET_ALL}")
    print("This script is now a 24/7 streaming service.")
    print("It solves 'Fragmented Liquidity' by aggregating data from:")
    print(f"  - {Fore.CYAN}Volume{Style.RESET_ALL}: " +
          ", ".join([ex for ex, c in EXCHANGES_TO_AGGREGATE.items() if c['vol_stream']]))
    print(f"  - {Fore.CYAN}Open Interest{Style.RESET_ALL}: " +
          ", ".join([ex for ex, c in EXCHANGES_TO_AGGREGATE.items() if c['oi_stream']]))
    print(f"  - {Fore.CYAN}'Real' CVD{Style.RESET_ALL}: " +
          ", ".join([ex for ex, c in EXCHANGES_TO_AGGREGATE.items() if c['cvd_stream']]))
    print("\nInitializing state... This may take a few minutes.")
    exchanges, symbols = await initialize_state()
    if not exchanges or not symbols:
        print("Initialization failed. Exiting.")
        for ex in (exchanges or {}).values():
            try:
                await ex.close()
            except Exception:
                pass
        return
    asyncio.create_task(print_top_loop())
    try:
        await subscribe_to_all_symbols(exchanges, symbols)
    finally:
        for ex in exchanges.values():
            try:
                await ex.close()
            except Exception:
                pass

if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        print("\nScreener stopped by user.")



# --- Inlined support modules: cache + ws_keepalive ---

# cache.py — SQLite HTTP cache + aiohttp monkeypatch (GET only)
import os, sqlite3, time, json, asyncio, zlib
from typing import Optional, Dict, Any

try:
    import aiohttp
except Exception:
    aiohttp = None

DEFAULT_DB = os.path.expanduser(os.environ.get("FRESH_MONEY_CACHE_DB", "~/fresh_money_cache.sqlite"))
DEFAULT_TTL = int(os.environ.get("FRESH_MONEY_CACHE_TTL", "30"))  # seconds

class CacheDB:
    def __init__(self, path: str = DEFAULT_DB):
        self.path = os.path.expanduser(path)
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self.conn = sqlite3.connect(self.path, timeout=30, isolation_level=None, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS http_cache ("
            " key TEXT PRIMARY KEY,"
            " ts INTEGER NOT NULL,"
            " status INTEGER NOT NULL,"
            " headers TEXT NOT NULL,"
            " body BLOB NOT NULL"
            ")"
        )
        self.lock = asyncio.Lock()

    async def get(self, key: str, ttl: int) -> Optional[Dict[str, Any]]:
        async with self.lock:
            cur = self.conn.execute("SELECT ts, status, headers, body FROM http_cache WHERE key=?", (key,))
            row = cur.fetchone()
        if not row:
            return None
        ts, status, headers, body = row
        if int(time.time()) - ts > ttl:
            return None
        try:
            headers = json.loads(headers)
        except Exception:
            headers = {}
        try:
            body = zlib.decompress(body)
        except Exception:
            pass
        return {"status": status, "headers": headers, "body": body}

    async def put(self, key: str, status: int, headers: Dict[str, Any], body: bytes):
        comp = zlib.compress(body)
        async with self.lock:
            self.conn.execute(
                "INSERT OR REPLACE INTO http_cache(key, ts, status, headers, body) VALUES(?,?,?,?,?)",
                (key, int(time.time()), status, json.dumps(dict(headers)), comp)
            )

def _build_key(url: str, params: Optional[dict]) -> str:
    if not params:
        return url
    items = "&".join(f"{k}={params[k]}" for k in sorted(params))
    return f"{url}?{items}"

class _CachedResponse:
    """Minimal aiohttp-like response for cached GETs."""
    def __init__(self, status: int, headers: Dict[str, Any], body: bytes, url: str):
        self.status = status
        self.headers = headers or {}
        self._body = body or b""
        self.url = url

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def read(self) -> bytes:
        return self._body

    async def text(self) -> str:
        return self._body.decode("utf-8", errors="replace")

    async def json(self, **kwargs):
        import json as _json
        return _json.loads(self._body.decode("utf-8", errors="replace"))

    def release(self):
        pass

_cache_db: Optional[CacheDB] = None
_original_request = None

def enable_sqlite_cache_patched(db_path: str = DEFAULT_DB, ttl: int = DEFAULT_TTL):
    global _cache_db, _original_request, aiohttp
    if aiohttp is None:
        import importlib
        aiohttp = importlib.import_module("aiohttp")

    if _cache_db is None:
        _cache_db = CacheDB(db_path)

    if _original_request is None:
        _original_request = aiohttp.ClientSession._request

        async def _cached_request(self, method, str_or_url, **kwargs):
            if method == "GET":
                url = str(str_or_url)
                params = kwargs.get("params")
                key = _build_key(url, params)
                hit = await _cache_db.get(key, ttl)
                if hit:
                    return _CachedResponse(hit["status"], hit["headers"], hit["body"], url)
                resp = await _original_request(self, method, str_or_url, **kwargs)
                body = await resp.read()
                await _cache_db.put(key, resp.status, dict(resp.headers), body)
                return _CachedResponse(resp.status, dict(resp.headers), body, url)
            return await _original_request(self, method, str_or_url, **kwargs)

        aiohttp.ClientSession._request = _cached_request

    return _cache_db


# ws_keepalive.py — aiohttp ws_connect keepalive (ping every N seconds)
import os, asyncio
try:
    import aiohttp
except Exception:
    aiohttp = None

PING_INTERVAL = int(os.environ.get("FRESH_MONEY_WS_PING", "20"))

class _KeepAliveWS:
    def __init__(self, ws):
        self._ws = ws
        self._task = None

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()
        return False

    def __getattr__(self, name):
        return getattr(self._ws, name)

    async def start(self):
        if self._task is None:
            self._task = asyncio.create_task(self._pinger())

    async def _pinger(self):
        try:
            while not self._ws.closed:
                await asyncio.sleep(PING_INTERVAL)
                try:
                    await self._ws.ping()
                except Exception:
                    break
        finally:
            pass

    async def close(self):
        if self._task:
            self._task.cancel()
        try:
            if not self._ws.closed:
                await self._ws.close()
        except Exception:
            pass

_original_ws_connect = None

def enable_ws_keepalive_patched():
    global _original_ws_connect, aiohttp
    if aiohttp is None:
        import importlib
        aiohttp = importlib.import_module("aiohttp")

    if _original_ws_connect is None:
        _original_ws_connect = aiohttp.ClientSession.ws_connect

        async def _ws_connect_with_keepalive(self, *args, **kwargs):
            ws = await _original_ws_connect(self, *args, **kwargs)
            wrapper = _KeepAliveWS(ws)
            await wrapper.start()
            return wrapper

        aiohttp.ClientSession.ws_connect = _ws_connect_with_keepalive
