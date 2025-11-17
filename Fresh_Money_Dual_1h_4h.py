#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fresh Money Dual Screener — 1h + 4h (Shared Connections)
Runs two screeners simultaneously with shared ccxt.pro clients:
 - 1h "Fast" screener (more responsive)
 - 4h "SAFE" screener (stricter gates)

Dependencies: ccxt-pro, pandas, numpy, tabulate, colorama
"""

import asyncio
import time
import random
from collections import deque

import ccxt.pro as ccxt
import pandas as pd
import numpy as np
from tabulate import tabulate
from colorama import Fore, Style, init

init(autoreset=True)

EXCHANGES_CFG = {
    'binanceusdm': {'oi_stream': False, 'vol_stream': True,  'cvd_stream': False, 'defaultType': 'future'},
    'bybit':       {'oi_stream': True,  'vol_stream': True,  'cvd_stream': True,  'defaultType': 'future'},
    'bitget':      {'oi_stream': True,  'vol_stream': True,  'cvd_stream': False, 'defaultType': 'swap'},
    'mexc':        {'oi_stream': True,  'vol_stream': True,  'cvd_stream': False, 'defaultType': 'swap'},
}

SUBSCRIPTION_DELAY = 0.20  # seconds between symbol pipelines
PRINT_INTERVAL = 10        # seconds

class ScreenerConfig:
    def __init__(self, name, timeframe, backfill, z_period, fast_ma, slow_ma, min_quote_vol, gate_z, oi_w=0.4, cvd_w=0.4, vol_w=0.2, risk_mult=0.6, sl_buf=0.2, tp2_mul=2.0, tp3_mul=3.0):
        self.name = name
        self.timeframe = timeframe
        self.backfill = backfill
        self.z_period = z_period
        self.fast_ma = fast_ma
        self.slow_ma = slow_ma
        self.min_quote_vol = min_quote_vol
        self.gate_z = gate_z
        self.oi_w = oi_w
        self.cvd_w = cvd_w
        self.vol_w = vol_w
        self.risk_mult = risk_mult
        self.sl_buf = sl_buf
        self.tp2_mul = tp2_mul
        self.tp3_mul = tp3_mul

CFG_1H = ScreenerConfig(
    name="FAST 1h",
    timeframe='1h',
    backfill=100,
    z_period=20,
    fast_ma=5,
    slow_ma=20,
    min_quote_vol=1_000_000,
    gate_z=0.52,
    risk_mult=0.6,
    sl_buf=0.20,
    tp2_mul=2.0,
    tp3_mul=3.0,
)

CFG_4H = ScreenerConfig(
    name="SAFE 4h",
    timeframe='4h',
    backfill=120,
    z_period=20,
    fast_ma=5,
    slow_ma=20,
    min_quote_vol=2_000_000,
    gate_z=0.65,
    risk_mult=0.8,
    sl_buf=0.25,
    tp2_mul=2.2,
    tp3_mul=3.3,
)

def calc_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    if df.empty:
        return pd.Series(dtype='float64')
    d = df[['high', 'low', 'close']].copy()
    d['h-l'] = d['high'] - d['low']
    d['h-pc'] = (d['high'] - d['close'].shift(1)).abs()
    d['l-pc'] = (d['low'] - d['close'].shift(1)).abs()
    tr = d[['h-l', 'h-pc', 'l-pc']].max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()

def vol_profile(df: pd.DataFrame, bins: int = 50):
    if df.empty:
        return None
    pmin = float(df['low'].min())
    pmax = float(df['high'].max())
    if not np.isfinite(pmin) or not np.isfinite(pmax) or pmax <= pmin:
        return None
    step = (pmax - pmin) / bins
    edges = np.arange(pmin, pmax + step, step)
    vols = np.zeros(len(edges))
    for _, r in df.iterrows():
        lo, hi, v = float(r['low']), float(r['high']), float(r['volume'])
        if not np.isfinite(lo) or not np.isfinite(hi) or hi <= lo: 
            continue
        i0 = np.searchsorted(edges, lo, 'left')
        i1 = np.searchsorted(edges, hi, 'right') - 1
        i0 = max(0, min(i0, len(edges)-1))
        i1 = max(0, min(i1, len(edges)-1))
        span = max(1, i1 - i0 + 1)
        vols[i0:i1+1] += v / span
    poc_idx = int(np.argmax(vols))
    poc = float(edges[poc_idx])
    tgt = float(vols.sum() * 0.7)
    L = R = poc_idx
    acc = float(vols[poc_idx])
    while acc < tgt:
        lv = vols[L-1] if L-1 >= 0 else -np.inf
        rv = vols[R+1] if R+1 < len(vols) else -np.inf
        if lv >= rv and np.isfinite(lv):
            L -= 1; acc += lv
        elif np.isfinite(rv):
            R += 1; acc += rv
        else:
            break
    val = float(edges[max(0, L)])
    vah = float(edges[min(R, len(edges)-1)])
    avg = float(vols.sum()/bins) if bins else 0.0
    hvns = [float(edges[i]) for i in range(1, len(vols)-1) if vols[i] > vols[i-1] and vols[i] > vols[i+1] and vols[i] > 1.5*avg]
    hvns = sorted([x for x in hvns if x > vah or x < val])
    return {'poc': poc, 'vah': vah, 'val': val, 'hvns': hvns}

class Screener:
    def __init__(self, cfg: ScreenerConfig, exchanges: dict, symbols: list[str]):
        self.cfg = cfg
        self.exchanges = exchanges
        self.symbols = symbols
        self.state = {}        # symbol -> {'lock', 'last_update', 'df_deque'}
        self.top = []          # ranked results
        self.lock = asyncio.Lock()

    async def backfill_symbol(self, ex, symbol: str):
        try:
            ohlcv = await ex.fetch_ohlcv(symbol, timeframe=self.cfg.timeframe, limit=self.cfg.backfill + 2)
            if not ohlcv or len(ohlcv) < 10:
                return None
            df = pd.DataFrame(ohlcv, columns=['timestamp','open','high','low','close','volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)

            oi_series = None
            if getattr(ex, 'has', {}).get('fetchOpenInterestHistory', False):
                try:
                    oi_data = await ex.fetch_open_interest_history(symbol, timeframe=self.cfg.timeframe, limit=self.cfg.backfill + 2)
                    if oi_data:
                        oi_df = pd.DataFrame(oi_data)
                        if 'timestamp' in oi_df and 'openInterestValue' in oi_df:
                            oi_df['timestamp'] = pd.to_datetime(oi_df['timestamp'], unit='ms')
                            oi_df.set_index('timestamp', inplace=True)
                            oi_series = oi_df['openInterestValue'].astype(float).resample(self.cfg.timeframe, label='right', closed='right').last()
                except Exception:
                    oi_series = None
            if oi_series is None:
                oi_series = pd.Series(index=df.index, data=np.nan, dtype='float64')
            df['openInterestValue'] = oi_series.reindex(df.index).ffill()

            df['cvd'] = 0.0
            df = df[['open','high','low','close','volume','openInterestValue','cvd']].dropna()
            return df
        except Exception as e:
            print(f"{Fore.RED}Backfill error {ex.id} {symbol}: {e}{Style.RESET_ALL}")
            return None

    async def initialize(self):
        print(f"{Fore.BLUE}[{self.cfg.name}] Backfilling {self.cfg.backfill} bars per venue…{Style.RESET_ALL}")
        sem = asyncio.Semaphore(10)

        async def agg_one(symbol: str):
            async with sem:
                dfs = []
                for ex_id in EXCHANGES_CFG.keys():
                    ex = self.exchanges[ex_id]
                    df = await self.backfill_symbol(ex, symbol)
                    if df is not None and not df.empty:
                        df = df.copy()
                        df[f'vol_{ex_id}'] = df['volume']
                        dfs.append(df)
                if not dfs:
                    return
                base = dfs[0].copy()
                for add in dfs[1:]:
                    base = base.join(add[['volume','openInterestValue']], how='outer', rsuffix='_r')
                    base['volume'] = base[['volume','volume_r']].sum(axis=1, skipna=True)
                    base.drop(columns=['volume_r'], inplace=True)
                    base['openInterestValue'] = base[['openInterestValue','openInterestValue_r']].sum(axis=1, skipna=True)
                    base.drop(columns=['openInterestValue_r'], inplace=True)
                base = base[['open','high','low','close','volume','openInterestValue','cvd']].sort_index().ffill().dropna()
                if base['volume'].iloc[-1] < self.cfg.min_quote_vol:
                    return
                self.state[symbol] = {
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
                            for ts, row in base.tail(self.cfg.backfill).iterrows()
                        ],
                        maxlen=self.cfg.backfill + 5
                    )
                }

        await asyncio.gather(*[agg_one(s) for s in self.symbols])
        kept = len(self.state)
        print(f"{Fore.BLUE}[{self.cfg.name}] Initialized {kept} symbols{Style.RESET_ALL}")
        return kept > 0

    async def update_and_rank(self, symbol: str):
        try:
            async with self.state[symbol]['lock']:
                dq = self.state[symbol]['df_deque']
                df = pd.DataFrame(dq)
                if df.empty or len(df) < max(self.cfg.z_period, self.cfg.slow_ma) + 1:
                    return
                vol_mean = df['volume'].rolling(self.cfg.z_period).mean().iloc[-1]
                vol_std  = df['volume'].rolling(self.cfg.z_period).std().iloc[-1]
                vol_z = (df['volume'].iloc[-1] - vol_mean) / (vol_std if vol_std else 1.0)
                oi_mean = df['openInterestValue'].rolling(self.cfg.z_period).mean().iloc[-1]
                oi_std  = df['openInterestValue'].rolling(self.cfg.z_period).std().iloc[-1]
                oi_z = (df['openInterestValue'].iloc[-1] - oi_mean) / (oi_std if oi_std else 1.0)
                cvd_fast = df['cvd'].rolling(self.cfg.fast_ma).mean().iloc[-1]
                cvd_slow = df['cvd'].rolling(self.cfg.slow_ma).mean().iloc[-1]
                cvd_accel = cvd_fast - cvd_slow

                cvd_acc_series = df['cvd'].rolling(self.cfg.z_period).apply(
                    lambda x: (pd.Series(x).rolling(self.cfg.fast_ma).mean().iloc[-1] - pd.Series(x).rolling(self.cfg.slow_ma).mean().iloc[-1]),
                    raw=False
                ).dropna()
                mu = float(cvd_acc_series.mean()) if len(cvd_acc_series) else 0.0
                sd = float(cvd_acc_series.std()) if len(cvd_acc_series) else 1.0
                cvd_acc_z = (cvd_accel - mu) / (sd if sd else 1.0)

                passes = (oi_z > self.cfg.gate_z) and (cvd_acc_z > self.cfg.gate_z)
                score = self.cfg.oi_w*oi_z + self.cfg.cvd_w*cvd_acc_z + self.cfg.vol_w*vol_z

                trade = None
                vp = vol_profile(df.tail(self.cfg.z_period))
                if passes and vp:
                    atr = calc_atr(df, 14)
                    if len(atr) and np.isfinite(atr.iloc[-1]) and atr.iloc[-1] > 0:
                        atr_v = float(atr.iloc[-1])
                        poc, vah, val = vp['poc'], vp['vah'], vp['val']
                        entry = float(poc)
                        risk = atr_v * self.cfg.risk_mult
                        sl = max(0.0, float(val) - self.cfg.sl_buf * atr_v)
                        tp1 = float(vah)
                        hvn_up = [h for h in vp['hvns'] if h > tp1]
                        tp2 = float(hvn_up[0]) if hvn_up else entry + self.cfg.tp2_mul * risk
                        tp3 = float(hvn_up[1]) if len(hvn_up) > 1 else entry + self.cfg.tp3_mul * risk
                        if tp1 > entry > sl > 0:
                            trade = {
                                'price': float(df['close'].iloc[-1]),
                                'entry': entry, 'sl': sl, 'tp1': tp1, 'tp2': tp2, 'tp3': tp3
                            }

            async with self.lock:
                self.top = [x for x in self.top if x['symbol'] != symbol]
                if passes and trade:
                    self.top.append({
                        'symbol': symbol,
                        'score': float(score),
                        'oi_z': float(oi_z),
                        'cvd_z': float(cvd_acc_z),
                        'vol_z': float(vol_z),
                        **trade
                    })
                self.top.sort(key=lambda x: x['score'], reverse=True)
        except Exception as e:
            print(f"{Fore.RED}[{self.cfg.name}] update_and_rank {symbol}: {e}{Style.RESET_ALL}")

    async def watch_candles(self, ex, symbol: str):
        retry = 5.0
        while True:
            try:
                candles = await ex.watch_ohlcv(symbol, self.cfg.timeframe)
                if symbol not in self.state: 
                    continue
                async with self.state[symbol]['lock']:
                    dq = self.state[symbol]['df_deque']
                    if not dq or len(candles) < 2: 
                        continue
                    t, o, h, l, c, v = candles[-2]
                    last_ts = dq[-1]['timestamp']
                    if t > last_ts:
                        dq.append({
                            'timestamp': int(t),
                            'open': float(o),'high': float(h),'low': float(l),'close': float(c),
                            'volume': float(v),
                            'openInterestValue': dq[-1].get('openInterestValue', 0.0),
                            'cvd': dq[-1].get('cvd', 0.0),
                        })
                        self.state[symbol]['last_update'] = int(t)
                        asyncio.create_task(self.update_and_rank(symbol))
                    else:
                        dq[-1]['volume'] = float(v)
                retry = 5.0
            except Exception as e:
                print(f"{Fore.RED}[{self.cfg.name}] {ex.id} candles {symbol}: {e}{Style.RESET_ALL}")
                await asyncio.sleep(retry + random.uniform(0,3))
                retry = min(retry*2, 300)

    async def watch_oi(self, ex, symbol: str):
        if not getattr(ex, 'has', {}).get('watchOpenInterest', False):
            return
        retry = 5.0
        while True:
            try:
                oi = await ex.watch_open_interest(symbol)
                if symbol not in self.state:
                    continue
                async with self.state[symbol]['lock']:
                    dq = self.state[symbol]['df_deque']
                    if dq:
                        val = float(oi.get('openInterestValue') or oi.get('openInterestUsd', 0.0) or 0.0)
                        dq[-1]['openInterestValue'] = max(dq[-1].get('openInterestValue', 0.0), val)
                retry = 5.0
            except Exception:
                await asyncio.sleep(retry + random.uniform(0,3))
                retry = min(retry*2, 300)

    async def watch_trades(self, ex, symbol: str):
        if not getattr(ex, 'has', {}).get('watchTrades', False):
            return
        retry = 5.0
        while True:
            try:
                trades = await ex.watch_trades(symbol)
                if symbol not in self.state:
                    continue
                async with self.state[symbol]['lock']:
                    dq = self.state[symbol]['df_deque']
                    if not dq:
                        continue
                    base = float(dq[-1].get('cvd', 0.0))
                    for t in trades:
                        side = t.get('side')
                        amt = float(t.get('amount', 0.0) or 0.0)
                        signed = amt if side == 'buy' else (-amt if side == 'sell' else 0.0)
                        base += signed
                    dq[-1]['cvd'] = base
                if trades and trades[-1].get('timestamp', 0) > self.state[symbol]['last_update']:
                    asyncio.create_task(self.update_and_rank(symbol))
                retry = 5.0
            except Exception:
                await asyncio.sleep(retry + random.uniform(0,3))
                retry = min(retry*2, 300)

    async def subscribe_all(self):
        tasks = []
        for symbol in self.symbols:
            if symbol not in self.state:
                continue
            for ex_id, cfg in EXCHANGES_CFG.items():
                ex = self.exchanges[ex_id]
                if getattr(ex, 'has', {}).get('watchOHLCV', False):
                    tasks.append(asyncio.create_task(self.watch_candles(ex, symbol)))
                if cfg['oi_stream'] and getattr(ex, 'has', {}).get('watchOpenInterest', False):
                    tasks.append(asyncio.create_task(self.watch_oi(ex, symbol)))
                if cfg['cvd_stream'] and ex_id == 'bybit':
                    tasks.append(asyncio.create_task(self.watch_trades(ex, symbol)))
            await asyncio.sleep(SUBSCRIPTION_DELAY)
        print(f"{Fore.GREEN}[{self.cfg.name}] Subscriptions ready. Streams running: {len(tasks)}{Style.RESET_ALL}")
        await asyncio.gather(*tasks)

    async def printer(self):
        while True:
            await asyncio.sleep(PRINT_INTERVAL)
            try:
                async with self.lock:
                    rows = []
                    for i, r in enumerate(self.top[:5], 1):
                        pr = 2 if r['price']>=10 else (4 if r['price']>=0.1 else (6 if r['price']>=0.001 else 8))
                        rows.append([
                            i,
                            f"{Fore.YELLOW}{Style.BRIGHT}{r['symbol']}{Style.RESET_ALL}",
                            f"{Fore.MAGENTA}{r['score']:.2f}{Style.RESET_ALL}",
                            f"{Fore.GREEN if r['oi_z']>1 else Fore.WHITE}{r['oi_z']:.2f}{Style.RESET_ALL}",
                            f"{Fore.GREEN if r['cvd_z']>1 else Fore.WHITE}{r['cvd_z']:.2f}{Style.RESET_ALL}",
                            f"{Fore.GREEN if r['vol_z']>1 else Fore.WHITE}{r['vol_z']:.2f}{Style.RESET_ALL}",
                            f"{r['price']:.{pr}f}",
                            f"{Fore.CYAN}{r['entry']:.{pr}f}{Style.RESET_ALL}",
                            f"{Fore.RED}{r['sl']:.{pr}f}{Style.RESET_ALL}",
                            f"{Fore.GREEN}{r['tp1']:.{pr}f}{Style.RESET_ALL}",
                            f"{Fore.GREEN}{r['tp2']:.{pr}f}{Style.RESET_ALL}",
                            f"{Fore.GREEN}{r['tp3']:.{pr}f}{Style.RESET_ALL}",
                        ])
                    headers = ["#", "Symbol", "Score 🔥", "OI Z 📈", "CVD Z 💨", "Vol Z 📊",
                               "Price", "Entry (POC) 🔵", "SL 🔴", "TP1 🟢", "TP2 🟢🟢", "TP3 🟢🟢🟢"]
                    print("\n" + Style.BRIGHT + Fore.CYAN + f"=== {self.cfg.name} (UTC {time.strftime('%Y-%m-%d %H:%M:%S')}) ===" + Style.RESET_ALL)
                    if rows:
                        print(tabulate(rows, headers=headers, tablefmt="fancy_grid"))
                    else:
                        print("⏳ Warming up… awaiting qualifying signals.")
            except Exception as e:
                print(f"{Fore.RED}[{self.cfg.name}] printer: {e}{Style.RESET_ALL}")

async def init_exchanges():
    exchanges = {}
    for ex_id, c in EXCHANGES_CFG.items():
        try:
            klass = getattr(ccxt, ex_id)
            ex = klass({'enableRateLimit': True, 'options': {'defaultType': c['defaultType']}})
            await ex.load_markets()
            exchanges[ex_id] = ex
            print(f"Loaded markets: {ex_id} → {len(ex.markets)}")
        except Exception as e:
            print(f"{Fore.RED}Failed to load {ex_id}: {e}{Style.RESET_ALL}")
            raise
    return exchanges

def common_usdt_symbols(exchanges: dict) -> list[str]:
    universe_per_ex = []
    for ex in exchanges.values():
        syms = [s for s,m in ex.markets.items() if isinstance(s, str) and '/USDT' in s and m.get('active', True)]
        universe_per_ex.append(set(syms))
    common = set.intersection(*universe_per_ex) if universe_per_ex else set()
    print(f"Common /USDT symbols across venues: {len(common)}")
    return sorted(common)

async def main():
    print(f"{Fore.GREEN}{Style.BRIGHT}--- Fresh Money Dual Screener (1h + 4h) ---{Style.RESET_ALL}")
    exchanges = await init_exchanges()
    symbols = common_usdt_symbols(exchanges)

    oneh = Screener(CFG_1H, exchanges, symbols)
    fourh = Screener(CFG_4H, exchanges, symbols)

    ok1 = await oneh.initialize()
    ok2 = await fourh.initialize()
    if not ok1 and not ok2:
        print(f"{Fore.RED}Initialization failed for both timeframes. Exiting.{Style.RESET_ALL}")
        for ex in exchanges.values():
            try: await ex.close()
            except Exception: pass
        return

    # Printers
    asyncio.create_task(oneh.printer())
    asyncio.create_task(fourh.printer())

    # Subscriptions (both TFs share clients but watch separate TF streams)
    try:
        await asyncio.gather(
            oneh.subscribe_all(),
            fourh.subscribe_all(),
        )
    finally:
        for ex in exchanges.values():
            try:
                await ex.close()
            except Exception:
                pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped by user.")
