#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fresh Money v5 (in-loop) — Spot–Perp Inflow Screener with Magnetic Targets
Runs Cryptofeed INSIDE the existing asyncio loop (Thonny/Jupyter safe):
- uses await fh.start() (no signal handlers, no extra threads)
- watchdog keeps resubscribing a probe-set if 90s without flow
- discovery on Binance/Bybit/Bitget; real TRADES/L2/FUNDING + REST OI slope
"""

import asyncio, argparse, time, random, inspect
from collections import deque, defaultdict
from statistics import mean, pstdev
from typing import Dict, Deque, List, Tuple, Optional

# ---- cryptofeed imports (guarded)
HAVE_CF = True
CF_IMPORT_ERR = None
try:
    from cryptofeed import FeedHandler
    from cryptofeed.defines import TRADES, L2_BOOK, FUNDING
    try:
        from cryptofeed.exchanges import Binance as CF_Binance
    except Exception:
        CF_Binance = None
    try:
        from cryptofeed.exchanges import BinanceFutures as CF_BinanceFutures
    except Exception:
        CF_BinanceFutures = None
    try:
        from cryptofeed.exchanges import Bybit as CF_Bybit
    except Exception:
        CF_Bybit = None
    try:
        from cryptofeed.exchanges import Bitget as CF_Bitget
    except Exception:
        CF_Bitget = None
except Exception as e:
    HAVE_CF = False
    CF_IMPORT_ERR = str(e)

# ---- REST deps
try:
    import aiohttp
    HAVE_AIOHTTP = True
except Exception:
    HAVE_AIOHTTP = False

# ---- config
OBI_LEVELS = 10
RVOL_LOOKBACK_MINUTES = 60
TOP_N = 5
WEIGHTS = {"CVD_PERP":0.26,"CVD_SPOT":0.22,"DIV":0.14,"OBI":0.08,"RVOL":0.06,"OI_SLOPE":0.24}
FUNDING_CROWD_THRESHOLD = 0.0005
DEFAULT_OI_INTERVAL_SEC = 60
OI_HISTORY_MINUTES = 15
OI_MIN_POINTS_FOR_SLOPE = 3
PER_HOST_RPS = {"fapi.binance.com":4,"api.binance.com":4,"api.bybit.com":3,"api.bitget.com":3}
JITTER_MS = (100,600)
ATR_LEN = 14
TP_ATR_MULTS = (0.8,1.6,2.5)
SL_ATR_MULT = 1.8
MAGNET_MIN_STRENGTH = 0.15
MAGNET_SNAP_TOL = 0.003
SL_BID_SAFETY = 1.10
WATCHDOG_SECS = 90

# ---- helpers
def norm_key(sym_like: str) -> str:
    s = sym_like.upper().replace("-", "").replace("_UMCBL", "").replace("PERP", "")
    return s.replace("USDTUSDT","USDT")

def to_cf_perp(sym_ccy: str) -> str:
    base, quote = sym_ccy[:-4], sym_ccy[-4:]
    return f"{base}-{quote}-PERP"

def to_cf_spot(sym_ccy: str) -> str:
    base, quote = sym_ccy[:-4], sym_ccy[-4:]
    return f"{base}-{quote}"

def now_utc_str() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

def _slope(xs: List[float], ys: List[float]) -> float:
    n=len(xs)
    if n<2: return 0.0
    xm=sum(xs)/n; ym=sum(ys)/n
    num=sum((x-xm)*(y-ym) for x,y in zip(xs,ys))
    den=sum((x-xm)**2 for x in xs) or 1.0
    return num/den

# ---- rolling structs
class RollingTrades:
    def __init__(self, maxlen=500_000):
        self.deq: Deque[Tuple[float,float,int,float]] = deque(maxlen=maxlen)
    def add(self, ts: float, dollar: float, side_buy: bool, price: float):
        self.deq.append((ts, dollar, 1 if side_buy else -1, price))
    def cvd(self, minutes: int) -> float:
        cutoff=time.time()-minutes*60
        s=0.0
        for ts,d,sgn,_ in reversed(self.deq):
            if ts<cutoff: break
            s+=d*sgn
        return s

class BookSnapshot:
    def __init__(self, levels=10):
        self.levels=levels; self.last_obi=0.0; self.bids={}; self.asks={}
    def update(self, bids, asks):
        self.bids=dict(bids); self.asks=dict(asks)
        bsum=0.0; asum=0.0
        for i,(p,sz) in enumerate(sorted(self.bids.items(), key=lambda x: float(x[0]), reverse=True)):
            if i>=self.levels: break
            bsum += float(sz)
        for i,(p,sz) in enumerate(sorted(self.asks.items(), key=lambda x: float(x[0]))):
            if i>=self.levels: break
            asum += float(sz)
        denom=max(bsum+asum,1e-12)
        self.last_obi=(bsum-asum)/denom
    def price_magnets(self, side, topk=5) -> List[Tuple[float,float]]:
        if side=="ask":
            lvls=sorted(self.asks.items(), key=lambda x: float(x[0]))[:self.levels]
        else:
            lvls=sorted(self.bids.items(), key=lambda x: float(x[0]), reverse=True)[:self.levels]
        sizes=[float(sz) for _,sz in lvls] or [0.0]
        total=sum(sizes) or 1.0
        pairs=[(float(p), float(sz)/total) for (p,sz) in lvls]
        pairs.sort(key=lambda x:x[1], reverse=True)
        return pairs[:topk]

class SymbolState:
    def __init__(self, key: str):
        self.key=key
        self.trades_perp=RollingTrades(); self.trades_spot=RollingTrades()
        self.book_perp=BookSnapshot(OBI_LEVELS); self.book_spot=BookSnapshot(OBI_LEVELS)
        self.vols_1m: Deque[float]=deque(maxlen=RVOL_LOOKBACK_MINUTES)
        self.last_minute=int(time.time()//60)
        self.funding_rate=0.0
        self.oi_points: Deque[Tuple[int,float]] = deque(maxlen=OI_HISTORY_MINUTES+5)
        self.cur_minute=int(time.time()//60)
        self.mbar_o=self.mbar_h=self.mbar_l=self.mbar_c=None
        self.mbars: Deque[Tuple[int,float,float,float,float,int]] = deque(maxlen=600)
        self.last_px=0.0
    def _tick_minute_bucket(self):
        now=int(time.time()//60)
        if now!=self.last_minute:
            self.last_minute=now; self.vols_1m.append(0.0)
    def add_perp_volume(self, dollar: float):
        self._tick_minute_bucket()
        if not self.vols_1m: self.vols_1m.append(dollar)
        else: self.vols_1m[-1]+=dollar
    def rvol_z(self) -> float:
        if len(self.vols_1m)<10: return 0.0
        cur=self.vols_1m[-1]; hist=list(self.vols_1m)[:-1]
        if len(hist)<10: return 0.0
        mu=mean(hist); sd=pstdev(hist) or 1.0
        return (cur-mu)/sd
    def record_oi(self, value: float):
        m=int(time.time()//60)
        if self.oi_points and self.oi_points[-1][0]==m:
            self.oi_points[-1]=(m,value)
        else:
            self.oi_points.append((m,value))
    def oi_slope(self) -> float:
        if len(self.oi_points)<OI_MIN_POINTS_FOR_SLOPE: return 0.0
        xs=list(range(len(self.oi_points))); ys=[v for _,v in self.oi_points]
        return _slope(xs,ys)
    def on_trade_price(self, price: float, dollar: float):
        self.last_px=float(price)
        m=int(time.time()//60)
        if self.mbar_o is None:
            self.mbar_o=self.mbar_h=self.mbar_l=self.mbar_c=price
            self.cur_minute=m; self._ticks=1; self.vols_1m.append(dollar); return
        if m!=self.cur_minute:
            self.mbars.append((self.cur_minute,self.mbar_o,self.mbar_h,self.mbar_l,self.mbar_c,getattr(self,"_ticks",0)))
            self.cur_minute=m; self.mbar_o=self.mbar_h=self.mbar_l=self.mbar_c=price; self._ticks=1; self.vols_1m.append(dollar)
        else:
            self.mbar_h=max(self.mbar_h,price); self.mbar_l=min(self.mbar_l,price); self.mbar_c=price
            self._ticks=getattr(self,"_ticks",0)+1
            if self.vols_1m: self.vols_1m[-1]+=dollar
            else: self.vols_1m.append(dollar)
    def atr(self, length=ATR_LEN) -> float:
        if len(self.mbars)<length+1: return 0.0
        trs=[]; prev_c=self.mbars[-(length+1)][3]
        for _,o,h,l,c,_ in list(self.mbars)[-(length):]:
            tr=max(h-l,abs(h-prev_c),abs(l-prev_c)); trs.append(tr); prev_c=c
        return sum(trs)/len(trs) if trs else 0.0
    def swing_high(self, minutes: int) -> float:
        if not self.mbars: return 0.0
        cutoff=int(time.time()//60)-minutes
        bars=[b for b in self.mbars if b[0]>=cutoff] or list(self.mbars)[-minutes:]
        return max((h for _,_,h,_,_,_ in bars), default=0.0)
    def swing_low(self, minutes: int) -> float:
        if not self.mbars: return 0.0
        cutoff=int(time.time()//60)-minutes
        bars=[b for b in self.mbars if b[0]>=cutoff] or list(self.mbars)[-minutes:]
        return min((l for _,_,_,l,_,_ in bars), default=0.0)

class TokenBucket:
    def __init__(self, rate_per_sec: float, burst: int):
        self.rate=rate_per_sec; self.tokens=burst; self.capacity=burst; self.updated=time.time()
    def consume(self, tokens: int = 1) -> bool:
        now=time.time(); delta=now-self.updated; self.updated=now
        self.tokens=min(self.capacity, self.tokens+delta*self.rate)
        if self.tokens>=tokens:
            self.tokens-=tokens
            return True
        return False

class Screener:
    def __init__(self, debug: bool, oi_interval_sec: int, max_per_venue: int):
        self.debug=debug; self.oi_interval=max(10,int(oi_interval_sec)); self.max_per_venue=max(0,int(max_per_venue))
        self.states: Dict[str,SymbolState]={}; self.lock=asyncio.Lock()
        self.cf_perp_to_key: Dict[str,str]={}; self.cf_spot_to_key: Dict[str,str]={}
        self.counts=defaultdict(int); self.debug_samples={}
        self.session: Optional[aiohttp.ClientSession]=None
        self.buckets={h:TokenBucket(rps,max(2,rps*2)) for h,rps in PER_HOST_RPS.items()}
        self._last_any_msg_ts=time.time()

    def ensure_state(self, key: str) -> SymbolState:
        if key not in self.states:
            self.states[key]=SymbolState(key)
        return self.states[key]

    # ---- callbacks
    async def on_trade_perp(self, *args, **kwargs):
        self.counts[("perp","TRADES")]+=1; self._last_any_msg_ts=time.time()
        ts=kwargs.get("receipt_timestamp",time.time())
        if args and hasattr(args[0],"symbol"):
            t=args[0]; cf_sym=t.symbol; side=(t.side or "").lower(); price=float(t.price); amount=float(t.amount)
        else:
            feed, cf_sym, *_=args
            side=str(args[3]).lower() if len(args)>=4 else kwargs.get("side","buy")
            amount=float(args[4]) if len(args)>=5 else float(kwargs.get("amount",0.0))
            price=float(args[5]) if len(args)>=6 else float(kwargs.get("price",0.0))
        if self.debug and ("perp","TRADES") not in self.debug_samples:
            self.debug_samples[("perp","TRADES")]=(cf_sym,side,price,amount)
        key=self.cf_perp_to_key.get(cf_sym)
        if not key:
            return
        dollar=price*amount
        st=self.ensure_state(key)
        st.trades_perp.add(ts,dollar,side.startswith("b"),price)
        st.on_trade_price(price,dollar)

    async def on_trade_spot(self, *args, **kwargs):
        self.counts[("spot","TRADES")]+=1; self._last_any_msg_ts=time.time()
        ts=kwargs.get("receipt_timestamp",time.time())
        if args and hasattr(args[0],"symbol"):
            t=args[0]; cf_sym=t.symbol; side=(t.side or "").lower(); price=float(t.price); amount=float(t.amount)
        else:
            feed, cf_sym, *_=args
            side=str(args[3]).lower() if len(args)>=4 else kwargs.get("side","buy")
            amount=float(args[4]) if len(args)>=5 else float(kwargs.get("amount",0.0))
            price=float(args[5]) if len(args)>=6 else float(kwargs.get("price",0.0))
        if self.debug and ("spot","TRADES") not in self.debug_samples:
            self.debug_samples[("spot","TRADES")]=(cf_sym,side,price,amount)
        key=self.cf_spot_to_key.get(cf_sym)
        if not key:
            return
        self.ensure_state(key).trades_spot.add(ts, price*amount, side.startswith("b"), price)

    async def on_book_perp(self, *args, **kwargs):
        self.counts[("perp","L2")]+=1; self._last_any_msg_ts=time.time()
        if self.debug and ("perp","L2") not in self.debug_samples:
            self.debug_samples[("perp","L2")]=args[:2]
        if args and hasattr(args[0],"symbol"):
            b=args[0]; cf_sym=b.symbol; bids=b.book.bids; asks=b.book.asks
        else:
            feed, cf_sym, book, *_=args; bids=book.bids; asks=book.asks
        key=self.cf_perp_to_key.get(cf_sym)
        if not key:
            return
        self.ensure_state(key).book_perp.update(bids,asks)

    async def on_book_spot(self, *args, **kwargs):
        self.counts[("spot","L2")]+=1; self._last_any_msg_ts=time.time()
        if self.debug and ("spot","L2") not in self.debug_samples:
            self.debug_samples[("spot","L2")]=args[:2]
        if args and hasattr(args[0],"symbol"):
            b=args[0]; cf_sym=b.symbol; bids=b.book.bids; asks=b.book.asks
        else:
            feed, cf_sym, book, *_=args; bids=book.bids; asks=book.asks
        key=self.cf_spot_to_key.get(cf_sym)
        if not key:
            return
        self.ensure_state(key).book_spot.update(bids,asks)

    async def on_funding(self, *args, **kwargs):
        self.counts[("perp","FUNDING")]+=1; self._last_any_msg_ts=time.time()
        if self.debug and ("perp","FUNDING") not in self.debug_samples:
            self.debug_samples[("perp","FUNDING")]=args[:2]
        if args and hasattr(args[0],"symbol"):
            f=args[0]; cf_sym=f.symbol; rate=float(getattr(f,"rate",0.0))
        else:
            feed, cf_sym, rate, *_=args; rate=float(rate)
        key=self.cf_perp_to_key.get(cf_sym)
        if not key:
            return
        self.ensure_state(key).funding_rate=rate

    # ---- scoring / targets
    def score_one(self, key: str) -> Dict[str, float]:
        st=self.ensure_state(key)
        perp_cvd_5=st.trades_perp.cvd(5); spot_cvd_5=st.trades_spot.cvd(5)
        perp_cvd_60=st.trades_perp.cvd(60); spot_cvd_60=st.trades_spot.cvd(60)
        div_5=spot_cvd_5 - perp_cvd_5
        hist=getattr(st,"_cvd_hist",deque(maxlen=240)); hist.append(perp_cvd_60+spot_cvd_60); st._cvd_hist=hist
        mu=mean(hist) if len(hist)>10 else 1.0; sd=pstdev(hist) if len(hist)>10 else 1.0
        z=lambda x:(x-mu)/(sd or 1.0)
        rvol=st.rvol_z(); obi=st.book_perp.last_obi
        fund_pen=(abs(st.funding_rate)-FUNDING_CROWD_THRESHOLD)*100.0 if abs(st.funding_rate)>FUNDING_CROWD_THRESHOLD else 0.0
        oi_slope=st.oi_slope()
        score=(WEIGHTS["CVD_PERP"]*z(perp_cvd_60)+WEIGHTS["CVD_SPOT"]*z(spot_cvd_60)+WEIGHTS["DIV"]*z(div_5)+
               WEIGHTS["OBI"]*obi+WEIGHTS["RVOL"]*rvol+WEIGHTS["OI_SLOPE"]*oi_slope - fund_pen)
        return {"score":score,"perp_cvd_5":perp_cvd_5,"spot_cvd_5":spot_cvd_5,"div_5":div_5,
                "perp_cvd_60":perp_cvd_60,"spot_cvd_60":spot_cvd_60,"rvol_z":rvol,"obi":obi,"fund":st.funding_rate,"oi_slope":oi_slope}

    def _ceilings(self, st: SymbolState) -> Tuple[float,float,float]:
        return (st.swing_high(15), st.swing_high(60), st.swing_high(240))
    def _magnets_above(self, st: SymbolState, px: float) -> List[Tuple[float,float]]:
        return [(p,w) for p,w in st.book_perp.price_magnets("ask",6) if p>=px and w>=MAGNET_MIN_STRENGTH]
    def _magnets_below(self, st: SymbolState, px: float) -> List[Tuple[float,float]]:
        return [(p,w) for p,w in st.book_perp.price_magnets("bid",6) if p<=px and w>=MAGNET_MIN_STRENGTH]
    def _snap_up(self, tgt: float, mags_up: List[Tuple[float,float]], ceil: float) -> float:
        c=ceil if ceil>0 else float("inf")
        cands=[p for p,_ in mags_up if p>=tgt and p<=c]
        if not cands:
            close=sorted([(abs(p-tgt)/max(tgt,1e-9),p) for p,_ in mags_up])
            return close[0][1] if (close and close[0][0]<=MAGNET_SNAP_TOL) else tgt
        return min(cands, key=lambda x:x-tgt)
    def _safe_sl(self, entry: float, atr: float, st: SymbolState) -> float:
        base=entry - SL_ATR_MULT*atr if atr>0 else entry*(1-0.01)
        bids=self._magnets_below(st, entry)
        if bids:
            nearest=max(bids, key=lambda x:x[0])[0]
            base=min(base, nearest/SL_BID_SAFETY)
        lo15=st.swing_low(15)
        if lo15>0:
            base=min(base, lo15*0.999)
        return base
    def build_targets(self, st: SymbolState) -> Tuple[float,float,float,float,float]:
        px=st.last_px or 0.0
        if px<=0: return (0,0,0,0,0)
        atr=st.atr(ATR_LEN); c15,c60,c240=self._ceilings(st); mags_up=self._magnets_above(st,px)
        tp_raw=[px+m*atr for m in TP_ATR_MULTS] if atr>0 else [px*1.006,px*1.012,px*1.02]
        ceils=[c15 or float("inf"), c60 or float("inf"), c240 or float("inf")]
        tp=[self._snap_up(t, mags_up, c) for t,c in zip(tp_raw, ceils)]
        sl=self._safe_sl(px, atr, st)
        return (px, sl, tp[0], tp[1], tp[2])

    # ---- printers & diagnostics
    async def print_counters(self):
        while True:
            await asyncio.sleep(30)
            parts=[f"{side}:{ch}={c}" for (side,ch),c in sorted(self.counts.items())]
            msg=" | ".join(parts) if parts else "no messages yet"
            print(f"[MSG] last 30s → {msg}")
            if self.debug and self.debug_samples:
                print(f"[SAMPLE] {self.debug_samples}")

    async def rank_and_print(self):
        while True:
            rows=[]
            async with self.lock:
                keys=list(self.states.keys())
            for k in keys:
                m=self.score_one(k); rows.append((k, m["score"], m))
            rows.sort(key=lambda x:x[1], reverse=True)
            ts=now_utc_str()
            print("="*140); print(f"Top {TOP_N} — {ts} UTC (Fresh-Money Longs + Magnetic Targets)"); print("-"*140)
            shown=0
            for i,(k,s,m) in enumerate(rows,1):
                st=self.states[k]
                if (m["perp_cvd_5"]==0 and m["spot_cvd_5"]==0 and m["perp_cvd_60"]==0 and m["spot_cvd_60"]==0) or (st.last_px<=0) or (len(st.mbars)<ATR_LEN+1):
                    continue
                entry,sl,tp1,tp2,tp3=self.build_targets(st); rr=(tp2-entry)/max(entry-sl,1e-9)
                print(f"{i:>2}. {k:12} | S:{s:6.3f} | OISlp:{m['oi_slope']:7.4f} | DIV5:{m['div_5']:9.1f} | "
                      f"P60:{m['perp_cvd_60']:10.1f} | S60:{m['spot_cvd_60']:10.1f} | RVOLz:{m['rvol_z']:5.2f} | "
                      f"OBI:{m['obi']:6.3f} | F:{m['fund']:+.5f} || Entry:{entry:.6g} SL:{sl:.6g} "
                      f"TP1:{tp1:.6g} TP2:{tp2:.6g} TP3:{tp3:.6g} RR@TP2:{rr:4.2f}")
                shown+=1
                if shown>=TOP_N: break
            if shown==0:
                print("⏳ Warming up… if this persists, watchdog will auto-probe and rebuild subs.")
            print("="*140)
            await asyncio.sleep(60)

    # ---- discovery
    async def discover(self, which: List[str]) -> Dict[str,List[str]]:
        result={"perp":[],"spot":[]}
        tasks=[]
        if "binance" in which: tasks.append(self._discover_binance())
        if "bybit"   in which: tasks.append(self._discover_bybit())
        if "bitget"  in which: tasks.append(self._discover_bitget())
        if not tasks: return result
        discovered=await asyncio.gather(*tasks, return_exceptions=True)
        for ent in discovered:
            if isinstance(ent,dict):
                result["perp"].extend(ent.get("perp",[]))
                result["spot"].extend(ent.get("spot",[]))
        result["perp"]=sorted(set(result["perp"]))
        result["spot"]=sorted(set(result["spot"]))
        if self.max_per_venue>0:
            result["perp"]=result["perp"][:self.max_per_venue]
            result["spot"]=result["spot"][:self.max_per_venue]
        return result

    async def _discover_binance(self) -> Dict[str,List[str]]:
        out={"perp":[],"spot":[]}
        if not HAVE_AIOHTTP: return out
        timeout=aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            try:
                async with s.get("https://fapi.binance.com/fapi/v1/exchangeInfo") as r:
                    j=await r.json()
                    for sym in j.get("symbols",[]):
                        if sym.get("status")!="TRADING" or sym.get("quoteAsset")!="USDT": continue
                        out["perp"].append(to_cf_perp(sym.get("symbol")))
            except Exception: pass
            try:
                async with s.get("https://api.binance.com/api/v3/exchangeInfo") as r:
                    j=await r.json()
                    for sym in j.get("symbols",[]):
                        if sym.get("status")!="TRADING" or sym.get("quoteAsset")!="USDT" or (sym.get("isSpotTradingAllowed") is False): continue
                        out["spot"].append(to_cf_spot(sym.get("symbol")))
            except Exception: pass
        return out

    async def _discover_bybit(self) -> Dict[str,List[str]]:
        out={"perp":[],"spot":[]}
        if not HAVE_AIOHTTP: return out
        timeout=aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            try:
                async with s.get("https://api.bybit.com/v5/market/instruments-info?category=linear") as r:
                    j=await r.json()
                    for it in j.get("result",{}).get("list",[]):
                        if it.get("status")!="Trading": continue
                        sym=it.get("symbol","")
                        if sym.endswith("USDT"): out["perp"].append(to_cf_perp(sym))
            except Exception: pass
            try:
                async with s.get("https://api.bybit.com/v5/market/instruments-info?category=spot") as r:
                    j=await r.json()
                    for it in j.get("result",{}).get("list",[]):
                        if it.get("status")!="Trading": continue
                        sym=it.get("symbol","")
                        if sym.endswith("USDT"): out["spot"].append(to_cf_spot(sym))
            except Exception: pass
        return out

    async def _discover_bitget(self) -> Dict[str,List[str]]:
        out={"perp":[],"spot":[]}
        if not HAVE_AIOHTTP: return out
        timeout=aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            try:
                async with s.get("https://api.bitget.com/api/mix/v1/market/contracts?productType=umcbl") as r:
                    j=await r.json()
                    for it in j.get("data",[]):
                        sym=it.get("symbol","")
                        if sym.endswith("USDT_UMCBL"):
                            out["perp"].append(to_cf_perp(sym.replace("_UMCBL","")))
            except Exception: pass
            try:
                async with s.get("https://api.bitget.com/api/spot/v1/public/symbols") as r:
                    j=await r.json()
                    for it in j.get("data",[]):
                        sym=it.get("symbol","")
                        if sym.endswith("USDT"): out["spot"].append(to_cf_spot(sym))
            except Exception: pass
        return out

    # ---- feed setup
    def _bind_maps(self, perps: List[str], spots: List[str]) -> None:
        for cf_sym in perps:  self.cf_perp_to_key[cf_sym]=norm_key(cf_sym)
        for cf_sym in spots:  self.cf_spot_to_key[cf_sym]=norm_key(cf_sym)
    def _seed_states(self, perps: List[str], spots: List[str]) -> None:
        for cf_sym in perps:
            k=self.cf_perp_to_key.get(cf_sym)
            if k: self.ensure_state(k)
        for cf_sym in spots:
            k=self.cf_spot_to_key.get(cf_sym)
            if k: self.ensure_state(k)

    def _fh_add_perp(self, fh: "FeedHandler", exchange_cls, perps: List[str], venue_name: str) -> None:
        if not exchange_cls or not perps: return
        print(f"[SUB] Perp {venue_name}: {len(perps)} symbols")
        try:
            fh.add_feed(exchange_cls(
                channels={TRADES: perps, L2_BOOK: perps, FUNDING: perps},
                callbacks={TRADES:[self.on_trade_perp], L2_BOOK:[self.on_book_perp], FUNDING:[self.on_funding]},
            ))
        except Exception:
            fh.add_feed(exchange_cls(
                channels={TRADES: perps, L2_BOOK: perps},
                callbacks={TRADES:[self.on_trade_perp], L2_BOOK:[self.on_book_perp]},
            ))

    def _fh_add_spot(self, fh: "FeedHandler", exchange_cls, spots: List[str], venue_name: str) -> None:
        if not exchange_cls or not spots: return
        print(f"[SUB] Spot {venue_name}: {len(spots)} symbols")
        fh.add_feed(exchange_cls(
            channels={TRADES: spots, L2_BOOK: spots},
            callbacks={TRADES:[self.on_trade_spot], L2_BOOK:[self.on_book_spot]},
        ))

    # ---- OI polling
    async def _oi_poll_loop(self, keys: List[str]) -> None:
        if not HAVE_AIOHTTP:
            print("[OI] aiohttp missing; OI slope disabled."); return
        timeout=aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            self.session=s
            while True:
                random.shuffle(keys)
                await self._poll_binance_oi(keys)
                await self._poll_bybit_oi(keys)
                await self._poll_bitget_oi(keys)
                await asyncio.sleep(self.oi_interval)

    async def _poll_binance_oi(self, keys: List[str]) -> None:
        host="fapi.binance.com"; base=f"https://{host}/fapi/v1/openInterest"
        for key in keys:
            await asyncio.sleep(random.uniform(*JITTER_MS)/1000.0)
            try:
                async with self.session.get(base, params={"symbol": key}) as r:
                    if r.status!=200: continue
                    j=await r.json()
                    self.ensure_state(key).record_oi(float(j.get("openInterest",0.0)))
            except Exception:
                continue

    async def _poll_bybit_oi(self, keys: List[str]) -> None:
        host="api.bybit.com"; base=f"https://{host}/v5/market/tickers"
        for key in keys:
            await asyncio.sleep(random.uniform(*JITTER_MS)/1000.0)
            try:
                async with self.session.get(base, params={"category":"linear","symbol":key}) as r:
                    if r.status!=200: continue
                    j=await r.json()
                    lst=j.get("result",{}).get("list",[])
                    if not lst: continue
                    it=lst[0]
                    val=it.get("openInterestValue") or it.get("openInterest")
                    if val is None: continue
                    self.ensure_state(key).record_oi(float(val))
            except Exception:
                continue

    async def _poll_bitget_oi(self, keys: List[str]) -> None:
        base="https://api.bitget.com/api/mix/v1/market/openInterest"
        for key in keys:
            await asyncio.sleep(random.uniform(*JITTER_MS)/1000.0)
            try:
                sym=f"{key}_UMCBL"
                async with self.session.get(base, params={"symbol": sym}) as r:
                    if r.status!=200: continue
                    j=await r.json()
                    data=j.get("data")
                    if isinstance(data,dict):
                        oi=float(data.get("amount",0.0) or data.get("openInterest",0.0))
                    elif isinstance(data,list) and data:
                        d=data[0]; oi=float(d.get("amount",0.0) or d.get("openInterest",0.0) or 0.0)
                    else:
                        oi=0.0
                    if oi>0:
                        self.ensure_state(key).record_oi(oi)
            except Exception:
                continue

    # ---- watchdog
    async def watchdog(self, full_perps: List[str], full_spots: List[str], fh: "FeedHandler") -> None:
        while True:
            await asyncio.sleep(5)
            if time.time()-self._last_any_msg_ts < WATCHDOG_SECS:
                continue
            print("[WATCHDOG] No WS messages detected. Rebuilding subscriptions with a small probe set…")
            try:
                # rebuild feeds in the SAME loop via fh.reset() + add_feed() + await fh.start()
                # not all CF versions have .reset(); we re-instantiate fh
                newfh = FeedHandler()
                probe_perps=[x for x in full_perps if any(x.startswith(p) for p in ("BTC-","ETH-","SOL-","BNB-","XRP-"))][:12]
                probe_spots=[x for x in full_spots if any(x.startswith(p) for p in ("BTC-","ETH-","SOL-","BNB-","XRP-"))][:12]
                print(f"[WATCHDOG] Probe set → Perp:{len(probe_perps)} Spot:{len(probe_spots)}")
                if CF_BinanceFutures: self._fh_add_perp(newfh, CF_BinanceFutures, probe_perps, "BinanceFutures")
                if CF_Bybit:          self._fh_add_perp(newfh, CF_Bybit,          probe_perps, "Bybit")
                if CF_Bitget:         self._fh_add_perp(newfh, CF_Bitget,         probe_perps, "Bitget")
                if CF_Binance:        self._fh_add_spot(newfh, CF_Binance,        probe_spots, "Binance")
                if CF_Bybit:          self._fh_add_spot(newfh, CF_Bybit,          probe_spots, "Bybit")
                if CF_Bitget:         self._fh_add_spot(newfh, CF_Bitget,         probe_spots, "Bitget")
                # start (non-blocking) inside current loop
                if hasattr(newfh, "start") and inspect.iscoroutinefunction(newfh.start):
                    await newfh.start()
                else:
                    # legacy fallback: run without starting a new loop
                    run=getattr(newfh, "run", None)
                    if run:
                        # run(start_loop=False) keeps current loop
                        try:
                            run(start_loop=False, install_signal_handlers=False)
                        except TypeError:
                            run()
                self._last_any_msg_ts=time.time()
            except Exception as e:
                print(f"[WATCHDOG] Rebuild error: {e}")

    # ---- run
    async def run(self, discover: List[str], manual_symbols: Optional[str], safe_default: bool):
        if not HAVE_CF:
            raise RuntimeError(f"cryptofeed import failed: {CF_IMPORT_ERR}")

        # discover or parse manual symbols
        perps: List[str] = []
        spots: List[str] = []
        if manual_symbols:
            chunks=[x for x in manual_symbols.split(";") if x.strip()]
            for ch in chunks:
                if ":" in ch:
                    _ex,listpart=ch.split(":",1)
                    for sym in listpart.split(","):
                        s=sym.strip()
                        if s.endswith("-PERP"):
                            perps.append(s)
                        else:
                            spots.append(s)
        else:
            disc=await self.discover(discover or ["binance","bybit","bitget"])
            perps,spots=disc["perp"],disc["spot"]
            print(f"[DISCOVER] Perp={len(perps)} Spot={len(spots)} (post-dedupe)")
        if safe_default or (not perps and not spots):
            for seedp in ("BTC-USDT-PERP","ETH-USDT-PERP","SOL-USDT-PERP"):
                if seedp not in perps: perps.append(seedp)
            for seeds in ("BTC-USDT","ETH-USDT","SOL-USDT"):
                if seeds not in spots: spots.append(seeds)

        self._bind_maps(perps, spots)
        self._seed_states(perps, spots)

        # Build FH and add feeds
        fh = FeedHandler()
        if CF_BinanceFutures: self._fh_add_perp(fh, CF_BinanceFutures, perps, "BinanceFutures")
        if CF_Bybit:          self._fh_add_perp(fh, CF_Bybit,          perps, "Bybit")
        if CF_Bitget:         self._fh_add_perp(fh, CF_Bitget,         perps, "Bitget")
        if CF_Binance:        self._fh_add_spot(fh, CF_Binance,        spots, "Binance")
        if CF_Bybit:          self._fh_add_spot(fh, CF_Bybit,          spots, "Bybit")
        if CF_Bitget:         self._fh_add_spot(fh, CF_Bitget,         spots, "Bitget")

        # >>> KEY CHANGE: start FH inside current loop (no new loop, no signals) <<<
        if hasattr(fh, "start") and inspect.iscoroutinefunction(fh.start):
            await fh.start()
        else:
            run=getattr(fh, "run", None)
            if run:
                try:
                    run(start_loop=False, install_signal_handlers=False)
                except TypeError:
                    run()

        # launch tasks
        asyncio.create_task(self.rank_and_print())
        asyncio.create_task(self.print_counters())
        perps_keys=sorted({norm_key(x) for x in perps})
        asyncio.create_task(self._oi_poll_loop(perps_keys))
        asyncio.create_task(self.watchdog(perps, spots, fh))

        # keep alive
        while True:
            await asyncio.sleep(3600)

# ---- CLI
def parse_args():
    ap=argparse.ArgumentParser()
    ap.add_argument("--discover", nargs="*", default=["binance","bybit","bitget"], help="Auto-discover venues")
    ap.add_argument("--symbols", type=str, default="", help="Manual symbols 'VENUE:BTC-USDT-PERP,ETH-USDT;...'")
    ap.add_argument("--safe-default", action="store_true", help="Always include BTC/ETH/SOL seeds")
    ap.add_argument("--debug", action="store_true", help="Print counters and a raw sample per channel")
    ap.add_argument("--oi-interval", type=int, default=DEFAULT_OI_INTERVAL_SEC, help="Seconds between OI snapshots")
    ap.add_argument("--max-per-venue", type=int, default=0, help="Cap symbols per venue (0=no cap)")
    return ap.parse_args()

def main():
    args=parse_args()
    scr=Screener(debug=args.debug, oi_interval_sec=args.oi_interval, max_per_venue=args.max_per_venue)
    try:
        asyncio.run(scr.run(discover=args.discover, manual_symbols=(args.symbols.strip() or None), safe_default=(args.safe_default or True)))
    except RuntimeError:
        # Thonny/Jupyter loop already running → attach into it cleanly
        loop=asyncio.get_event_loop()
        loop.create_task(scr.run(discover=args.discover, manual_symbols=(args.symbols.strip() or None), safe_default=(args.safe_default or True)))

if __name__=="__main__":
    main()
