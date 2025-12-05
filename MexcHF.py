import asyncio
import json
import math
import time
import sqlite3
import numpy as np
import aiohttp
import websockets
import random 
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Deque

# 🚨 HIGH-PERFORMANCE IMPORTS 🚨
from rich import box
from rich.console import Console
from rich.live import Live
from rich.table import Table
import uvloop 
from numba import njit 
from tdigest import TDigest 
import ccxt.async_support as ccxt 

# 🚨 PROTOC GENERATED FILE (Assumed to be compiled and present) 🚨
try:
    from PushDataV3ApiWrapper_pb2 import PushDataV3ApiWrapper
except ImportError:
    # Exit if the critical dependency is missing
    print("❌ CRITICAL: Protobuf file 'PushDataV3ApiWrapper_pb2.py' not found.")
    exit(1)

console = Console()

# ===========================
# CONFIGURATION
# ===========================

# 🚨 CRITICAL FIX: Use the V3 API endpoint and the required regional domain (.co) 🚨
MEXC_WS_URL_PB = "wss://wbs-api.mexc.co/ws" 
MEXC_REST_BASE = "https://api.mexc.com"

MICROCAP_MAX_PRICE = 2.0          
MIN_24H_QV = 50_000.0             

TRADE_DB = "mexc_trade_log.sqlite" 
WS_SHARD_SIZE = 10 

T_DIGEST_QUANTILES = 99.5 
MIN_EPS_THRESHOLD = 0.5  
RSI_LEN = 14
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

# ===========================
# UTILS / CACHE / DATABASE 
# ===========================

def safe_float(x, default: float = 0.0) -> float:
    """Safely converts input x to a float, returning default on failure."""
    try:
        if isinstance(x, str):
            return float(x)
        elif isinstance(x, (int, float)):
            return float(x)
        else:
            return default
    except Exception:
        return default

@contextmanager
def sqlite_conn(path: str):
    conn = sqlite3.connect(path)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        yield conn
    finally:
        conn.close()

def init_trade_db():
    """Initializes the local SQLite trade log database."""
    with sqlite_conn(TRADE_DB) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                symbol TEXT NOT NULL,
                entry_price REAL,
                exit_price REAL,
                position_size REAL,
                pnl_usd REAL,
                pnl_pct REAL,
                duration_seconds REAL,
                result TEXT, -- WIN / LOSS / BREAKEVEN
                signal_score REAL
            )
            """
        )
        conn.commit()

def log_trade_result(data: Dict):
    """Logs the completed trade result to the local SQLite database."""
    result_flag = "WIN" if data['pnl_pct'] > 0.01 else ("LOSS" if data['pnl_pct'] < -0.01 else "BREAKEVEN")
    with sqlite_conn(TRADE_DB) as conn:
        conn.execute(
            """
            INSERT INTO trade_log (timestamp, symbol, entry_price, exit_price, position_size, 
                                   pnl_usd, pnl_pct, duration_seconds, result, signal_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                datetime.now().isoformat(), data['symbol'], data['entry_price'], data['exit_price'],
                data['position_size'], data['pnl_usd'], data['pnl_pct'], data['duration_seconds'],
                result_flag, data['signal_score']
            )
        )
        conn.commit()

# ===========================
# JIT ACCELERATED INDICATORS (NUMBA)
# ===========================

@njit(cache=True)
def _calculate_ema_numba(data, span):
    if len(data) == 0:
        return np.array([0.0])
    
    alpha = 2.0 / (span + 1.0)
    ema = np.empty_like(data)
    ema[0] = data[0]
    
    for i in range(1, len(data)):
        ema[i] = alpha * data[i] + (1 - alpha) * ema[i - 1]
    return ema

@njit(cache=True)
def compute_rsi_numba(closes, length=RSI_LEN):
    if len(closes) < length + 1:
        return 50.0

    diff = np.diff(closes)
    gains = np.where(diff > 0, diff, 0.0)
    losses = np.where(diff < 0, -diff, 0.0)

    avg_gain = _calculate_ema_numba(gains, length)
    avg_loss = _calculate_ema_numba(losses, length)
    
    with np.errstate(divide='ignore', invalid='ignore'):
        rs = avg_gain / avg_loss
    
    rsi = 100.0 - (100.0 / (1.0 + rs))
    
    return rsi[-1] if len(rsi) > 0 and not np.isnan(rsi[-1]) else 50.0

@njit(cache=True)
def compute_macd_hist_numba(closes):
    if len(closes) < MACD_SLOW:
        return 0.0

    ema_fast = _calculate_ema_numba(closes, MACD_FAST)
    ema_slow = _calculate_ema_numba(closes, MACD_SLOW)
    
    macd_line = ema_fast[len(ema_slow)-1:] - ema_slow[len(ema_slow)-1:]
    
    if len(macd_line) < MACD_SIGNAL:
        return 0.0
        
    signal_line = _calculate_ema_numba(macd_line, MACD_SIGNAL)
    
    histogram = macd_line[len(signal_line)-1:] - signal_line[len(signal_line)-1:]
    
    return histogram[-1] if len(histogram) > 0 else 0.0

# ===========================
# DYNAMIC SCORING ENGINE
# ===========================

class TDigestEstimator:
    def __init__(self, compression=100):
        self.digest = TDigest(delta=compression) 
        
    def update(self, score: float):
        if score > 0.0:
            self.digest.update(score)
        
    def get_quantile(self, percentile: float) -> float:
        if self.digest.count() < 100:
            return MIN_EPS_THRESHOLD
        return self.digest.percentile(percentile) 

@dataclass
class SymbolState:
    symbol: str
    
    candles_1m_data: Dict[str, deque] = field(default_factory=lambda: {
        'O': deque(maxlen=300), 'H': deque(maxlen=300), 'L': deque(maxlen=300),
        'C': deque(maxlen=300), 'V': deque(maxlen=300), 'Delta': deque(maxlen=300)
    })
    
    current_1m_bar: Optional[Dict] = None
    
    pump_score_digest: TDigestEstimator = field(default_factory=TDigestEstimator)
    dynamic_eps_threshold: float = 0.0

    last_price: float = 0.0
    cvd: float = 0.0
    early_pump_score: float = 0.0
    top_risk_score: float = 0.0
    
    trade_active: bool = False
    entry_data: Optional[Dict] = None
    
    # --- Client-Side 1M Bar Building ---

    def start_new_1m_bar(self, minute_start: int, price: float):
        self.current_1m_bar = {
            'minute_start': minute_start, 'O': price, 'H': price, 'L': price, 
            'C': price, 'Volume': 0.0, 'Delta': 0.0
        }

    def finalize_1m_bar(self):
        if self.current_1m_bar and self.current_1m_bar['Volume'] > 0:
            
            self.candles_1m_data['O'].append(self.current_1m_bar['O'])
            self.candles_1m_data['H'].append(self.current_1m_bar['H'])
            self.candles_1m_data['L'].append(self.current_1m_bar['L'])
            self.candles_1m_data['C'].append(self.current_1m_bar['C'])
            self.candles_1m_data['V'].append(self.current_1m_bar['Volume'])
            self.candles_1m_data['Delta'].append(self.current_1m_bar['Delta'])
            
            self.dynamic_eps_threshold = self.pump_score_digest.get_quantile(T_DIGEST_QUANTILES)
            self.dynamic_eps_threshold = max(MIN_EPS_THRESHOLD, self.dynamic_eps_threshold)

        self.current_1m_bar = None
        
    def update_with_trade(self, deal):
        price = safe_float(deal.price)
        qty = safe_float(deal.quantity)
        
        now = time.time()
        minute_start = int(now // 60) * 60

        if self.current_1m_bar is None or self.current_1m_bar['minute_start'] != minute_start:
            self.finalize_1m_bar()
            self.start_new_1m_bar(minute_start, price)
        
        bar = self.current_1m_bar
        bar['H'] = max(bar['H'], price)
        bar['L'] = min(bar['L'], price)
        bar['C'] = price
        delta_qty = qty * (1 if deal.tradeType == 1 else -1)
        bar['Volume'] += qty
        bar['Delta'] += delta_qty
        self.cvd += delta_qty
        self.last_price = price
        
        self.update_micro_scores() 
        
    def check_negative_delta_divergence(self) -> bool:
        if len(self.candles_1m_data['C']) < 10 or not self.current_1m_bar: return False

        closes = np.array(self.candles_1m_data['C'])
        current_price = self.current_1m_bar['C']
        
        price_hh = current_price > np.max(closes[-5:]) 
        
        recent_deltas = np.array(list(self.candles_1m_data['Delta'])[-5:])
        avg_recent_delta = np.mean(recent_deltas)
        delta_failing = self.current_1m_bar['Delta'] < avg_recent_delta * 0.5 
        
        current_high = self.current_1m_bar['H']
        current_low = self.current_1m_bar['L']
        price_range = current_high - current_low
        rejection = (current_high - current_price) / price_range > 0.3 if price_range > 0 else False

        return price_hh and delta_failing and rejection
        
    def update_micro_scores(self):
        
        closes_array = np.array(self.candles_1m_data['C'])
        if len(closes_array) > 60:
            current_rsi = compute_rsi_numba(closes_array[-60:], length=14)
        
        self.early_pump_score = 1.0 # Placeholder
        
        if self.early_pump_score >= self.dynamic_eps_threshold:
             self.early_pump_score += 5 

        if self.check_negative_delta_divergence():
             self.top_risk_score += 1.5 
             
        self.pump_score_digest.update(self.early_pump_score) 
        
        # 🚨 Trade Management/Logging 🚨
        if self.early_pump_score > 5.0 and not self.trade_active:
            self.trade_active = True
            self.entry_data = {'entry_price': self.last_price, 'entry_time': time.time()}
            console.print(f"[TRADE] ENTRY: {self.symbol} @ {self.last_price}")
        
        if self.trade_active and self.top_risk_score > 1.5:
            pnl_pct = (self.last_price / self.entry_data['entry_price'] - 1.0) * 100
            duration = time.time() - self.entry_data['entry_time']
            log_trade_result({
                'symbol': self.symbol,
                'entry_price': self.entry_data['entry_price'],
                'exit_price': self.last_price,
                'position_size': 1000.0, 
                'pnl_usd': 1000 * pnl_pct / 100,
                'pnl_pct': pnl_pct,
                'duration_seconds': duration,
                'signal_score': self.top_risk_score
            })
            self.trade_active = False
            console.print(f"[TRADE] EXIT: {self.symbol} PnL: {pnl_pct:.2f}%")
        
# [The WebsocketShard class remains here]

class WebsocketShard:
    def __init__(self, shard_id, symbols, state_ref, queue):
        self.id = shard_id
        self.symbols = symbols
        self.state_ref = state_ref
        self.queue = queue
        self.ws = None

    async def connect(self):
        base_symbols = [s.replace('/USDT', '') for s in self.symbols]
        topics = [f"spot@public.aggre.deals.v3.api.pb@{sym}USDT" for sym in base_symbols]
        
        sub_msg = {"method": "SUBSCRIPTION", "params": topics}
        
        while True:
            try:
                async with websockets.connect(MEXC_WS_URL_PB) as ws:
                    self.ws = ws
                    await ws.send(json.dumps(sub_msg))
                    
                    last_ping = time.time()
                    while True:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=20)
                            if isinstance(msg, bytes):
                                await self.queue.put(msg)
                            elif msg == '{"method":"PONG"}':
                                last_ping = time.time()
                            
                            if time.time() - last_ping > 15:
                                await ws.send(json.dumps({"method": "PING"}))
                                last_ping = time.time()
                        except asyncio.TimeoutError:
                            await ws.send(json.dumps({"method": "PING"}))
            except Exception:
                await asyncio.sleep(5)

class MexcEngineHF:
    def __init__(self, symbols, mexc_qv_map):
        self.symbols = symbols
        self.mexc_qv_map = mexc_qv_map
        self.queue = asyncio.Queue()
        self.states = {s: SymbolState(symbol=s) for s in symbols}
        self.shards = []
        self._stop = asyncio.Event()

    def _deserialize_protobuf_message(self, raw_bytes: bytes):
        try:
            wrapper = PushDataV3ApiWrapper() 
            wrapper.ParseFromString(raw_bytes) 
            
            if wrapper.HasField("publicAggreDeals"):
                return {
                    'channel': wrapper.channel,
                    'symbol': wrapper.symbol,
                    'deals': wrapper.publicAggreDeals.deals 
                }
        except Exception:
            return None

    async def discover_mexc_microcaps_and_bootstrap(self):
        url = f"{MEXC_REST_BASE}/api/v3/ticker/24hr"
        
        async with aiohttp.ClientSession() as session:
            
            records = []
            for attempt in range(5):
                try:
                    # 🚨 IP BAN SAFETY: Random Delays
                    delay = random.uniform(0.05, 0.20)
                    await asyncio.sleep(delay)
                    
                    async with session.get(url, timeout=15) as resp:
                        # 🚨 IP BAN SAFETY: Error Monitoring
                        if resp.status == 429:
                            retry_after = resp.headers.get('Retry-After', '5')
                            console.print(f"[RATE-LIMIT] 429 received. Sleeping for {retry_after}s.")
                            await asyncio.sleep(safe_float(retry_after, 5.0) + 1)
                            continue
                        elif resp.status == 403:
                            console.print("[BAN-RISK] 403 Forbidden received. Halting.")
                            raise Exception("403 Forbidden - IP Ban or Auth Failure")
                        
                        resp.raise_for_status() 
                        data = await resp.json()
                        
                        for t in data:
                            sym = t.get('symbol')
                            vol = safe_float(t.get('quoteVolume'))
                            price = safe_float(t.get('lastPrice'))
                            
                            if sym and "USDT" in sym and vol >= MIN_24H_QV and price <= MICROCAP_MAX_PRICE:
                                records.append(sym)

                        break 
                except aiohttp.ClientConnectorError as e:
                    console.print(f"[CONNECTION] Attempt {attempt+1} failed: {e}. Retrying.")
                    await asyncio.sleep(2 ** attempt)
                except Exception as e:
                    console.print(f"[ERROR] Failed REST request: {e}")
                    raise
            else:
                raise Exception("Failed to fetch market data after multiple retries.")
            
            self.symbols = [s.replace('USDT', '/USDT') for s in records] 
            self.states = {s: SymbolState(symbol=s) for s in self.symbols}
            
            chunks = [self.symbols[i:i + WS_SHARD_SIZE] 
                      for i in range(0, len(self.symbols), WS_SHARD_SIZE)]
            
            console.print(f"[SHARD] Spawning {len(chunks)} Protobuf Shards for {len(self.symbols)} Coins.")

            for i, chunk in enumerate(chunks):
                client = WebsocketShard(i, chunk, self.states, self.queue)
                self.shards.append(client)
                asyncio.create_task(client.connect())
                await asyncio.sleep(0.5) 

    async def processor(self):
        while not self._stop.is_set():
            try:
                raw_bytes = await self.queue.get() 
                msg = self._deserialize_protobuf_message(raw_bytes)
                
                if msg and 'deals' in msg:
                    symbol = msg['symbol'].replace('USDT', '/USDT')
                    
                    if symbol in self.states:
                        state = self.states[symbol]
                        
                        for deal in msg['deals']:
                            state.update_with_trade(deal) 
                            
                self.queue.task_done()
            except Exception:
                pass 

    async def dashboard_loop(self):
        # NOTE: This would display the Rich TUI table
        while not self._stop.is_set():
            await asyncio.sleep(0.2) 

    def stop(self):
        self._stop.set()
        for shard in self.shards:
            if shard.ws:
                asyncio.create_task(shard.ws.close())


# ===========================
# MAIN EXECUTION
# ===========================

async def main():
    console.print("[INIT] Starting High-Frequency MEXC Pump Hunter...")
    
    # 0. Initialize local logging DB
    init_trade_db()
    
    # 1. Initialize Engine 
    engine = MexcEngineHF(symbols=[], mexc_qv_map={})
    
    try:
        # 2. Discover Symbols and Launch Shards
        await engine.discover_mexc_microcaps_and_bootstrap()
        
        console.print(f"[INIT] Tracking {len(engine.symbols)} symbols. Launching HF tasks...")

        # 3. Launch Core Tasks
        proc_task = asyncio.create_task(engine.processor())
        dash_task = asyncio.create_task(engine.dashboard_loop())
        
        await asyncio.gather(proc_task, dash_task)

    except KeyboardInterrupt:
        console.print("\n[STOP] Shutting down engine...")
    except Exception as e:
        console.print(f"\n❌ CRITICAL CRASH: Script terminated unexpectedly: {e}")
    finally:
        engine.stop()
        console.print("[INIT] Engine architecture fully stopped.")


if __name__ == "__main__":
    try:
        uvloop.install() 
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting on user interrupt.")
    except Exception as e:
        print(f"\n❌ FINAL CRASH: An unhandled error occurred during startup: {e}")