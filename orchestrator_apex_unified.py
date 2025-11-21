import asyncio
import logging
import datetime
import sqlite3
import math
import json
from collections import deque, defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any

# -----------------------------------------------------------------------------
# 1. HIGH-PERFORMANCE LIBRARIES
# -----------------------------------------------------------------------------
try:
    import uvloop
    uvloop.install()
except ImportError:
    pass

try:
    import orjson
    def json_loads(s): return orjson.loads(s)
    def json_dumps(obj): return orjson.dumps(obj).decode('utf-8')
except ImportError:
    import json
    def json_loads(s): return json.loads(s)
    def json_dumps(obj): return json.dumps(obj)

import aiohttp
import websockets
import numpy as np
from rich.live import Live
from rich.table import Table
from rich.layout import Layout
from rich.panel import Panel
from rich import box
from rich.console import Console

# --- MEXC PROTOBUF IMPORT ---
try:
    import PushDataV3ApiWrapper_pb2
    MEXC_PROTO_AVAILABLE = True
except ImportError:
    print("⚠️ MEXC Proto files not found. Running in Bybit-Only mode.")
    MEXC_PROTO_AVAILABLE = False

# -----------------------------------------------------------------------------
# 2. CONFIGURATION
# -----------------------------------------------------------------------------
DB_NAME = "apex_ultimate.sqlite"

# API Endpoints
BYBIT_WS_LINEAR = "wss://stream.bybit.com/v5/public/linear"
BYBIT_HTTP_TICKERS = "https://api.bybit.com/v5/market/tickers?category=linear"
MEXC_WS_SPOT = "wss://wbs-api.mexc.com/ws"

# Strategy Thresholds
MIN_CONFIDENCE = 50.0
WHALE_THRESHOLD = 50000
LIQ_MAGNET_DIST = 0.02

logging.basicConfig(level=logging.ERROR, format="%(asctime)s [%(levelname)s] %(message)s")

# -----------------------------------------------------------------------------
# 3. DATA MODELS
# -----------------------------------------------------------------------------
@dataclass
class OrderBook:
    """Minimalist book for OFI calculation"""
    best_bid: float = 0.0
    best_ask: float = 0.0
    best_bid_qty: float = 0.0
    best_ask_qty: float = 0.0
    # Store top levels for Magnetic TP logic
    bids: Dict[float, float] = field(default_factory=dict)
    asks: Dict[float, float] = field(default_factory=dict)

@dataclass
class MarketState:
    symbol: str
    
    # Price & Vol (Fused)
    price: float = 0.0
    price_history: deque = field(default_factory=lambda: deque(maxlen=200))
    volatility_atr: float = 0.0
    
    # OFI & Depth
    ofi_cumulative: float = 0.0
    ofi_history: deque = field(default_factory=lambda: deque(maxlen=50))
    orderbook: OrderBook = field(default_factory=OrderBook)

    # CVD (Truth)
    cvd_cumulative: float = 0.0
    cvd_history: deque = field(default_factory=lambda: deque(maxlen=200))
    
    # Magnets
    liq_clusters: Dict[float, float] = field(default_factory=lambda: defaultdict(float))
    
    # State
    active_tags: List[str] = field(default_factory=list)
    confidence: float = 0.0
    trade_active: bool = False
    last_update: float = 0.0
    source_feed: str = "WAITING"

    def update_price(self, p: float, source: str = "BYBIT"):
        if p <= 0: return
        self.price = p
        self.price_history.append(p)
        self.source_feed = source
        self.last_update = datetime.datetime.now().timestamp()
        if len(self.price_history) > 10:
            self.volatility_atr = np.std(list(self.price_history))

    def update_cvd(self, side: str, size_usd: float):
        delta = size_usd if side == "Buy" else -size_usd
        self.cvd_cumulative += delta
        self.cvd_history.append(self.cvd_cumulative)

# Global State
universe: Dict[str, MarketState] = {}
active_trades: List[Dict] = []

# -----------------------------------------------------------------------------
# 4. SMART LOGIC (Structure, Divs, Magnets)
# -----------------------------------------------------------------------------
def calculate_ofi(st: MarketState, new_bid: float, new_bid_q: float, new_ask: float, new_ask_q: float):
    ob = st.orderbook
    ofi_delta = 0.0
    if new_bid > ob.best_bid: ofi_delta += new_bid_q
    elif new_bid == ob.best_bid: ofi_delta += (new_bid_q - ob.best_bid_qty)
    else: ofi_delta -= ob.best_bid_qty

    if new_ask < ob.best_ask: ofi_delta -= new_ask_q
    elif new_ask == ob.best_ask: ofi_delta -= (new_ask_q - ob.best_ask_qty)
    else: ofi_delta += ob.best_ask_qty

    st.ofi_cumulative += ofi_delta
    st.ofi_history.append(st.ofi_cumulative)
    ob.best_bid, ob.best_bid_qty = new_bid, new_bid_q
    ob.best_ask, ob.best_ask_qty = new_ask, new_ask_q

def find_last_higher_low(prices: list, window: int = 5) -> float:
    """Finds the CHOCH level (Swing Low) to use as Smart SL"""
    if len(prices) < window * 2 + 1: return prices[-1] * 0.99
    np_prices = np.array(prices)
    for i in range(len(np_prices) - window - 1, window, -1):
        current = np_prices[i]
        if current < np.min(np_prices[i-window:i]) and current < np.min(np_prices[i+1:i+window+1]):
            return float(current)
    return prices[-1] * 0.99

def check_cvd_divergence(prices: list, cvd: list, lookback: int = 30) -> bool:
    """Returns True if Price is High/Flat but CVD is Dumping"""
    if len(prices) < lookback or len(cvd) < lookback: return False
    price_chg = (prices[-1] - prices[-lookback]) / prices[-lookback]
    cvd_chg = cvd[-1] - cvd[-lookback]
    return price_chg > -0.001 and cvd_chg < 0

def adjust_tp_to_magnet(target_price: float, entry: float, side: str, st: MarketState) -> float:
    """
    MAGNETIC TP: Scans for Walls or Liquidation Clusters BEFORE the target.
    If found, moves TP to just in front of them.
    """
    best_target = target_price
    wall_threshold = 50000 
    
    # 1. Check Order Book Walls
    if side == "LONG":
        for p, qty in st.orderbook.asks.items():
            if p > entry and p < target_price and (p * qty) > wall_threshold:
                best_target = min(best_target, p * 0.999) # Front-run
    elif side == "SHORT":
        for p, qty in st.orderbook.bids.items():
            if p < entry and p > target_price and (p * qty) > wall_threshold:
                best_target = max(best_target, p * 1.001)

    # 2. Check Liquidation Clusters
    closest_liq_price = 0.0
    closest_dist = float('inf')
    
    for price, vol in st.liq_clusters.items():
        if vol > wall_threshold:
            dist = abs(price - target_price)
            # If cluster is close to our TP (within 10% of trade range)
            if dist < abs(target_price - entry) * 0.10: 
                if dist < closest_dist:
                    closest_dist = dist
                    closest_liq_price = price
    
    if closest_liq_price > 0:
        if side == "LONG": best_target = min(best_target, closest_liq_price * 0.998)
        else: best_target = max(best_target, closest_liq_price * 1.002)

    return best_target

def analyze_market(st: MarketState):
    if len(st.price_history) < 20: return
    score, tags = 0, []

    # OFI Velocity
    if len(st.ofi_history) > 5:
        ofi_slope = st.ofi_history[-1] - st.ofi_history[-5]
        if ofi_slope > 10000: score += 30; tags.append("🌊OFI_Surge")
        elif ofi_slope < -10000: score -= 30; tags.append("🌊OFI_Dump")

    # CVD Trend
    if st.cvd_cumulative > 0 and "🌊OFI_Surge" in tags: score += 10; tags.append("✅CVD_Confirm")
    elif st.cvd_cumulative < 0 and "🌊OFI_Dump" in tags: score -= 10; tags.append("✅CVD_Confirm")

    # Magnets
    magnet_strength = 0
    for price, vol in st.liq_clusters.items():
        dist_pct = abs(price - st.price) / st.price
        if dist_pct < LIQ_MAGNET_DIST and vol > WHALE_THRESHOLD:
            magnet_strength += 10
            tags.append("🧲Magnet")
            break 
    
    if score > 0: score += magnet_strength
    elif score < 0: score -= magnet_strength

    st.confidence = abs(score)
    st.active_tags = tags

    if st.confidence >= MIN_CONFIDENCE and not st.trade_active:
        trigger_trade(st, "LONG" if score > 0 else "SHORT")

def trigger_trade(st: MarketState, side: str):
    st.trade_active = True
    entry = st.price
    vol = st.volatility_atr if st.volatility_atr > 0 else entry * 0.005
    
    smart_level = None
    
    if side == "LONG":
        hard_sl = entry - (vol * 2.5)
        raw_tp1 = entry + (vol * 1.5)
        raw_tp2 = entry + (vol * 3.0)
        raw_tp3 = entry + (vol * 6.0)
        
        # Smart SL (CHOCH)
        structure_low = find_last_higher_low(list(st.price_history))
        if structure_low > hard_sl and structure_low < entry:
            smart_level = structure_low
    else:
        hard_sl = entry + (vol * 2.5)
        raw_tp1 = entry - (vol * 1.5)
        raw_tp2 = entry - (vol * 3.0)
        raw_tp3 = entry - (vol * 6.0)
        # (Short logic omitted for brevity)

    # Apply Magnetic TP Logic
    tp1 = adjust_tp_to_magnet(raw_tp1, entry, side, st)
    tp2 = adjust_tp_to_magnet(raw_tp2, entry, side, st)
    tp3 = adjust_tp_to_magnet(raw_tp3, entry, side, st)

    active_trades.append({
        "time": datetime.datetime.now().strftime("%H:%M:%S"),
        "symbol": st.symbol,
        "side": side,
        "entry": entry,
        "sl": hard_sl,
        "smart_level": smart_level,
        "tp1": tp1, "tp2": tp2, "tp3": tp3,
        "conf": st.confidence
    })

# -----------------------------------------------------------------------------
# 5. CONNECTORS (BYBIT + MEXC POOL)
# -----------------------------------------------------------------------------
async def fetch_symbols() -> List[str]:
    """Fetches Top 150 USDT pairs sorted by Volume from Bybit."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(BYBIT_HTTP_TICKERS) as resp:
                data = await resp.json()
                usdt = [t for t in data['result']['list'] if t['symbol'].endswith('USDT')]
                sorted_vol = sorted(usdt, key=lambda x: float(x.get('turnover24h', 0)), reverse=True)
                return [t['symbol'] for t in sorted_vol[:150]]
    except Exception:
        return ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

async def bybit_stream(symbols):
    async with websockets.connect(BYBIT_WS_LINEAR) as ws:
        for i in range(0, len(symbols), 10):
            batch = symbols[i:i+10]
            args = [f"orderbook.50.{s}" for s in batch] + \
                   [f"publicTrade.{s}" for s in batch] + \
                   [f"liquidation.{s}" for s in batch]
            await ws.send(json_dumps({"op": "subscribe", "args": args}))
            await asyncio.sleep(0.1)

        while True:
            try:
                msg = await ws.recv()
                data = json_loads(msg)
                if "topic" not in data: continue
                topic, payload = data["topic"], data["data"]

                if "orderbook" in topic:
                    sym = topic.split(".")[2]
                    if sym not in universe: universe[sym] = MarketState(symbol=sym)
                    st = universe[sym]
                    # Store full book for Magnet Logic
                    for x in payload.get("b", []): st.orderbook.bids[float(x[0])] = float(x[1])
                    for x in payload.get("a", []): st.orderbook.asks[float(x[0])] = float(x[1])
                    
                    # OFI Calc (Best Bid/Ask)
                    if payload.get("b") and payload.get("a"):
                        calculate_ofi(st, float(payload["b"][0][0]), float(payload["b"][0][1]), 
                                      float(payload["a"][0][0]), float(payload["a"][0][1]))
                        analyze_market(st)

                elif "publicTrade" in topic:
                    sym = topic.split(".")[1]
                    for t in payload:
                        universe[sym].update_price(float(t["p"]), source="BYBIT")
                        universe[sym].update_cvd(t["S"], float(t["v"]) * float(t["p"]))

                elif "liquidation" in topic:
                    sym = topic.split(".")[1]
                    universe[sym].liq_clusters[round(float(payload["price"]), -1)] += float(payload["size"]) * float(payload["price"])

            except Exception: continue

class MexcConnector:
    def __init__(self, symbols):
        self.chunks = [symbols[i:i + 30] for i in range(0, len(symbols), 30)]
    
    async def start(self):
        if not MEXC_PROTO_AVAILABLE: return
        await asyncio.gather(*[self._connect_chunk(c) for c in self.chunks])

    async def _connect_chunk(self, symbols):
        async with websockets.connect(MEXC_WS_SPOT) as ws:
            params = []
            for s in symbols:
                params.append(f"spot@public.aggre.deals.v3.api.pb@100ms@{s}")
                params.append(f"spot@public.aggre.depth.v3.api.pb@100ms@{s}")
            await ws.send(json.dumps({"method": "SUBSCRIPTION", "params": params}))
            asyncio.create_task(self._keep_alive(ws))

            while True:
                try:
                    msg = await ws.recv()
                    if not isinstance(msg, bytes): continue
                    wrapper = PushDataV3ApiWrapper_pb2.PushDataV3ApiWrapper()
                    wrapper.ParseFromString(msg)
                    symbol = wrapper.symbol
                    if symbol not in universe: universe[symbol] = MarketState(symbol=symbol)
                    st = universe[symbol]

                    if wrapper.HasField("publicAggreDeals"):
                        for t in getattr(wrapper.publicAggreDeals, "deals", []):
                            side = "Buy" if t.tradeType == 1 else "Sell"
                            st.update_price(float(t.price), source="MEXC")
                            st.update_cvd(side, float(t.price) * float(t.quantity))

                    elif wrapper.HasField("publicAggreDepths"):
                        d = wrapper.publicAggreDepths
                        if d.asks and d.bids:
                            calculate_ofi(st, float(d.bids[0].price), float(d.bids[0].quantity), 
                                          float(d.asks[0].price), float(d.asks[0].quantity))
                            analyze_market(st)
                except Exception: break

    async def _keep_alive(self, ws):
        while True:
            await asyncio.sleep(15)
            try: await ws.send(json.dumps({"method": "PING"}))
            except: break

# -----------------------------------------------------------------------------
# 6. MANAGER & UI
# -----------------------------------------------------------------------------
def init_db():
    conn = sqlite3.connect(DB_NAME)
    conn.execute("CREATE TABLE IF NOT EXISTS trade_history (id INTEGER PRIMARY KEY, timestamp TEXT, symbol TEXT, side TEXT, entry REAL, exit_price REAL, outcome TEXT, pnl_percent REAL)")
    conn.commit(); conn.close()

def log_trade_result(trade, exit_price, outcome):
    try:
        pnl = (exit_price - trade['entry']) / trade['entry'] * 100
        if trade['side'] == "SHORT": pnl = -pnl
        conn = sqlite3.connect(DB_NAME)
        conn.execute("INSERT INTO trade_history (timestamp, symbol, side, entry, exit_price, outcome, pnl_percent) VALUES (?,?,?,?,?,?,?)", 
                     (datetime.datetime.now().isoformat(), trade['symbol'], trade['side'], trade['entry'], exit_price, outcome, pnl))
        conn.commit(); conn.close()
    except: pass

async def trade_manager_loop():
    while True:
        await asyncio.sleep(1)
        for t in active_trades[:]:
            if t['symbol'] not in universe: continue
            st = universe[t['symbol']]
            curr = st.price
            if curr == 0: continue
            
            outcome = None
            if t['side'] == "LONG":
                if curr <= t['sl']: outcome = "SL_HIT 🛑"
                elif curr >= t['tp3']: outcome = "TP3 🚀"
                elif curr >= t['tp2']: outcome = "TP2 💰"
                elif curr >= t['tp1']: outcome = "TP1 ✅"
                # Smart Structure Break check
                elif t['smart_level'] and curr < t['smart_level']:
                    if check_cvd_divergence(list(st.price_history), list(st.cvd_history)): outcome = "SMART 🧠"
            else:
                if curr >= t['sl']: outcome = "SL_HIT 🛑"
                elif curr <= t['tp3']: outcome = "TP3 🚀"
                elif curr <= t['tp2']: outcome = "TP2 💰"
                elif curr <= t['tp1']: outcome = "TP1 ✅"

            if outcome:
                log_trade_result(t, curr, outcome)
                active_trades.remove(t)
                universe[t['symbol']].trade_active = False

def generate_dashboard() -> Layout:
    layout = Layout()
    layout.split(Layout(name="header", size=3), Layout(name="main", ratio=1), Layout(name="footer", size=12))
    layout["header"].update(Panel("🦅 APEX ULTIMATE: UNIFIED DATA + SMART RISK + MAGNETS", style="bold white on blue"))

    table = Table(expand=True, box=box.SIMPLE_HEAD)
    table.add_column("Sym", style="cyan"); table.add_column("Price"); table.add_column("Src", style="yellow"); table.add_column("🌊 OFI"); table.add_column("💰 CVD"); table.add_column("Tags"); table.add_column("Conf", style="bold")

    for st in sorted(universe.values(), key=lambda x: x.confidence, reverse=True)[:20]:
        ofi_c = "green" if st.ofi_cumulative > 0 else "red"
        conf_c = "green" if st.confidence >= 50 else "dim"
        table.add_row(st.symbol, f"{st.price:.4f}", st.source_feed, f"[{ofi_c}]{st.ofi_cumulative:.0f}[/]", 
                      f"{st.cvd_cumulative/1000:.0f}k", ", ".join(st.active_tags), f"[{conf_c}]{st.confidence:.0f}[/]")

    layout["main"].update(Panel(table, title="Live Market Scanner (Top 150 Vol)"))

    t_table = Table(expand=True, box=box.SIMPLE)
    t_table.add_column("Time", style="dim"); t_table.add_column("Sym", style="bold white"); t_table.add_column("Side"); t_table.add_column("Entry"); t_table.add_column("🧠 Smart"); t_table.add_column("🛑 Hard"); t_table.add_column("TP1"); t_table.add_column("TP2"); t_table.add_column("TP3")
    
    for t in active_trades[-8:]:
        c = "green" if t['side'] == "LONG" else "red"
        smart_txt = f"{t['smart_level']:.4f}" if t.get('smart_level') else "-"
        t_table.add_row(t['time'], t['symbol'], f"[{c}]{t['side']}[/]", f"{t['entry']:.4f}", smart_txt, f"{t['sl']:.4f}", f"{t['tp1']:.4f}", f"{t['tp2']:.4f}", f"{t['tp3']:.4f}")
        
    layout["footer"].update(Panel(t_table, title="Active Trades (With Magnetic Targets)"))
    return layout

async def ui_loop(live):
    while True:
        live.update(generate_dashboard())
        await asyncio.sleep(0.2)

async def main():
    init_db()
    console = Console()
    console.print("[yellow]Initializing Apex Ultimate Engine...[/]")
    symbols = await fetch_symbols()
    for s in symbols: universe[s] = MarketState(symbol=s)
    mexc_pool = MexcConnector(symbols)

    with Live(generate_dashboard(), refresh_per_second=4, screen=True) as live:
        await asyncio.gather(bybit_stream(symbols), mexc_pool.start(), trade_manager_loop(), ui_loop(live))

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: pass