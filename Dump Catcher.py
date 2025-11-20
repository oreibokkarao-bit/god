import asyncio
import aiohttp
import orjson
import time
import logging
import aiosqlite
import math
from collections import deque, defaultdict
from dataclasses import dataclass, field
from typing import Dict, Set, List, Optional
from datetime import datetime

from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.text import Text
from rich import box

# --- CONFIGURATION ---
BYBIT_WS_LINEAR = "wss://stream.bybit.com/v5/public/linear"
BYBIT_WS_SPOT   = "wss://stream.bybit.com/v5/public/spot"
BINANCE_WS_FUT  = "wss://fstream.binance.com/ws/!miniTicker@arr"
BINANCE_REST    = "https://fapi.binance.com"
DB_FILE         = "dump_catcher.db"

# FILTER SETTINGS
MIN_24H_VOL     = 1_000_000   # $1M Min Volume
PUMP_THRESHOLD  = 3.0         # > 3% Pump
CVD_WINDOW_SEC  = 900         # 15-min Flow Window
SPOT_CVD_LIMIT  = -25_000     # Trigger: Spot Sell > 25k
PERP_CVD_LIMIT  = 25_000      # Trigger: Perp Buy > 25k

# RISK SETTINGS (The Arsenal)
ATR_PERIOD      = 14          
ATR_SL_MULT     = 2.0         # SL = Entry + 2ATR
TIME_STOP_SEC   = 3600        # Hard close after 60 mins

console = Console()
logging.basicConfig(level=logging.WARNING)

# --- DATA STRUCTURES ---

@dataclass
class FlowState:
    symbol: str
    perp_cvd: float = 0.0
    spot_cvd: float = 0.0
    oi: float = 0.0
    funding: float = 0.0
    perp_hist: deque = field(default_factory=deque)
    spot_hist: deque = field(default_factory=deque)

@dataclass
class MarketState:
    symbol: str
    price: float = 0.0
    price_low: float = 0.0
    pump_24h: float = 0.0
    volume_24h: float = 0.0

@dataclass
class ActiveTrade:
    symbol: str
    entry_price: float
    entry_time: float
    sl_price: float
    tp1: float
    tp2: float
    tp3: float
    atr: float
    reason: str

# --- DATABASE LAYER ---

class TradeDB:
    def __init__(self, db_path: str):
        self.db_path = db_path

    async def init(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS trade_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    symbol TEXT,
                    side TEXT,
                    entry_price REAL,
                    exit_price REAL,
                    pnl_pct REAL,
                    outcome TEXT,
                    reason TEXT
                )
            """)
            await db.commit()

    async def log_trade(self, t: ActiveTrade, exit_price: float, outcome: str):
        pnl_pct = (t.entry_price - exit_price) / t.entry_price * 100
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT INTO trade_history 
                (timestamp, symbol, side, entry_price, exit_price, pnl_pct, outcome, reason)
                VALUES (?, ?, 'SHORT', ?, ?, ?, ?, ?)
            """, (ts, t.symbol, t.entry_price, exit_price, pnl_pct, outcome, t.reason))
            await db.commit()

    async def get_history(self, limit=10):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(f"SELECT * FROM trade_history ORDER BY id DESC LIMIT {limit}") as cursor:
                return await cursor.fetchall()

# --- UTILS: ATR CALCULATION ---

async def fetch_atr(symbol: str, interval="1h", limit=15) -> float:
    try:
        async with aiohttp.ClientSession() as session:
            url = f"{BINANCE_REST}/fapi/v1/klines"
            params = {"symbol": symbol, "interval": interval, "limit": limit}
            async with session.get(url, params=params) as resp:
                data = await resp.json()
                if not data or len(data) < 2: return 0.0
                trs = []
                for i in range(1, len(data)):
                    high = float(data[i][2])
                    low  = float(data[i][3])
                    close_prev = float(data[i-1][4])
                    tr = max(high - low, abs(high - close_prev), abs(low - close_prev))
                    trs.append(tr)
                if not trs: return 0.0
                return sum(trs) / len(trs)
    except Exception:
        return 0.0

# --- CORE ENGINES ---

class FlowEngine:
    def __init__(self):
        self.targets: Set[str] = set()
        self.flow_data: Dict[str, FlowState] = defaultdict(lambda: FlowState("Unknown"))
        self.sub_queue = asyncio.Queue()
        self._lock = asyncio.Lock()

    async def add_target(self, symbol: str):
        async with self._lock:
            if symbol in self.targets: return
            self.targets.add(symbol)
            self.flow_data[symbol] = FlowState(symbol)
            args = [f"publicTrade.{symbol}", f"tickers.{symbol}"]
            await self.sub_queue.put({"op": "subscribe", "args": args})

    async def run(self):
        await asyncio.gather(
            self._stream_handler(BYBIT_WS_LINEAR, "perp"),
            self._stream_handler(BYBIT_WS_SPOT, "spot")
        )

    async def _stream_handler(self, url: str, mode: str):
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url) as ws:
                        if mode == "perp": 
                            async def sender():
                                while True:
                                    msg = await self.sub_queue.get()
                                    await ws.send_json(msg)
                            asyncio.create_task(sender())

                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = orjson.loads(msg.data)
                                if "topic" in data:
                                    topic = data["topic"]
                                    if "publicTrade" in topic:
                                        self._process_trades(data["data"], mode)
                                    elif "tickers" in topic and mode == "perp":
                                        self._process_tickers(data["data"])
            except Exception:
                await asyncio.sleep(2)

    def _process_trades(self, trades: list, mode: str):
        now = time.time()
        for t in trades:
            sym = t['s']
            if sym not in self.targets: continue
            size = float(t['v'])
            price = float(t['p'])
            signed_vol = (size * price) * (1 if t['S'] == 'Buy' else -1)

            state = self.flow_data[sym]
            target_hist = state.perp_hist if mode == "perp" else state.spot_hist
            target_hist.append((now, signed_vol))

            cutoff = now - CVD_WINDOW_SEC
            while target_hist and target_hist[0][0] < cutoff:
                target_hist.popleft()
            
            cvd_val = sum(x[1] for x in target_hist)
            if mode == "perp": state.perp_cvd = cvd_val
            else: state.spot_cvd = cvd_val

    def _process_tickers(self, data: dict):
        item = data[0] if isinstance(data, list) else data
        sym = item.get('symbol')
        if sym not in self.targets: return
        state = self.flow_data[sym]
        if 'openInterest' in item: state.oi = float(item['openInterest'])
        if 'fundingRate' in item: state.funding = float(item['fundingRate'])

class MarketScanner:
    def __init__(self, flow_engine: FlowEngine):
        self.market_data: Dict[str, MarketState] = {}
        self.flow_engine = flow_engine

    async def run(self):
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(BINANCE_WS_FUT) as ws:
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                await self._process_tickers(orjson.loads(msg.data))
            except Exception:
                await asyncio.sleep(5)

    async def _process_tickers(self, data: list):
        for t in data:
            sym = t['s']
            if not sym.endswith("USDT"): continue
            price = float(t['c'])
            vol = float(t['q'])
            low = float(t['l'])
            pump = (price - low) / low * 100
            
            if sym not in self.market_data:
                self.market_data[sym] = MarketState(sym)
            m = self.market_data[sym]
            m.price = price
            m.price_low = low
            m.pump_24h = pump
            
            if vol > MIN_24H_VOL and pump > PUMP_THRESHOLD:
                await self.flow_engine.add_target(sym)

# --- EXECUTION ENGINE ---

class ExecutionEngine:
    def __init__(self, db: TradeDB, market: MarketScanner):
        self.active_trades: Dict[str, ActiveTrade] = {}
        self.db = db
        self.market = market
        self._trigger_lock = asyncio.Lock()
        
    async def update_loop(self):
        while True:
            current_time = time.time()
            for sym in list(self.active_trades.keys()):
                trade = self.active_trades[sym]
                if sym not in self.market.market_data: continue
                
                current_price = self.market.market_data[sym].price
                outcome = None
                
                if current_price >= trade.sl_price:
                    outcome = "🛑 STOP LOSS HIT"
                elif current_price <= trade.tp3:
                    outcome = "💰 TP3 (MAGNET) HIT"
                elif (current_time - trade.entry_time) > TIME_STOP_SEC:
                    outcome = "⌛ TIME STOP"
                    
                if outcome:
                    await self.db.log_trade(trade, current_price, outcome)
                    del self.active_trades[sym]
            await asyncio.sleep(1)

    async def check_trigger(self, sym: str, m: MarketState, f: FlowState):
        if sym in self.active_trades: return

        spot_dumping = f.spot_cvd < SPOT_CVD_LIMIT
        perp_fomo    = f.perp_cvd > PERP_CVD_LIMIT
        
        if spot_dumping and perp_fomo:
            async with self._trigger_lock:
                if sym in self.active_trades: return
                
                atr = await fetch_atr(sym)
                if atr == 0: atr = m.price * 0.02

                # LADDER LOGIC
                sl_price = m.price + (ATR_SL_MULT * atr)
                tp1 = m.price - (1.0 * atr)
                tp2 = m.price - (2.0 * atr)
                # TP3 is Liquidity Magnet (Low), or at least 3R away
                tp3 = min(m.price_low, m.price - (3.0 * atr))
                
                self.active_trades[sym] = ActiveTrade(
                    symbol=sym,
                    entry_price=m.price,
                    entry_time=time.time(),
                    sl_price=sl_price,
                    tp1=tp1, tp2=tp2, tp3=tp3,
                    atr=atr,
                    reason=f"Trap: Spot{f.spot_cvd/1000:.0f}k"
                )

# --- UI DASHBOARD ---

class Dashboard:
    def __init__(self, market, flow, execution, db):
        self.market = market
        self.flow = flow
        self.execution = execution
        self.db = db

    def generate_scanner_table(self) -> Table:
        table = Table(title="📡 LIVE RADAR (Trap Detection)", box=box.SIMPLE)
        table.add_column("Symbol", style="cyan")
        table.add_column("Pump", style="magenta")
        table.add_column("Spot CVD", style="yellow")
        table.add_column("Perp CVD", style="blue")
        table.add_column("OI", style="white")
        table.add_column("Explain", style="bold green")

        candidates = []
        for sym in list(self.flow.targets):
            if sym not in self.market.market_data: continue
            m = self.market.market_data[sym]
            f = self.flow.flow_data[sym]
            asyncio.create_task(self.execution.check_trigger(sym, m, f))

            spot_col = "red" if f.spot_cvd < 0 else "green"
            perp_col = "red" if f.perp_cvd < 0 else "green"
            
            explain = "Watching..."
            if f.spot_cvd < SPOT_CVD_LIMIT and f.perp_cvd > PERP_CVD_LIMIT: explain = "⚠️ TRAP!"
            elif f.spot_cvd < SPOT_CVD_LIMIT: explain = "📉 Spot Sold"
            
            candidates.append({
                "row": [sym, f"+{m.pump_24h:.1f}%", f"[{spot_col}]${f.spot_cvd/1000:.0f}k[/]", f"[{perp_col}]${f.perp_cvd/1000:.0f}k[/]", f"{f.oi:,.0f}", explain],
                "score": m.pump_24h
            })

        candidates.sort(key=lambda x: x["score"], reverse=True)
        for c in candidates[:8]: table.add_row(*c["row"])
        return table

    def generate_active_table(self) -> Table:
        table = Table(title="🚀 ACTIVE TRADES (Ladder Exits)", box=box.HEAVY_HEAD)
        table.add_column("Symbol", style="bold cyan")
        table.add_column("Entry", style="yellow")
        table.add_column("Price", style="white")
        table.add_column("SL (ATR)", style="bold red")
        table.add_column("TP1 | TP2 | TP3", style="green")
        table.add_column("Warning", style="bold orange1") # NEW WARNING COLUMN
        table.add_column("PnL", style="bold")
        
        trades = sorted(self.execution.active_trades.values(), key=lambda x: x.entry_time, reverse=True)
        for t in trades:
            if t.symbol not in self.market.market_data: continue
            curr = self.market.market_data[t.symbol].price
            pnl = (t.entry_price - curr) / t.entry_price * 100
            pnl_fmt = f"[green]{pnl:+.2f}%[/]" if pnl > 0 else f"[red]{pnl:+.2f}%[/]"
            
            # Warning Logic
            elapsed = time.time() - t.entry_time
            sl_dist = (t.sl_price - curr) / t.sl_price * 100
            
            warn = "🟢 CRUISING"
            if elapsed > (TIME_STOP_SEC * 0.8): warn = "⏳ STALE"
            if abs(sl_dist) < 0.5: warn = "⚠️ NEAR SL"
            
            ladder = f"{t.tp1:.2f} | {t.tp2:.2f} | {t.tp3:.2f}"

            table.add_row(t.symbol, f"{t.entry_price}", f"{curr}", f"{t.sl_price:.3f}", ladder, warn, pnl_fmt)
        return table

    async def generate_history_table(self) -> Table:
        table = Table(title="📜 TRADE HISTORY (SQLite)", box=box.DOUBLE_EDGE)
        table.add_column("Time", style="dim")
        table.add_column("Symbol", style="cyan")
        table.add_column("PnL", style="bold")
        table.add_column("Outcome")
        table.add_column("Reason")

        history = await self.db.get_history(5)
        for row in history:
            pnl = row['pnl_pct']
            pnl_fmt = f"[green]{pnl:+.2f}%[/]" if pnl > 0 else f"[red]{pnl:+.2f}%[/]"
            outcome_raw = row['outcome']
            out_col = "green" if "PROFIT" in outcome_raw or "TP" in outcome_raw else "red"
            table.add_row(row['timestamp'].split(' ')[1], row['symbol'], pnl_fmt, f"[{out_col}]{row['outcome']}[/]", row['reason'])
        return table

async def main():
    db = TradeDB(DB_FILE)
    await db.init()
    flow = FlowEngine()
    market = MarketScanner(flow)
    execution = ExecutionEngine(db, market)
    dashboard = Dashboard(market, flow, execution, db)

    asyncio.create_task(flow.run())
    asyncio.create_task(market.run())
    asyncio.create_task(execution.update_loop())

    layout = Layout()
    layout.split_column(Layout(name="top", ratio=2), Layout(name="mid", ratio=2), Layout(name="bot", ratio=2))

    with Live(layout, refresh_per_second=2) as live:
        while True:
            layout["top"].update(Panel(dashboard.generate_scanner_table()))
            layout["mid"].update(Panel(dashboard.generate_active_table()))
            layout["bot"].update(Panel(await dashboard.generate_history_table()))
            await asyncio.sleep(0.5)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Shutting down...")