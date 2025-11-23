import asyncio
import aiohttp
import sqlite3
import logging
import time
from datetime import datetime
from typing import List, Dict, Any, Optional
from collections import deque
from aiohttp_retry import RetryClient, ExponentialRetry

# --- RICH UI IMPORTS ---
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich import box
from rich.console import Console

# ==========================================
#   MEXC ORCHESTRATOR: PURE EXCHANGE DATA
# ==========================================

# --- CONFIGURATION ---
MEXC_BASE_URL = "https://api.mexc.com"
MIN_24H_VOLUME_USDT = 50_000      # Ignore dead coins (<$50k volume)
MAX_24H_VOLUME_USDT = 50_000_000  # Ignore Majors (BTC/ETH/SOL)
MIN_PRICE_CHANGE_24H = 10.0       # Only look at coins pumping >10%
VOLUME_SPIKE_FACTOR = 3.0         # Current Vol must be 3x Average
SCAN_INTERVAL_SECONDS = 15        # Scan speed (Much faster than CoinGecko)
ALERT_THRESHOLD = 75              # Score to trigger DB log
DB_NAME = "mexc_orchestrator.sqlite"

# --- STATE MANAGEMENT ---
class ScreenerState:
    def __init__(self):
        self.status = "Initializing..."
        self.last_scan_time = "N/A"
        self.candidates: List[Dict] = []
        self.alerts = deque(maxlen=10)
        self.scan_progress = ""

state = ScreenerState()

# --- DATABASE SETUP ---
def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS mexc_alerts (
            symbol TEXT PRIMARY KEY,
            alert_timestamp INTEGER,
            alert_price REAL,
            volume_24h REAL,
            change_24h REAL,
            score INTEGER,
            tags TEXT
        )
    ''')
    conn.commit()
    conn.close()

def has_been_alerted(symbol: str, cooldown_hours: int = 4) -> bool:
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    threshold = int(time.time()) - (cooldown_hours * 3600)
    cursor.execute("SELECT 1 FROM mexc_alerts WHERE symbol = ? AND alert_timestamp > ?", (symbol, threshold))
    result = cursor.fetchone()
    conn.close()
    return result is not None

def record_alert(data: Dict[str, Any]):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR REPLACE INTO mexc_alerts 
        (symbol, alert_timestamp, alert_price, volume_24h, change_24h, score, tags)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (
        data['symbol'], int(time.time()), data['price'],
        data['volume'], data['change_24h'], data['score'], ','.join(data['tags'])
    ))
    conn.commit()
    conn.close()
    
    alert_entry = {
        "time": datetime.now().strftime("%H:%M:%S"),
        "symbol": data['symbol'],
        "price": data['price'],
        "change": data['change_24h'],
        "score": data['score'],
        "tags": data['tags']
    }
    state.alerts.appendleft(alert_entry)

# --- MEXC API ENGINE ---
async def fetch_all_tickers(session: RetryClient) -> List[Dict[str, Any]]:
    """Fetches snapshots of ALL 2000+ coins on MEXC"""
    state.status = "Scanning MEXC Global Market..."
    url = f"{MEXC_BASE_URL}/api/v3/ticker/24hr"
    try:
        async with session.get(url) as response:
            if response.status == 429:
                state.status = "⚠️ HTTP 429: Rate Limited. Cooling down..."
                await asyncio.sleep(5)
                return []
            return await response.json()
    except Exception as e:
        state.status = f"API Error: {str(e)}"
        return []

async def analyze_candles(session: RetryClient, symbol: str) -> Dict[str, float]:
    """Fetches 1H candles to calculate Volume Spike & Momentum"""
    url = f"{MEXC_BASE_URL}/api/v3/klines"
    # Get last 24 hours of 1-hour candles
    params = {'symbol': symbol, 'interval': '60m', 'limit': 24}
    try:
        async with session.get(url, params=params) as response:
            if response.status != 200: return {}
            data = await response.json()
            
            if not data or len(data) < 5: return {}

            # Data format: [Time, Open, High, Low, Close, Volume, ...]
            # Calculate Average Volume of previous candles vs Current Candle
            volumes = [float(k[5]) for k in data]
            
            current_vol = volumes[-1]
            # Average of the last 24h excluding current
            avg_vol = sum(volumes[:-1]) / len(volumes[:-1]) if len(volumes) > 1 else 1
            
            vol_spike = current_vol / avg_vol if avg_vol > 0 else 0
            
            # Calculate 1H Momentum (Close vs Open of current candle)
            open_p = float(data[-1][1])
            close_p = float(data[-1][4])
            mom_1h = ((close_p - open_p) / open_p) * 100
            
            return {"vol_spike": vol_spike, "mom_1h": mom_1h}
    except:
        return {}

# --- FILTERING PIPELINE ---
async def process_market(session: RetryClient):
    raw_data = await fetch_all_tickers(session)
    candidates = []
    
    state.status = f"Filtering {len(raw_data)} Pairs..."
    
    # 1. FAST FILTER (Local CPU)
    for ticker in raw_data:
        symbol = ticker['symbol']
        
        # Logic: Must be USDT pair
        if not symbol.endswith("USDT"): continue
        
        try:
            vol = float(ticker['quoteVolume']) # Volume in USDT
            change = float(ticker['priceChangePercent']) * 100
            last_price = float(ticker['lastPrice'])
        except ValueError:
            continue
        
        # Logic: Orchestrator Rules
        if vol < MIN_24H_VOLUME_USDT: continue  # Skip dead coins
        if vol > MAX_24H_VOLUME_USDT: continue  # Skip BTC/ETH
        if change < MIN_PRICE_CHANGE_24H: continue # Skip non-movers
        
        candidates.append({
            "symbol": symbol,
            "price": last_price,
            "volume": vol,
            "change_24h": change
        })
    
    # Sort by % Change to prioritize top gainers
    candidates.sort(key=lambda x: x['change_24h'], reverse=True)
    
    # 2. DEEP ANALYSIS (API Calls)
    # We only analyze the top 20 candidates to avoid Rate Limits
    top_candidates = candidates[:20]
    state.candidates = [] # Clear UI list for update
    
    state.status = f"Analyzing Top {len(top_candidates)} Momentum Targets..."
    
    analyzed_batch = []
    for cand in top_candidates:
        if has_been_alerted(cand['symbol']): continue
        
        # Fetch Candles for deep metrics
        metrics = await analyze_candles(session, cand['symbol'])
        if not metrics: continue
        
        # Calculate Score
        score = 40 # Base score for passing filter
        tags = [f"{cand['change_24h']:.1f}%_24H"]
        
        vol_spike = metrics.get('vol_spike', 0)
        mom_1h = metrics.get('mom_1h', 0)
        
        if vol_spike > VOLUME_SPIKE_FACTOR:
            score += 30
            tags.append(f"VOLx{vol_spike:.1f}")
        
        if mom_1h > 3.0: # 3% move in last hour
            score += 30
            tags.append(f"MOM_{mom_1h:.1f}%")
            
        # Update Candidate Object
        final_obj = {
            **cand,
            "score": score,
            "vol_spike": vol_spike,
            "mom_1h": mom_1h,
            "tags": tags
        }
        
        analyzed_batch.append(final_obj)
        
        # Alert Logic
        if score >= ALERT_THRESHOLD:
            record_alert(final_obj)
            
        await asyncio.sleep(0.1) # Tiny sleep to be nice to API
    
    # Update state with analyzed batch (sorted by score this time)
    analyzed_batch.sort(key=lambda x: x['score'], reverse=True)
    state.candidates = analyzed_batch

# --- UI GENERATION ---
def generate_dashboard() -> Layout:
    layout = Layout()
    layout.split(
        Layout(name="header", size=3),
        Layout(name="main", ratio=1),
        Layout(name="footer", size=10)
    )
    
    # Header (Typo Fixed)
    header_text = f"🦅 MEXC ORCHESTRATOR | Vol Filter: ${MIN_24H_VOLUME_USDT/1000:.0f}k-${MAX_24H_VOLUME_USDT/1e6:.0f}M | Pump: >{MIN_PRICE_CHANGE_24H}% | Status: {state.status}"
    layout["header"].update(Panel(header_text, style="white on blue"))

    # Main Table
    table = Table(title=f"🔎 LIVE MARKET SCAN (Top {len(state.candidates)})", expand=True, box=box.SIMPLE_HEAD)
    table.add_column("Symbol", style="cyan bold")
    table.add_column("Price", justify="right")
    table.add_column("24h Vol", justify="right")
    table.add_column("24h %", justify="right")
    table.add_column("Vol Spike", justify="center")
    table.add_column("1H Mom", justify="center")
    table.add_column("Score", justify="right")
    
    if not state.candidates:
        table.add_row("-", "-", "-", "-", "-", "-", "-")
    else:
        for c in state.candidates:
            change_color = "green" if c['change_24h'] > 0 else "red"
            spike_color = "magenta bold" if c['vol_spike'] >= VOLUME_SPIKE_FACTOR else "dim"
            
            score = c.get('score', 0)
            if score >= ALERT_THRESHOLD:
                score_style = "bold green on white"
            elif score >= 50:
                score_style = "bold yellow"
            else:
                score_style = "dim"
            
            # --- HYPERLINK LOGIC ---
            raw_symbol = c['symbol'] # e.g. BTCUSDT
            base_symbol = raw_symbol.replace("USDT", "")
            # MEXC URL Format: https://www.mexc.com/exchange/BTC_USDT
            url = f"https://www.mexc.com/exchange/{base_symbol}_USDT"
            linked_symbol = f"[link={url}]{base_symbol}[/link]"
            
            table.add_row(
                linked_symbol,
                f"${c['price']:.6f}",
                f"${c['volume']/1000:.0f}k",
                f"[{change_color}]{c['change_24h']:.1f}%[/]",
                f"[{spike_color}]{c['vol_spike']:.1f}x[/]",
                f"{c['mom_1h']:.1f}%",
                f"[{score_style}]{score}[/]"
            )
    
    layout["main"].update(Panel(table))

    # Footer
    alert_table = Table(title="🚨 ALERTS LOG", expand=True, box=box.MINIMAL)
    alert_table.add_column("Time", style="dim"); 
    alert_table.add_column("Symbol", style="bold yellow"); 
    alert_table.add_column("Price"); 
    alert_table.add_column("Reason")
    
    for a in state.alerts:
        alert_table.add_row(a['time'], a['symbol'], f"${a['price']:.6f}", ",".join(a['tags']))
        
    layout["footer"].update(Panel(alert_table))
    return layout

# --- RUNNER ---
async def main():
    init_db()
    retry_options = ExponentialRetry(attempts=3)
    
    with Live(generate_dashboard(), refresh_per_second=4, screen=True) as live:
        async with RetryClient(retry_options=retry_options) as session:
            while True:
                state.last_scan_time = datetime.now().strftime("%H:%M:%S")
                try:
                    await process_market(session)
                except Exception as e:
                    state.status = f"CRASH: {e}"
                    await asyncio.sleep(5)
                
                # Cooldown countdown
                for i in range(SCAN_INTERVAL_SECONDS, 0, -1):
                    state.status = f"Scanning Complete. Next sweep in {i}s..."
                    live.update(generate_dashboard())
                    await asyncio.sleep(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Exiting...")