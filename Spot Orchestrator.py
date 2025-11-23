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
from rich.text import Text
from rich.console import Console

# ==========================================
#   ORCHESTRATOR V3.1: VISUAL MICROCAP SCREENER
# ==========================================

# --- CONFIGURATION ---
CG_API_BASE_URL = "https://api.coingecko.com/api/v3"
MIN_MARKET_CAP_USD = 1_000_000
MAX_MARKET_CAP_USD = 50_000_000
MIN_24H_VOLUME_USD = 100_000
MIN_24H_PRICE_CHANGE_PCT = 30.0 
VOLUME_SPIKE_FACTOR = 5.0 
MIN_1H_PRICE_CHANGE_PCT = 5.0 
HISTORICAL_DATA_DAYS = 7 
SCAN_INTERVAL_SECONDS = 300 
DB_NAME = "microcap_screener.sqlite"
ALERT_THRESHOLD = 75 

# --- STATE MANAGEMENT FOR UI ---
class ScreenerState:
    def __init__(self):
        self.status = "Initializing..."
        self.last_scan_time = "N/A"
        self.candidates: List[Dict] = []  # Coins currently being analyzed
        self.alerts = deque(maxlen=10)    # Last 10 alerts found
        self.scan_progress = ""

state = ScreenerState()

# --- DATABASE SETUP ---
def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS alerted_pumps (
            coin_id TEXT PRIMARY KEY,
            symbol TEXT,
            alert_timestamp INTEGER,
            alert_price REAL,
            market_cap REAL,
            volume_24h REAL,
            change_24h REAL,
            confidence_score INTEGER,
            tags TEXT
        )
    ''')
    conn.commit()
    conn.close()

def has_been_alerted(coin_id: str, hours: int = 24) -> bool:
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    threshold = int(time.time()) - (hours * 3600)
    cursor.execute("SELECT 1 FROM alerted_pumps WHERE coin_id = ? AND alert_timestamp > ?", (coin_id, threshold))
    result = cursor.fetchone()
    conn.close()
    return result is not None

def record_alert(coin: Dict[str, Any], score: int, tags: List[str]):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR REPLACE INTO alerted_pumps 
        (coin_id, symbol, alert_timestamp, alert_price, market_cap, volume_24h, change_24h, confidence_score, tags)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        coin['id'], coin['symbol'].upper(), int(time.time()), coin['current_price'],
        coin['market_cap'], coin['total_volume'], coin['price_change_percentage_24h'],
        score, ','.join(tags)
    ))
    conn.commit()
    conn.close()
    
    # Add to UI state
    alert_entry = {
        "time": datetime.now().strftime("%H:%M:%S"),
        "symbol": coin['symbol'].upper(),
        "price": coin['current_price'],
        "change": coin['price_change_percentage_24h'],
        "score": score,
        "tags": tags
    }
    state.alerts.appendleft(alert_entry)

# --- API CLIENT ---
async def get_top_movers(session: RetryClient) -> List[Dict[str, Any]]:
    state.status = "Fetching Global Market Data..."
    url = f"{CG_API_BASE_URL}/coins/markets"
    params = {
        'vs_currency': 'usd', 'order': 'market_cap_desc', 'per_page': 250,
        'page': 1, 'sparkline': 'false', 'price_change_percentage': '24h'
    }
    all_coins = []
    for page in range(1, 6): 
        try:
            state.scan_progress = f"Page {page}/5"
            params['page'] = page
            async with session.get(url, params=params) as response:
                if response.status == 429:
                    state.status = "⚠️ API Rate Limit (429). Pausing..."
                    await asyncio.sleep(60)
                    continue
                response.raise_for_status()
                data = await response.json()
                if not data: break
                all_coins.extend(data)
                await asyncio.sleep(1.5) 
        except Exception:
            break
    return all_coins

async def get_historical_data(session: RetryClient, coin_id: str) -> Optional[Dict[str, Any]]:
    url = f"{CG_API_BASE_URL}/coins/{coin_id}/market_chart"
    params = {'vs_currency': 'usd', 'days': HISTORICAL_DATA_DAYS, 'interval': 'hourly'}
    try:
        async with session.get(url, params=params) as response:
            if response.status == 429: return None
            response.raise_for_status()
            return await response.json()
    except Exception:
        return None

# --- ANALYSIS ENGINE ---
def analyze_candidate(coin: Dict[str, Any], historical_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        prices = historical_data.get('prices', [])
        volumes = historical_data.get('total_volumes', [])
        if len(prices) < 24 or len(volumes) < 24: return None

        # Metrics
        recent_volumes = [v[1] for v in volumes[-24:]] 
        avg_volume = sum(recent_volumes) / len(recent_volumes)
        current_volume = recent_volumes[-1]
        volume_spike = current_volume / avg_volume if avg_volume > 0 else 0

        price_now = prices[-1][1]
        price_1h_ago = prices[-2][1]
        change_1h = ((price_now - price_1h_ago) / price_1h_ago) * 100 if price_1h_ago > 0 else 0

        # Scoring
        score = 0
        tags = []
        score += 40 # Base
        tags.append(f"{coin.get('price_change_percentage_24h', 0):.1f}%_24H")

        if volume_spike >= VOLUME_SPIKE_FACTOR:
            score += 35
            tags.append(f"VOLx{volume_spike:.1f}")
        
        if change_1h >= MIN_1H_PRICE_CHANGE_PCT:
            score += 25
            tags.append(f"MOM_{change_1h:.1f}%_1H")

        return {"score": score, "tags": tags, "vol_spike": volume_spike, "mom_1h": change_1h}
    except Exception:
        return None

# --- UI GENERATION ---
def generate_dashboard() -> Layout:
    layout = Layout()
    layout.split(
        Layout(name="header", size=3),
        Layout(name="main", ratio=1),
        Layout(name="footer", size=12)
    )
    
    # Header
    status_style = "bold green" if "Sleeping" in state.status else "bold yellow"
    header_text = f"🚀 ORCHESTRATOR V3.1 | [dim]Cap:[/dim] ${MIN_MARKET_CAP_USD/1e6:.0f}M-${MAX_MARKET_CAP_USD/1e6:.0f}M | [dim]Pump:[/dim] >{MIN_24H_PRICE_CHANGE_PCT}% | [dim]Status:[/dim] [{status_style}]{state.status}[/]"
    layout["header"].update(Panel(header_text, style="white on blue"))

    # Main Table (Candidates)
    table = Table(title="🔎 LIVE CANDIDATE SCANNER", expand=True, box=box.SIMPLE_HEAD)
    table.add_column("Symbol", style="cyan bold")
    table.add_column("Price", justify="right")
    table.add_column("24h Change", justify="right")
    table.add_column("Vol Spike", justify="center")
    table.add_column("1h Mom", justify="center")
    table.add_column("Score", justify="right")
    
    if not state.candidates:
        table.add_row("-", "-", "-", "-", "-", "-")
    else:
        for c in state.candidates:
            # Color coding
            change_color = "green" if c['change_24h'] > 0 else "red"
            mom_color = "green" if c.get('mom_1h', 0) > 0 else "red"
            
            score_str = str(c.get('score', 0))
            if c.get('score', 0) >= ALERT_THRESHOLD:
                score_str = f"[bold green on white]{score_str}[/]"
            elif c.get('score', 0) >= 50:
                score_str = f"[yellow]{score_str}[/]"
            else:
                score_str = f"[dim]{score_str}[/]"

            vol_str = f"{c.get('vol_spike', 0):.1f}x"
            if c.get('vol_spike', 0) >= VOLUME_SPIKE_FACTOR:
                vol_str = f"[bold magenta]{vol_str}[/]"

            table.add_row(
                c['symbol'],
                f"${c['price']:.6f}",
                f"[{change_color}]{c['change_24h']:.2f}%[/]",
                vol_str,
                f"[{mom_color}]{c.get('mom_1h', 0):.2f}%[/]",
                score_str
            )
    
    layout["main"].update(Panel(table))

    # Footer (Alerts)
    alert_table = Table(title="🚨 HIGH CONVICTION ALERTS LOG", expand=True, box=box.HORIZONTALS)
    alert_table.add_column("Time", style="dim")
    alert_table.add_column("Symbol", style="bold yellow")
    alert_table.add_column("Price")
    alert_table.add_column("Score", style="bold red")
    alert_table.add_column("Tags")

    if not state.alerts:
        alert_table.add_row("-", "No alerts yet", "-", "-", "-")
    else:
        for a in state.alerts:
            alert_table.add_row(
                a['time'], a['symbol'], f"${a['price']:.6f}", str(a['score']), ",".join(a['tags'])
            )

    layout["footer"].update(Panel(alert_table))
    return layout

# --- MAIN LOOP ---
async def screener_loop():
    retry_options = ExponentialRetry(attempts=3)
    async with RetryClient(raise_for_status=False, retry_options=retry_options) as session:
        while True:
            start_time = time.time()
            state.last_scan_time = datetime.now().strftime("%H:%M:%S")
            state.candidates = [] # Clear previous scan

            try:
                # 1. Broad Fetch
                all_coins = await get_top_movers(session)
                
                # 2. Filter
                potential_pumps = []
                state.status = "Filtering Universe..."
                for c in all_coins:
                    if not c.get('market_cap') or not c.get('total_volume') or not c.get('price_change_percentage_24h'): continue
                    if not (MIN_MARKET_CAP_USD <= c['market_cap'] <= MAX_MARKET_CAP_USD): continue
                    if c['total_volume'] < MIN_24H_VOLUME_USD: continue
                    if c['price_change_percentage_24h'] >= MIN_24H_PRICE_CHANGE_PCT:
                        potential_pumps.append(c)
                
                # Add to UI as raw candidates first
                for p in potential_pumps:
                    state.candidates.append({
                        "symbol": p['symbol'].upper(), "price": p['current_price'],
                        "change_24h": p['price_change_percentage_24h'], "score": 40, "vol_spike": 0, "mom_1h": 0
                    })
                
                # 3. Deep Analysis
                state.status = f"Analyzing {len(potential_pumps)} Candidates..."
                verified_results = []
                
                for i, coin in enumerate(potential_pumps):
                    coin_id = coin['id']
                    if has_been_alerted(coin_id): continue
                    
                    await asyncio.sleep(2.0) # Rate limit
                    hist_data = await get_historical_data(session, coin_id)
                    if not hist_data: continue

                    res = analyze_candidate(coin, hist_data)
                    if res:
                        # Update the candidate in the UI list with real data
                        for cand in state.candidates:
                            if cand['symbol'] == coin['symbol'].upper():
                                cand['score'] = res['score']
                                cand['vol_spike'] = res['vol_spike']
                                cand['mom_1h'] = res['mom_1h']

                        if res['score'] >= ALERT_THRESHOLD:
                            record_alert(coin, res['score'], res['tags'])

            except Exception as e:
                state.status = f"Error: {str(e)[:20]}"

            elapsed = time.time() - start_time
            sleep_time = max(0, SCAN_INTERVAL_SECONDS - elapsed)
            
            # Countdown timer in UI
            for i in range(int(sleep_time), 0, -1):
                state.status = f"Sleeping... Next scan in {i}s"
                await asyncio.sleep(1)

async def ui_loop(live):
    """Refreshes the UI independently of the screener loop."""
    while True:
        live.update(generate_dashboard())
        await asyncio.sleep(0.5)

async def main():
    init_db()
    # Run UI and Screener concurrently
    with Live(generate_dashboard(), refresh_per_second=4, screen=True) as live:
        await asyncio.gather(
            screener_loop(),
            ui_loop(live)
        )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Exiting...")