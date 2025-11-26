# -*- coding: utf-8 -*-
"""
🔮 DeepFlow v8: Slang Edition
- Added "Trader Slang" Column with relevant emojis.
- Translates complex microstructure signals into "Crypto Speak".
- LOCKED UI: Stable, non-jumping dashboard.
"""

import asyncio
import aiohttp
import json
import numpy as np
import logging
import subprocess
import time
from collections import deque
from dataclasses import dataclass, field
from typing import List, Dict
from rich.live import Live
from rich.table import Table
from rich.console import Console
from rich import box

# ==========================================
# 1. USER CONFIGURATION
# ==========================================

SYMBOLS = [
    "BTC", "ETH", "SOL", "PIPPIN", "GOAT", 
    "PARTI", "XRP", "FARTCOIN", "ACT", "BONK"
]

# --- STABILIZATION SETTINGS ---
PERSISTENCE_SEC = 0.5 
OFI_TRIGGER = 50000   
OFI_RESET   = 25000   
EMA_ALPHA = 0.2 

# Audio
SOUND_ENABLED = True
SOUND_PUMP = "/System/Library/Sounds/Glass.aiff" 
SOUND_DUMP = "/System/Library/Sounds/Basso.aiff" 
SOUND_ALERT = "/System/Library/Sounds/Ping.aiff" 

# ==========================================
# 2. STATE & STABILIZATION
# ==========================================

@dataclass
class MarketState:
    symbol: str
    stream_name: str = "" 
    price: float = 0.0
    
    # Order Flow
    bb_p: float = 0.0; bb_q: float = 0.0
    ba_p: float = 0.0; ba_q: float = 0.0
    
    # Histories
    cvd_history: deque = field(default_factory=lambda: deque(maxlen=100))
    price_history: deque = field(default_factory=lambda: deque(maxlen=100))
    ofi_history: deque = field(default_factory=lambda: deque(maxlen=20))
    
    # Stabilization
    smoothed_ofi: float = 0.0
    pending_signal: str = "NONE"
    signal_start_ts: float = 0.0
    confirmed_signal: str = "NONE"
    
    # Metrics
    last_1s_vol: float = 0.0
    last_1s_price: float = 0.0
    
    # Display
    status: str = "INIT..."
    color: str = "dim white"
    last_alert_ts: float = 0.0

states: Dict[str, MarketState] = {s: MarketState(symbol=s) for s in SYMBOLS}
STREAM_TO_SYMBOL = {}

async def resolve_symbols():
    print("🔍 Auto-resolving symbols on Binance Futures...")
    async with aiohttp.ClientSession() as session:
        async with session.get("https://fapi.binance.com/fapi/v1/exchangeInfo") as resp:
            data = await resp.json()
            all_pairs = [s['symbol'] for s in data['symbols'] if s['status'] == 'TRADING']
            for user_sym in SYMBOLS:
                target = f"{user_sym}USDT"
                target_1k = f"1000{user_sym}USDT"
                if target in all_pairs:
                    states[user_sym].stream_name = target.lower()
                    STREAM_TO_SYMBOL[target.lower()] = user_sym
                elif target_1k in all_pairs:
                    states[user_sym].stream_name = target_1k.lower()
                    STREAM_TO_SYMBOL[target_1k.lower()] = user_sym
                else:
                    states[user_sym].status = "❌ NOT FOUND"

# ==========================================
# 3. LOGIC & SLANG ENGINE
# ==========================================

def play_sound(sound_file):
    if SOUND_ENABLED:
        try: subprocess.Popen(["afplay", sound_file])
        except: pass

def get_slope(history_list):
    if len(history_list) < 10: return 0
    y = np.array(history_list)
    x = np.arange(len(y))
    return np.polyfit(x, y, 1)[0]

def get_trader_slang(status_code):
    """Maps technical status to cute trader slang"""
    mapping = {
        "🟢 PUMPING": "🐂 Bulls Arrived!",
        "🔴 DUMPING": "🐻 Bears in Control",
        "🚀 V-BOUNCE": "🎯 Sniper Entry",
        "🟢 HEALTHY FLUSH": "💎🙌 Diamond Hands",
        "🔴 REAL SELL": "🐋 Whales Dumping",
        "💀 TRAP": "🪤 Fakeout / Trap",
        "🟡 CHOP": "🦀 Crabbing",
        "❌ NOT FOUND": "👻 Ghost Town",
        "INIT...": "👀 Watching..."
    }
    return mapping.get(status_code, "Unknown")

def calculate_ofi(st: MarketState, bp, bq, ap, aq):
    mid = (bp + ap) / 2
    ofi_qty = 0
    if bp > st.bb_p: ofi_qty += bq
    elif bp == st.bb_p: ofi_qty += (bq - st.bb_q)
    else: ofi_qty -= st.bb_q
    if ap < st.ba_p: ofi_qty -= aq
    elif ap == st.ba_p: ofi_qty -= (aq - st.ba_q)
    else: ofi_qty += st.ba_q
    st.bb_p, st.bb_q, st.ba_p, st.ba_q = bp, bq, ap, aq
    
    raw_ofi_usd = ofi_qty * mid
    if st.smoothed_ofi == 0: st.smoothed_ofi = raw_ofi_usd
    else: st.smoothed_ofi = (raw_ofi_usd * EMA_ALPHA) + (st.smoothed_ofi * (1 - EMA_ALPHA))
    st.ofi_history.append(st.smoothed_ofi)

def analyze_market(st: MarketState):
    if len(st.cvd_history) < 20: return
    
    net_ofi = sum(list(st.ofi_history))
    cvd_slope = get_slope(list(st.cvd_history)[-20:])
    price_slope = get_slope(list(st.price_history)[-20:])
    
    # --- 1. INSTANT SIGNALS (Overrides) ---
    vol_spike = st.last_1s_vol > 150000 
    price_flat = abs(st.price - st.last_1s_price) / (st.price or 1) < 0.0001
    
    # Sniper Entry
    if vol_spike and price_flat:
        st.status = "🚀 V-BOUNCE"
        st.color = "bold magenta"
        if time.time() - st.last_alert_ts > 5:
            play_sound(SOUND_ALERT)
            st.last_alert_ts = time.time()
        return

    # True Top / Diamond Hands
    if price_slope < 0:
        if cvd_slope > 0:
            st.status = "🟢 HEALTHY FLUSH"
            st.color = "green"
            return
        elif cvd_slope < 0:
            st.status = "🔴 REAL SELL"
            st.color = "bold red"
            if time.time() - st.last_alert_ts > 10:
                play_sound(SOUND_DUMP)
                st.last_alert_ts = time.time()
            return

    # --- 2. STABILIZED TREND SIGNALS ---
    candidate = "NONE"
    if st.confirmed_signal == "PUMP":
        if net_ofi > OFI_RESET: candidate = "PUMP"
    elif st.confirmed_signal == "DUMP":
        if net_ofi < -OFI_RESET: candidate = "DUMP"
    else:
        if net_ofi > OFI_TRIGGER and cvd_slope > 0: candidate = "PUMP"
        elif net_ofi < -OFI_TRIGGER and cvd_slope < 0: candidate = "DUMP"

    now = time.time()
    if candidate != st.pending_signal:
        st.pending_signal = candidate
        st.signal_start_ts = now
    
    if (now - st.signal_start_ts) > PERSISTENCE_SEC:
        if st.confirmed_signal != candidate:
            st.confirmed_signal = candidate
            if candidate == "PUMP": play_sound(SOUND_PUMP)
            elif candidate == "DUMP": play_sound(SOUND_DUMP)

    if st.confirmed_signal == "PUMP":
        st.status = "🟢 PUMPING"
        st.color = "bold green"
    elif st.confirmed_signal == "DUMP":
        st.status = "🔴 DUMPING"
        st.color = "bold red"
    else:
        st.status = "🟡 CHOP"
        st.color = "dim white"

# ==========================================
# 4. DATA STREAM
# ==========================================

async def binance_stream():
    await resolve_symbols()
    while True:
        try:
            streams = []
            valid_streams = [st.stream_name for st in states.values() if st.stream_name]
            for s in valid_streams:
                streams.append(f"{s}@bookTicker")
                streams.append(f"{s}@aggTrade")
            
            url = f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(url) as ws:
                    async for msg in ws:
                        data = json.loads(msg.data)
                        stream = data['stream']
                        payload = data['data']
                        s_name = stream.split('@')[0]
                        base_sym = STREAM_TO_SYMBOL.get(s_name)
                        if not base_sym: continue
                        st = states[base_sym]
                        
                        if "@bookTicker" in stream:
                            calculate_ofi(st, float(payload['b']), float(payload['B']), float(payload['a']), float(payload['A']))
                        elif "@aggTrade" in stream:
                            p = float(payload['p'])
                            q = float(payload['q'])
                            m = payload['m']
                            st.last_1s_vol = p * q
                            st.last_1s_price = st.price
                            st.price = p
                            st.price_history.append(p)
                            delta = -(p*q) if m else (p*q)
                            st.cvd_history.append(delta)
        except Exception: await asyncio.sleep(5)

# ==========================================
# 5. UI RENDERER
# ==========================================

def generate_table():
    table = Table(
        title="[bold italic]🔮 DeepFlow v8: Slang Edition[/]", 
        expand=True, 
        box=box.ROUNDED,
        border_style="dim white",
        show_lines=True,
        header_style="bold cyan"
    )
    # LOCKED WIDTHS
    table.add_column("Symbol", style="bold white", justify="left", min_width=10)
    table.add_column("Price", justify="right", min_width=12)
    table.add_column("Net OFI ($)", justify="right", min_width=15)
    table.add_column("Signal", justify="center", min_width=15)
    table.add_column("Trader Slang", justify="left", min_width=20) # NEW COLUMN

    for s in SYMBOLS:
        st = states[s]
        if not st.stream_name:
            table.add_row(s, "-", "-", "[red]NOT FOUND[/]", "👻 Ghost Town")
            continue
        analyze_market(st)
        
        ofi = sum(list(st.ofi_history))
        c_ofi = "green" if ofi > 0 else "red"
        slang = get_trader_slang(st.status) # Get Slang Text

        table.add_row(
            s, 
            f"{st.price:.5f}", 
            f"[{c_ofi}]${ofi/1000:.1f}k[/{c_ofi}]", 
            f"[{st.color}]{st.status}[/{st.color}]",
            f"[{st.color}]{slang}[/{st.color}]" # Slang with matching color
        )
    return table

async def ui_loop():
    console = Console(force_terminal=True)
    with Live(generate_table(), refresh_per_second=4, console=console, screen=True) as live:
        while True:
            live.update(generate_table())
            await asyncio.sleep(0.25)

async def main():
    await asyncio.gather(binance_stream(), ui_loop())

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: print("Stopped.")