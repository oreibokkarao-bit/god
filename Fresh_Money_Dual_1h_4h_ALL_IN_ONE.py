# ============================================================================
# Fresh_Money_Dual_1h_4h_ALL_IN_ONE.py — Single-File, All Features
#
# - Robust WS keepalive (Bybit-safe) with swallowed app-level ping/pong
# - MEXC candle sanitizer (safe_float + valid_ohlcv_tuple) in watch_candles
# - ADDED: Stale trade invalidation (Price < SL or Price > TP1)
# - ADDED: (FIX 4) "Freshness" check: Invalidate if price > 40% of move to TP1
# - ADDED: (FIX 5) Removed broken TP2/TP3 fallback logic
# - ADDED: (FIX 7) "Smart Fallback" for TP2/TP3 using ATR-Risk multiple
# - ADDED: (PHASE 2) Confluence check with free Liquidation stream data
# - ADDED: (FIX 9) Final robust keepalive logic for _KeepAliveWS.receive
# - ADDED: (FIX 10) Exhaustion Filter (vol_z > 3.0) to avoid V-reversals
# - ADDED: (FIX 11) Funding Rate Filter to avoid extreme funding traps
# - ADDED: (FIX 12) Data Staleness Check (CVD/OI) to prevent ffill traps
# - ADDED: (FIX 13) "POC Lock" to prevent entry drift-up on pumps
# - ADDED: (FIX 14) Removed __file__ dependency to fix Thonny %Run error
# - ADDED: (MERGE) Integrated liquidation_collector.py into this file
# - ADDED: (ROLLBACK) Removed broken Binance historical data connectors
# - ADDED: (FIX 16) Corrected NameError 'open_thr' in update_and_rank
# - ADDED: (FIX 17) Disabled redundant pinger in _KeepAliveWS
# - ADDED: (FIX 18) Added type check in liquidation_collector to prevent crash
#
# --- (MAJOR UPGRADE V2) ---
# - ADDED: Full trade management system (TradeManager class)
# - ADDED: SQLite database logging for all closed trades (trade_logger.py)
# - ADDED: Replaced `tabulate` with `rich` for a 3-table dynamic dashboard
# - ADDED: Dashboard for "Live Trades", "Recent Closes", and "Overall Stats"
# - ADDED: Expectancy, PnL, and R-Unit calculations
# - FIXED: Bitget `code: 30006` rate limit error in subscribe_all
# - FIXED: Bitget ping/pong timeout error in _KeepAliveWS.receive
# ============================================================================

# ============================ EMBEDDED MODULE ============================
# ws_keepalive.py — robust aiohttp ws_connect keepalive for Bybit V5
import os, asyncio, json, random

try:
    import aiohttp
except Exception:
    aiohttp = None

PING_INTERVAL = int(os.environ.get("FRESH_MONEY_WS_PING", "20"))
RECV_TIMEOUT  = int(os.environ.get("FRESH_MONEY_WS_TIMEOUT", "65"))


class _KeepAliveWS:
    def __init__(self, ws):
        self._ws = ws
        self._task = None
        self._closed = False

    async def __aenter__(self):
        # --- (FIX 17) Disabled redundant pinger ---
        # await self.start() 
        # --- END (FIX 17) ---
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()
        return False

    def __getattr__(self, name):
        return getattr(self._ws, name)

    async def start(self):
        if self._task is None:
            self._task = asyncio.create_task(self._pinger(), name="ws-pinger")

    async def _pinger(self):
        try:
            # small jitter so multiple sockets don't ping at once
            await asyncio.sleep(random.uniform(0.2, 1.2))
            while not self._ws.closed:
                # send a control-frame ping
                try:
                    await self._ws.ping()
                except Exception:
                    break
                await asyncio.sleep(PING_INTERVAL + random.uniform(-2, 2))
        finally:
            pass

    # --- START (FIX 9) - Replaced function ---
    async def receive(self, *args, **kwargs):
        """
        Intercepts messages to handle all forms of ping/pong (transport,
        raw text, and JSON) before they can crash the ccxt.pro JSON parser.
        """
        while True:
            msg = await self._ws.receive(*args, **kwargs)

            if aiohttp is None:
                return msg # Failsafe if aiohttp isn't loaded

            # 1. Handle Transport-Level PING/PONG
            # Let ccxt.pro's internal keepalive handle PONGs
            if msg.type == aiohttp.WSMsgType.PONG:
                return msg 
            # Swallow transport PINGs; aiohttp handles them automatically
            if msg.type == aiohttp.WSMsgType.PING:
                continue

            # 2. Handle Text-Based PING/PONG (the source of the bug)
            if msg.type == aiohttp.WSMsgType.TEXT:
                raw_data = msg.data
                raw_clean = raw_data.strip().lower()

                # Bybit sends raw "ping"
                if raw_clean == "ping":
                    try:
                        await self._ws.send_str("pong")
                    except Exception:
                        pass
                    continue # Swallow the ping

                # Bybit sends raw "pong"
                if raw_clean == "pong":
                    continue # Swallow the pong

                # 3. Handle JSON-Based PING/PONG
                try:
                    data = json.loads(raw_data)
                    
                    if isinstance(data, dict):
                        op = (data.get("op") or "").lower()
                        
                        # Respond to {"op":"ping"}
                        if op == "ping":
                            ts = None
                            args_ = data.get("args")
                            if isinstance(args_, list) and args_:
                                ts = args_[0]
                            try:
                                if ts is not None:
                                    await self._ws.send_str(json.dumps({"op": "pong", "args": [ts]}))
                                else:
                                    await self._ws.send_str(json.dumps({"op": "pong"}))
                            except Exception:
                                pass
                            continue # Swallow the ping
                        
                        # Swallow {"op":"pong"}
                        if op == "pong":
                            continue
                            
                except json.JSONDecodeError:
                    # --- (FIX) FOR BITGET KEEPALIVE ---
                    # This was unexpected raw text that wasn't "ping" or "pong".
                    # DO NOT swallow it. Let ccxt.pro try to handle it.
                    return msg
                    # --- END (FIX) ---
                except Exception:
                    # --- (FIX) FOR BITGET KEEPALIVE ---
                    # Some other parse error, also let it pass through
                    return msg
                    # --- END (FIX) ---

            # 4. If it's not a keepalive, return the message
            return msg
    # --- END (FIX 9) ---

    async def close(self):
        if self._closed:
            return
        self._closed = True
        if self._task:
            self._task.cancel()
        try:
            if not self._ws.closed:
                await self._ws.close()
        except Exception:
            pass


_original_ws_connect = None


def patch_aiohttp_ws_keepalive():
    global _original_ws_connect, aiohttp
    if aiohttp is None:
        import importlib
        aiohttp = importlib.import_module("aiohttp")

    if _original_ws_connect is None:
        _original_ws_connect = aiohttp.ClientSession.ws_connect

        def _ws_connect_with_keepalive(self, *args, **kwargs):
            # --- (FIX 17) Let aiohttp handle pinging ---
            kwargs.setdefault("autoping", True)
            kwargs.setdefault("heartbeat", max(10, PING_INTERVAL))
            # --- END (FIX 17) ---
            kwargs.setdefault("receive_timeout", RECV_TIMEOUT)
            if "timeout" not in kwargs:
                kwargs["timeout"] = aiohttp.ClientTimeout(
                    total=None, sock_read=RECV_TIMEOUT + 5, sock_connect=30
                )

            base_cm = _original_ws_connect(self, *args, **kwargs)

            class _KAContext:
                async def __aenter__(inner_self):
                    ws = await base_cm.__aenter__()
                    wrapper = _KeepAliveWS(ws)
                    # --- (FIX 17) Disabled redundant pinger ---
                    # await wrapper.start()
                    # --- END (FIX 17) ---
                    return wrapper

                async def __aexit__(inner_self, exc_type, exc, tb):
                    return await base_cm.__aexit__(exc_type, exc, tb)

            return _KAContext()

        # Guard to avoid double-patching
        if not getattr(aiohttp.ClientSession.ws_connect, "_fm_keepalive_wrapped", False):
            _ws_connect_with_keepalive._fm_keepalive_wrapped = True
            aiohttp.ClientSession.ws_connect = _ws_connect_with_keepalive


# call this once at import time
patch_aiohttp_ws_keepalive()


# ============================ EMBEDDED MODULE ============================
# cache.py — SQLite HTTP cache + aiohttp monkeypatch (GET only)
import os as _os, sqlite3, time, json as _json, asyncio as _asyncio, zlib
from typing import Optional, Dict, Any, Deque, List

try:
    import aiohttp as _aiohttp
except Exception:
    _aiohttp = None

DEFAULT_DB = _os.path.expanduser(_os.environ.get("FRESH_MONEY_CACHE_DB", "~/fresh_money_cache.sqlite"))
DEFAULT_TTL = int(_os.environ.get("FRESH_MONEY_CACHE_TTL", "30"))  # seconds


class CacheDB:
    def __init__(self, path: str = DEFAULT_DB):
        self.path = _os.path.expanduser(path)
        _os.makedirs(_os.path.dirname(self.path), exist_ok=True)
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
        self.lock = _asyncio.Lock()

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
            headers = _json.loads(headers)
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
                (key, int(time.time()), status, _json.dumps(dict(headers)), comp),
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
        import json as __json
        return __json.loads(self._body.decode("utf-8", errors="replace"))

    def release(self):
        pass


_cache_db: Optional[CacheDB] = None
_original_request = None


def enable_sqlite_cache(db_path: str = DEFAULT_DB, ttl: int = DEFAULT_TTL):
    global _cache_db, _original_request, _aiohttp
    if _aiohttp is None:
        import importlib
        _aiohttp = importlib.import_module("aiohttp")

    if _cache_db is None:
        _cache_db = CacheDB(db_path)

    if _original_request is None:
        _original_request = _aiohttp.ClientSession._request

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

        _aiohttp.ClientSession._request = _cached_request

    return _cache_db


# ============================ PATCHED ADAPTERS ============================
import asyncio as _async
import time as _time
import random as _random
import subprocess, os as _os2
from collections import deque
from pathlib import Path as _Path
import sqlite3 as _sqlite3
from typing import Optional as _Optional

# --- Numeric sanitizers (avoid None -> float() errors) ---
def safe_float(x, default=None):
    try:
        if x is None:
            return default
        return float(x)
    except (TypeError, ValueError):
        return default

def valid_ohlcv_tuple(tup):
    # Expect [ts, o, h, l, c, v]
    if not isinstance(tup, (list, tuple)) or len(tup) < 6:
        return False
    ts, o, h, l, c, v = tup[:6]
    return (ts is not None and safe_float(o) is not None and safe_float(h) is not None
            and safe_float(l) is not None and safe_float(c) is not None and safe_float(v) is not None)

try:
    import aiosqlite
except Exception:
    aiosqlite = None

# --- (FIX 14) Remove __file__ dependency ---
_PINNED_DB  = _os.path.expanduser("~/fresh_money_cache.sqlite")
# --- END (FIX 14) ---


def _ensure_sqlite_file(_path: str):
    try:
        conn = _sqlite3.connect(_path)
        cur = conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT, ts REAL);")
        conn.commit()
    except Exception:
        pass
    finally:
        try:
            conn.close()
        except Exception:
            pass


try:
    _ensure_sqlite_file(_PINNED_DB)
except Exception:
    pass

# ============================ EMBEDDED MODULE (NEW) ============================
# trade_logger.py — SQLite logger for trade outcomes
import aiosqlite
import time

# --- (UPGRADE) New DB Path for Trades ---
TRADE_DB_PATH = _os.path.expanduser("~/fresh_money_trades.sqlite")

class TradeLogger:
    def __init__(self, path: str = TRADE_DB_PATH):
        self.path = path
        self.lock = asyncio.Lock()

    async def init_db(self):
        async with self.lock:
            async with aiosqlite.connect(self.path) as db:
                await db.execute("PRAGMA journal_mode=WAL;")
                await db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS trades (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp REAL NOT NULL,
                        symbol TEXT NOT NULL,
                        timeframe TEXT NOT NULL,
                        side TEXT NOT NULL,
                        entry_price REAL NOT NULL,
                        exit_price REAL NOT NULL,
                        sl_price REAL NOT NULL,
                        tp1_price REAL NOT NULL,
                        pnl_pct REAL NOT NULL,
                        r_net REAL NOT NULL,
                        outcome TEXT NOT NULL,
                        duration_sec INTEGER NOT NULL,
                        conf REAL
                    )
                    """
                )
                await db.commit()
        print(f"[TradeLogger] Database initialized at {self.path}")

    async def save_trade(self, trade_data: dict):
        """
        Saves a closed trade to the SQLite database.
        trade_data = {
            "timestamp", "symbol", "timeframe", "side", "entry_price",
            "exit_price", "sl_price", "tp1_price", "pnl_pct", "r_net",
            "outcome", "duration_sec", "conf"
        }
        """
        async with self.lock:
            try:
                async with aiosqlite.connect(self.path) as db:
                    await db.execute(
                        """
                        INSERT INTO trades (
                            timestamp, symbol, timeframe, side, entry_price, exit_price,
                            sl_price, tp1_price, pnl_pct, r_net, outcome, duration_sec, conf
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            trade_data.get("timestamp"),
                            trade_data.get("symbol"),
                            trade_data.get("timeframe"),
                            trade_data.get("side"),
                            trade_data.get("entry_price"),
                            trade_data.get("exit_price"),
                            trade_data.get("sl_price"),
                            trade_data.get("tp1_price"),
                            trade_data.get("pnl_pct"),
                            trade_data.get("r_net"),
                            trade_data.get("outcome"),
                            trade_data.get("duration_sec"),
                            trade_data.get("conf")
                        ),
                    )
                    await db.commit()
            except Exception as e:
                print(f"[TradeLogger] {type(e)} Error saving trade: {e}")

    async def get_trade_stats(self) -> dict:
        """
Running this script requires the `rich` library. You can install it with `pip install rich`."""
        stats = {
            "total_trades": 0,
            "win_rate": 0.0,
            "avg_win_r": 0.0,
            "avg_loss_r": -1.0, # Default to -1R
            "avg_r": 0.0,
            "expectancy": 0.0
        }
        async with self.lock:
            try:
                async with aiosqlite.connect(self.path) as db:
                    # Get total trades
                    async with db.execute("SELECT COUNT(*), AVG(r_net) FROM trades") as cursor:
                        row = await cursor.fetchone()
                        stats["total_trades"] = row[0] if row and row[0] is not None else 0
                        stats["avg_r"] = row[1] if row and row[1] is not None else 0.0

                    # Get wins
                    async with db.execute("SELECT COUNT(*), AVG(r_net) FROM trades WHERE r_net > 0") as cursor:
                        row = await cursor.fetchone()
                        total_wins = row[0] if row and row[0] is not None else 0
                        stats["avg_win_r"] = row[1] if row and row[1] is not None else 0.0

                    # Get losses
                    async with db.execute("SELECT COUNT(*), AVG(r_net) FROM trades WHERE r_net <= 0") as cursor:
                        row = await cursor.fetchone()
                        total_losses = row[0] if row and row[0] is not None else 0
                        stats["avg_loss_r"] = row[1] if row and row[1] is not None else 0.0
                        
            except Exception as e:
                print(f"[TradeLogger] {type(e)} Error getting stats: {e}")
                return stats # Return defaults on error

        if stats["total_trades"] > 0:
            win_rate = total_wins / stats["total_trades"]
            loss_rate = total_losses / stats["total_trades"]
            stats["win_rate"] = win_rate
            
            # Expectancy = (Win Rate × Average Win) − (Loss Rate × Average Loss)
            # Note: Avg Loss is negative, so we subtract
            stats["expectancy"] = (win_rate * stats["avg_win_r"]) + (loss_rate * stats["avg_loss_r"])
        
        return stats
# --- END EMBEDDED MODULE (NEW) ---


# ========================================================================
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fresh Money Dual Screener — 1h + 4h (Shared Connections) + CHANGE-TRIGGER PRINT + MAC DING
"""

import ccxt.pro as ccxt
import pandas as pd
import numpy as np
from collections import deque
import orjson
import websockets
import time
import asyncio

# --- (UPGRADE) Replaced tabulate/colorama with rich ---
from rich.console import Console
from rich.table import Table
from rich import print
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.align import Align
from rich.text import Text
# --- END (UPGRADE) ---


# --- (MERGE) LIQUIDATION COLLECTOR FUNCTIONS ---
# ============================================================================
# liquidation_collector.py
#
# Connects to free public liquidation streams from exchanges as per
# the "Free-Source Data Map" (CoinGlass-Grade Crypto Analytics... doc).
#
# FIX: Added full keepalive logic to connect_bybit_liquidations
# to handle text-based "ping" and "pong" messages and prevent crashes.
# ============================================================================

# Use combined streams as recommended in the doc
BINANCE_WSS = "wss://fstream.binance.com/ws/!forceOrder@arr"
BYBIT_WSS = "wss://stream.bybit.com/v5/public/linear"

async def connect_binance_liquidations(queue, symbols):
    """
    Connects to Binance '!forceOrder@arr' stream.
    This single stream provides all liquidations.
   
    """
    # Binance expects "BTCUSDT", Bybit expects "BTCUSDT".
    # Our main script sends symbols as "BTCUSDT", "ETHUSDT", etc.
    # We must match against the format Binance sends: "BTCUSDT"
    symbol_set = set(symbols)
    
    while True:
        try:
            async with websockets.connect(BINANCE_WSS) as ws:
                print(f"[LIQ-COLLECTOR] Connected to Binance liquidations stream.")
                while True:
                    data = await ws.recv()
                    payload = orjson.loads(data)
                    
                    if payload and 'e' in payload and payload['e'] == 'forceOrder':
                        liq = payload.get('o', {})
                        symbol = liq.get('s')
                        
                        # Filter for our symbol universe
                        # We match Binance "BTCUSDT" against our set
                        if symbol and symbol in symbol_set:
                            try:
                                # Normalize the data
                                normalized = (
                                    int(liq.get('T', 0)), # Timestamp
                                    symbol, # Send as "BTCUSDT"
                                    liq.get('S', 'UNKNOWN'), # Side (BUY/SELL)
                                    float(liq.get('p', 0.0)), # Price
                                    float(liq.get('q', 0.0))  # Size (original qty)
                                )
                                
                                if normalized[3] > 0 and normalized[4] > 0:
                                    await queue.put(normalized)
                            except Exception:
                                pass # Ignore parse errors on a single liq
        except Exception as e:
            print(f"[LIQ-COLLECTOR] Binance connection error: {e}. Retrying in 20s...")
            await asyncio.sleep(20 + random.uniform(0, 5))

async def connect_bybit_liquidations(queue, symbols):
    """
    Connects to Bybit and subscribes to liquidation topics.
   
    """
    # Bybit needs symbols in "BTCUSDT" format
    symbol_set = set(symbols) 
    
    while True:
        try:
            async with websockets.connect(BYBIT_WSS) as ws:
                # Subscribe to symbols
                args = [f"liquidation.{s}" for s in symbol_set]
                sub_msg = {"op": "subscribe", "args": args}
                await ws.send(orjson.dumps(sub_msg))

                print(f"[LIQ-COLLECTOR] Connected to Bybit liquidations stream.")
                
                last_ping = time.time()
                while True:
                    # Send keepalive ping every 20s
                    if time.time() - last_ping > 20:
                        await ws.send('{"op":"ping"}')
                        last_ping = time.time()

                    try:
                        data_raw = await asyncio.wait_for(ws.recv(), timeout=30.0)
                        
                        # --- START FIX ---
                        # Handle Bybit's text-based keepalive messages
                        if '"op":"ping"' in data_raw:
                            await ws.send('{"op":"pong"}')
                            last_ping = time.time()
                            continue
                        
                        if '"op":"pong"' in data_raw or '"ret_msg":"pong"' in data_raw:
                            last_ping = time.time()
                            continue
                        
                        if data_raw == "pong":
                            last_ping = time.time()
                            continue
                        # --- END FIX ---
                            
                        payload = orjson.loads(data_raw)

                        # --- (FIX 18) Check if payload is a dict ---
                        if isinstance(payload, dict) and payload.get('topic', '').startswith('liquidation.'):
                        # --- END (FIX 18) ---
                            for liq in payload.get('data', []):
                                symbol = liq.get('symbol')
                                if symbol and symbol in symbol_set:
                                    try:
                                        # Normalize the data
                                        normalized = (
                                            int(liq.get('updatedTime', 0)), # Timestamp
                                            symbol, # Send as "BTCUSDT"
                                            liq.get('side', 'UNKNOWN'), # Side (Buy/Sell)
                                            float(liq.get('price', 0.0)),
                                            float(liq.get('size', 0.0))
                                        )
                                        if normalized[3] > 0 and normalized[4] > 0:
                                            await queue.put(normalized)
                                    except Exception:
                                        pass # Ignore parse errors
                    
                    except asyncio.TimeoutError:
                        print(f"[LIQ-COLLECTOR] Bybit timeout, sending ping...")
                        await ws.send('{"op":"ping"}') # Send ping on timeout
                        last_ping = time.time()
                    except Exception as e:
                        # This will catch JSON parse errors on "pong" etc.
                        if "readObjectStart" not in str(e) and "'str' object has no attribute 'get'" not in str(e):
                            print(f"[LIQ-COLLECTOR] Bybit inner loop error: {e}")
                        pass # Ignore and continue loop

        except Exception as e:
            print(f"[LIQ-COLLECTOR] Bybit connection error: {e}. Retrying in 20s...")
            await asyncio.sleep(20 + random.uniform(0, 5))
# --- END (MERGE) LIQUIDATION COLLECTOR ---


async def get_funding_rate(ex, symbol):
    try:
        if getattr(ex, "has", {}).get("fetchFundingRate", False):
            fr = await ex.fetch_funding_rate(symbol)
            return float(fr.get("fundingRate", 0.0) or 0.0)
    except Exception:
        pass
    return 0.0

EXCHANGES_CFG = {
    "binanceusdm": {"oi_stream": False, "vol_stream": True,  "cvd_stream": False, "defaultType": "future"},
    "bybit":       {"oi_stream": True,  "vol_stream": True,  "cvd_stream": True,  "defaultType": "future"},
    "bitget":      {"oi_stream": True,  "vol_stream": True,  "cvd_stream": False, "defaultType": "swap"},
    "mexc":        {"oi_stream": True,  "vol_stream": True,  "cvd_stream": False, "defaultType": "swap"},
}

SUBSCRIPTION_DELAY = 1.0
PRINT_INTERVAL     = 5 # (UPGRADE) Shorted print interval for a more responsive UI

# --- Signal stability controls (hysteresis + hold + cooldown) ---
OPEN_THR_1H   = 2.0
CLOSE_THR_1H  = 1.4
OPEN_THR_4H   = 2.4
CLOSE_THR_4H  = 1.8
MIN_HOLD_SEC  = 60
COOLDOWN_SEC  = 180

# --- Change-triggered printing & mac sound (shared) ---
ENABLE_SOUND         = True
SOUND_PATH           = "/System/Library/Sounds/Glass.aiff"
SOUND_RATE_LIMIT_SEC = 2.0
_LAST_SOUND_TS       = 0.0


def _mac_ding():
    global _LAST_SOUND_TS
    if not ENABLE_SOUND or not _os2.path.exists(SOUND_PATH):
        return
    now = _time.time()
    if now - _LAST_SOUND_TS < SOUND_RATE_LIMIT_SEC:
        return
    try:
        subprocess.Popen(["afplay", SOUND_PATH], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        _LAST_SOUND_TS = now
    except Exception:
        pass


def _fingerprint_top_dual(rows: list[dict]) -> tuple:
    # Handle None in TPs for fingerprinting
    return tuple(
        (
            r["symbol"], round(r["score"], 3), round(r["entry"], 6),
            round(r["tp1"], 6),
            round(r["tp2"], 6) if r.get("tp2") is not None else -1,
            round(r["tp3"], 6) if r.get("tp3") is not None else -1,
            r.get("confluence", False) # Add confluence to fingerprint
        )
        for r in rows[:5]
    )


class ScreenerConfig:
    def __init__(
        self, name, timeframe, backfill, z_period, fast_ma, slow_ma,
        min_quote_vol, gate_z, oi_w=0.4, cvd_w=0.4, vol_w=0.2,
        risk_mult=0.6, sl_buf=0.2, tp2_mul=2.0, tp3_mul=3.0,
    ):
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
        self.tp2_mul = tp2_mul # Now used as fallback multiplier
        self.tp3_mul = tp3_mul # Now used as fallback multiplier
        
        # --- (FIX 12) Staleness Check ---
        tf_map = {"1h": 3600, "4h": 3600 * 4, "1m": 60}
        self.tf_seconds = tf_map.get(timeframe, 3600)
        self.staleness_threshold_ms = (self.tf_seconds * 1000) * 3 # 3 intervals
        # --- END (FIX 12) ---


CFG_1H = ScreenerConfig("FAST 1h","1h",100,20,5,20,1_000_000,0.52, risk_mult=0.6, sl_buf=0.20, tp2_mul=1.5, tp3_mul=1.5)
CFG_4H = ScreenerConfig("SAFE 4h","4h",120,20,5,20,2_000_000,0.65, risk_mult=0.8, sl_buf=0.25, tp2_mul=1.5, tp3_mul=1.5)


def calc_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="float64")
    d = df[["high", "low", "close"]].copy()
    d["h-l"] = d["high"] - d["low"]
    d["h-pc"] = (d["high"] - d["close"].shift(1)).abs()
    d["l-pc"] = (d["low"] - d["close"].shift(1)).abs()
    tr = d[["h-l", "h-pc", "l-pc"]].max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()


def vol_profile(df: pd.DataFrame, bins: int = 50):
    if df.empty:
        return None
    pmin = float(df["low"].min()); pmax = float(df["high"].max())
    if not np.isfinite(pmin) or not np.isfinite(pmax) or pmax <= pmin:
        return None
    step = (pmax - pmin) / bins
    edges = np.arange(pmin, pmax + step, step)
    vols = np.zeros(len(edges))
    for _, r in df.iterrows():
        lo, hi, v = float(r["low"]), float(r["high"]), float(r["volume"])
        if not np.isfinite(lo) or not np.isfinite(hi) or hi <= lo:
            continue
        i0 = np.searchsorted(edges, lo, "left")
        i1 = np.searchsorted(edges, hi, "right") - 1
        i0 = max(0, min(i0, len(edges) - 1))
        i1 = max(0, min(i1, len(edges) - 1))
        span = max(1, i1 - i0 + 1)
        vols[i0 : i1 + 1] += v / span
    poc_idx = int(np.argmax(vols)); poc = float(edges[poc_idx])
    tgt = float(vols.sum() * 0.7)
    L = R = poc_idx; acc = float(vols[poc_idx])
    while acc < tgt:
        lv = vols[L - 1] if L - 1 >= 0 else -np.inf
        rv = vols[R + 1] if R + 1 < len(vols) else -np.inf
        if lv >= rv and np.isfinite(lv):
            L -= 1; acc += lv
        elif np.isfinite(rv):
            R += 1; acc += rv
        else:
            break
    val = float(edges[max(0, L)]); vah = float(edges[min(R, len(edges) - 1)])
    avg = float(vols.sum() / bins) if bins else 0.0
    hvns = [float(edges[i]) for i in range(1, len(vols)-1) if vols[i] > vols[i-1] and vols[i] > vols[i+1] and vols[i] > 1.5 * avg]
    hvns = sorted([x for x in hvns if x > vah or x < val])
    return {"poc": poc, "vah": vah, "val": val, "hvns": hvns}


# --- (UPGRADE) New TradeManager Class ---
# This class will hold the state of all trades from all screeners
class TradeManager:
    def __init__(self, trade_logger: TradeLogger):
        self.trade_logger = trade_logger
        self.live_trades: Dict[str, dict] = {} # Keyed by symbol
        self.closed_trades: Deque[dict] = deque(maxlen=20) # For "Recent Closes"
        self.stats: dict = {}
        self.lock = asyncio.Lock()
        self.console = Console() # For logging trade closures
        self.last_sound_ts = 0.0

    async def init(self):
        """Initialize the manager by loading stats."""
        await self.update_stats()

    async def update_stats(self):
        """Fetches the latest trade stats from the database."""
        async with self.lock:
            self.stats = await self.trade_logger.get_trade_stats()

    def _get_rr(self, entry: float, sl: float, tp: float) -> float:
        """Helper to calculate Risk/Reward ratio."""
        risk = entry - sl
        reward = tp - entry
        if risk <= 0:
            return 0.0
        return reward / risk

    async def process_signal(self, screener_name: str, timeframe: str, signal: dict):
        """
        Called by a Screener when it identifies a valid signal.
        This function decides whether to create a new "LIVE" trade.
        """
        symbol = signal["symbol"]
        async with self.lock:
            if symbol not in self.live_trades:
                # --- This is a NEW trade ---
                
                # Filter for high-quality trades (as per user request)
                conf = signal["score"]
                rr = self._get_rr(signal["entry"], signal["sl"], signal["tp1"])
                
                # --- (UPGRADE) Gatekeeping for "Live Trade Log" ---
                # Set minimum R:R and Confidence (Score) here
                if rr < 1.0 or conf < (OPEN_THR_1H if "1h" in timeframe else OPEN_THR_4H):
                    return # Signal is valid but doesn't meet our criteria for logging
                
                # --- DING ---
                now = time.time()
                if now - self.last_sound_ts > SOUND_RATE_LIMIT_SEC:
                    _mac_ding()
                    self.last_sound_ts = now
                # --- END DING ---

                trade = {
                    "open_ts": int(time.time()),
                    "symbol": symbol,
                    "side": "LONG", # This strategy is LONG only
                    "tf": timeframe,
                    "conf": conf,
                    "entry": signal["entry"],
                    "sl": signal["sl"],
                    "tp1": signal["tp1"],
                    "tp2": signal["tp2"],
                    "tp3": signal["tp3"],
                    "current_price": signal["price"],
                    "outcome": "LIVE",
                    "screener": screener_name
                }
                self.live_trades[symbol] = trade
                self.console.print(f"🔔 [bold green]NEW LIVE TRADE[/]: {symbol} ({timeframe}) | Entry: {trade['entry']:.4f} | SL: {trade['sl']:.4f} | TP1: {trade['tp1']:.4f}")

    async def remove_invalid_signal(self, screener_name: str, symbol: str):
        """
        Called by a Screener when a signal becomes invalid (e.g., score drops,
        cooldown, etc.)
        """
        async with self.lock:
            if symbol in self.live_trades:
                # Check if this trade belongs to the screener that is invalidating it
                if self.live_trades[symbol]["screener"] == screener_name:
                    trade = self.live_trades.pop(symbol)
                    
                    # Log this as a "Fail" or "Invalid"
                    closed_trade = await self._close_trade(
                        trade=trade, 
                        exit_price=trade["current_price"], # Close at last known price
                        outcome="Fail"
                    )
                    self.closed_trades.appendleft(closed_trade)
                    self.console.print(f"❌ [bold yellow]SIGNAL INVALIDATED[/]: {symbol} ({trade['tf']}) | Score dropped. Closing trade as 'Fail'.")
                    await self.update_stats()


    async def update_live_price(self, symbol: str, price: float):
        """
        Called by a Screener when it receives a new price for a symbol.
        This is the core monitoring loop.
        """
        if symbol not in self.live_trades:
            return

        async with self.lock:
            # Re-check to avoid race condition
            if symbol not in self.live_trades:
                return
                
            trade = self.live_trades[symbol]
            trade["current_price"] = price # Always update the price
            
            outcome = None
            exit_price = None

            # Check for SL hit
            if price <= trade["sl"]:
                outcome = "SL"
                exit_price = trade["sl"] # Assume SL fill at SL price
            
            # Check for TP1 hit
            elif price >= trade["tp1"]:
                outcome = "TP1"
                exit_price = trade["tp1"] # Assume TP fill at TP1 price

            if outcome and exit_price:
                # --- Trade is Closed ---
                # Remove from live dict
                closed_trade = self.live_trades.pop(symbol)
                
                # Finalize and log
                final_trade_log = await self._close_trade(closed_trade, exit_price, outcome)
                self.closed_trades.appendleft(final_trade_log)
                
                if outcome == "TP1":
                    self.console.print(f"✅ [bold green]TRADE CLOSED (TP1)[/]: {symbol} ({trade['tf']}) | PnL: {final_trade_log['pnl_pct']:.2f}% | R: +{final_trade_log['r_net']:.2f}R")
                else:
                    self.console.print(f"🛑 [bold red]TRADE CLOSED (SL)[/]: {symbol} ({trade['tf']}) | PnL: {final_trade_log['pnl_pct']:.2f}% | R: {final_trade_log['r_net']:.2f}R")
                
                # Update stats for the dashboard
                await self.update_stats()

    async def _close_trade(self, trade: dict, exit_price: float, outcome: str) -> dict:
        """Internal helper to calculate PnL, R-unit, and save to DB."""
        
        entry_price = trade["entry"]
        sl_price = trade["sl"]
        
        # Calculate PnL %
        # (exit_price / entry_price) - 1
        pnl_pct = 0.0
        if entry_price > 0:
            pnl_pct = ((exit_price / entry_price) - 1) * 100 # For LONG
            
        # Calculate R-unit
        r_net = 0.0
        initial_risk_per_unit = entry_price - sl_price
        pnl_per_unit = exit_price - entry_price
        
        if initial_risk_per_unit > 0:
            r_net = pnl_per_unit / initial_risk_per_unit
        
        # Ensure SL hit is always -1.0R (accounts for slippage modeling)
        if outcome == "SL":
            r_net = -1.0
            pnl_pct = ((sl_price / entry_price) - 1) * 100

        # Ensure "Fail" is -1.0R, as it's a failed trade
        if outcome == "Fail":
            r_net = -1.0 

        now = int(time.time())
        duration = now - trade["open_ts"]
        
        log_entry = {
            "timestamp": now,
            "symbol": trade["symbol"],
            "timeframe": trade["tf"],
            "side": "LONG",
            "entry_price": entry_price,
            "exit_price": exit_price,
            "sl_price": sl_price,
            "tp1_price": trade["tp1"],
            "pnl_pct": pnl_pct,
            "r_net": r_net,
            "outcome": outcome,
            "duration_sec": duration,
            "conf": trade["conf"]
        }
        
        # Save to database
        await self.trade_logger.save_trade(log_entry)
        
        # Return a simplified dict for the "Recent Closes" table
        return {
            "close_ts": now,
            "symbol": trade["symbol"],
            "side": "LONG",
            "tf": trade["tf"],
            "entry": entry_price,
            "exit": exit_price,
            "pnl_pct": pnl_pct,
            "r_net": r_net,
            "outcome": outcome,
            "duration": f"{duration // 60}m{duration % 60}s"
        }

    # --- (UPGRADE) Functions to provide data to the dashboard ---
    async def get_live_trades(self) -> List[dict]:
        async with self.lock:
            return sorted(self.live_trades.values(), key=lambda x: x["open_ts"], reverse=True)
    
    async def get_closed_trades(self) -> List[dict]:
        async with self.lock:
            return list(self.closed_trades) # Returns a copy

    async def get_stats(self) -> dict:
        async with self.lock:
            return self.stats.copy() # Returns a copy
# --- END (UPGRADE) TradeManager Class ---


class Screener:
    # --- (UPGRADE) Modified __init__ ---
    def __init__(self, cfg: ScreenerConfig, exchanges: dict, symbols: list[str], liquidations_storage: dict, trade_manager: TradeManager):
        self.cfg = cfg
        self.exchanges = exchanges
        self.symbols = symbols
        self.state = {}
        self.top = [] # This is now just a "signal list", not the final log
        self.lock = _async.Lock()
        self.last_sig = None
        self.liquidations = liquidations_storage 
        self._signals = {}
        self._locked_levels = {} 
        self.trade_manager = trade_manager # (UPGRADE) Link to the manager
    # --- END (UPGRADE) ---

    # --- PHASE 2: NEW FUNCTION ---
    def check_liq_confluence(self, symbol: str, tp1_price: float) -> bool:
        """
        Checks for a recent, large liquidation near the TP1 price.
        This provides "confluence" for the trade.
        """
        try:
            # Config: 4-hour window, 1% proximity, $10k notional threshold
            window_sec = 3600 * 4 
            proximity_pct = 0.01 
            min_notional_threshold = 10000 
            
            now_ms = int(_time.time() * 1000)
            window_start_ms = now_ms - (window_sec * 1000)
            
            symbol_liqs = self.liquidations.get(symbol)
            if not symbol_liqs:
                return False
                
            price_min = tp1_price * (1 - proximity_pct)
            price_max = tp1_price * (1 + proximity_pct)
            
            # Check recent liquidations (deques are fast at iterating)
            for i in range(len(symbol_liqs) - 1, -1, -1):
                (timestamp, price, size) = symbol_liqs[i]
                
                # Stop checking if data is too old
                if timestamp < window_start_ms:
                    break 
                
                # Check if the liquidation is within our price target zone
                if price_min <= price <= price_max:
                    if (price * size) > min_notional_threshold:
                        return True # Found a single large liquidation
                        
            return False # No large liquidations found in the zone
        except Exception:
            return False # Fail safe
    # --- END PHASE 2 ---

    async def backfill_symbol(self, ex, symbol: str):
        try:
            ohlcv = await ex.fetch_ohlcv(symbol, timeframe=self.cfg.timeframe, limit=self.cfg.backfill + 2)
            if not ohlcv or len(ohlcv) < 10:
                return None
            df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
            df.set_index("timestamp", inplace=True)

            oi_series = None
            if getattr(ex, "has", {}).get("fetchOpenInterestHistory", False):
                try:
                    oi_data = await ex.fetch_open_interest_history(symbol, timeframe=self.cfg.timeframe, limit=self.cfg.backfill + 2)
                    if oi_data:
                        oi_df = pd.DataFrame(oi_data)
                        if "timestamp" in oi_df and "openInterestValue" in oi_df:
                            oi_df["timestamp"] = pd.to_datetime(oi_df["timestamp"], unit="ms")
                            oi_df.set_index("timestamp", inplace=True)
                            oi_series = (
                                oi_df["openInterestValue"].astype(float)
                                .resample(self.cfg.timeframe, label="right", closed="right").last()
                            )
                except Exception:
                    oi_series = None
            if oi_series is None:
                oi_series = pd.Series(index=df.index, data=np.nan, dtype="float64")
            df["openInterestValue"] = oi_series.reindex(df.index).ffill()

            df["cvd"] = 0.0
            df["funding"] = 0.0 
            df["last_oi_update"] = 0 
            df["last_cvd_update"] = 0
            
            df = df[["open", "high", "low", "close", "volume", "openInterestValue", "cvd", "funding", "last_oi_update", "last_cvd_update"]].dropna() 
            return df
        except Exception as e:
            print(f"[{self.cfg.name}] Backfill error {ex.id} {symbol}: {e}")
            return None

    async def initialize(self):
        print(f"[{self.cfg.name}] Backfilling {self.cfg.backfill} bars per venue…")
        sem = _async.Semaphore(10)

        async def agg_one(symbol: str):
            async with sem:
                dfs = []
                for ex_id in EXCHANGES_CFG.keys():
                    pass
                for ex_id in EXCHANGES_CFG.keys():
                    ex = self.exchanges[ex_id]
                    # We pass the full `ex` object to backfill_symbol
                    df = await self.backfill_symbol(ex, symbol) 
                    if df is not None and not df.empty:
                        df = df.copy()
                        df[f"vol_{ex_id}"] = df["volume"]
                        dfs.append(df)
                if not dfs:
                    return
                base = dfs[0].copy()
                for add in dfs[1:]:
                    base = base.join(add[["volume", "openInterestValue"]], how="outer", rsuffix="_r")
                    base["volume"] = base[["volume", "volume_r"]].sum(axis=1, skipna=True)
                    base.drop(columns=["volume_r"], inplace=True)
                    base["openInterestValue"] = base[["openInterestValue", "openInterestValue_r"]].sum(axis=1, skipna=True)
                    base.drop(columns=["openInterestValue_r"], inplace=True)
                
                base = base[["open","high","low","close","volume","openInterestValue","cvd", "funding", "last_oi_update", "last_cvd_update"]].sort_index().ffill().dropna()
                if base["volume"].iloc[-1] < self.cfg.min_quote_vol:
                    return
                self.state[symbol] = {
                    "lock": _async.Lock(),
                    "last_update": int(base.index[-1].value // 1_000_000),
                    "df_deque": deque(
                        [{
                            "timestamp": int(ts.value // 1_000_000),
                            "open": float(row["open"]),
                            "high": float(row["high"]),
                            "low": float(row["low"]),
                            "close": float(row["close"]),
                            "volume": float(row["volume"]),
                            "openInterestValue": float(row["openInterestValue"]),
                            "cvd": float(row.get("cvd", 0.0)),
                            "funding": float(row.get("funding", 0.0)),
                            "last_oi_update": 0, 
                            "last_cvd_update": 0
                        } for ts, row in base.tail(self.cfg.backfill).iterrows()],
                        maxlen=self.cfg.backfill + 5,
                    ),
                }
                self._signals[symbol] = {"active": False, "since": 0, "cooldown_until": 0}
        
        await _async.gather(*[agg_one(s) for s in self.symbols])
        kept = len(self.state)
        print(f"[{self.cfg.name}] Initialized {kept} symbols")
        # Trigger one immediate ranking pass so early prints aren't biased by subscription order
        try:
            await _async.gather(*[self.update_and_rank(s) for s in self.state.keys()])
        except Exception:
            pass
        return kept > 0

    async def update_and_rank(self, symbol: str):
        try:
            async with self.state[symbol]["lock"]:
                dq = self.state[symbol]["df_deque"]
                df = pd.DataFrame(dq)
                if df.empty or len(df) < max(self.cfg.z_period, self.cfg.slow_ma) + 1:
                    return

                passes = True # Start with passes = True
                
                # --- (FIX 12) DATA STALENESS CHECK ---
                now_ms = int(_time.time() * 1000)
                last_oi_update = df["last_oi_update"].iloc[-1]
                last_cvd_update = df["last_cvd_update"].iloc[-1]
                
                # Check if data is stale (and not 0, which means just initialized)
                if (now_ms - last_oi_update) > self.cfg.staleness_threshold_ms and last_oi_update != 0:
                    passes = False # OI data is stale
                if (now_ms - last_cvd_update) > self.cfg.staleness_threshold_ms and last_cvd_update != 0:
                    passes = False # CVD data is stale
                # --- END (FIX 12) ---
                
                vol_mean = df["volume"].rolling(self.cfg.z_period).mean().iloc[-1]
                vol_std  = df["volume"].rolling(self.cfg.z_period).std().iloc[-1]
                vol_z = (df["volume"].iloc[-1] - vol_mean) / (vol_std if vol_std else 1.0)
                
                # --- (FIX 10) EXHAUSTION FILTER ---
                if vol_z > 3.0:
                    passes = False # Invalidate trade
                # --- END (FIX 10) ---

                # --- (FIX 11) FUNDING RATE FILTER ---
                last_funding_rate = safe_float(df["funding"].iloc[-1], 0.0)
                # Invalidate if funding is extreme ( > 0.75% or < -0.75% )
                if abs(last_funding_rate) > 0.0075:
                    passes = False # Invalidate trade
                # --- END (FIX 11) ---

                oi_mean = df["openInterestValue"].rolling(self.cfg.z_period).mean().iloc[-1]
                oi_std  = df["openInterestValue"].rolling(self.cfg.z_period).std().iloc[-1]
                oi_z = (df["openInterestValue"].iloc[-1] - oi_mean) / (oi_std if oi_std else 1.0)
                
                cvd_fast = df["cvd"].rolling(self.cfg.fast_ma).mean().iloc[-1]
                cvd_slow = df["cvd"].rolling(self.cfg.slow_ma).mean().iloc[-1]
                cvd_accel = cvd_fast - cvd_slow

                cvd_acc_series = (
                    df["cvd"].rolling(self.cfg.z_period).apply(
                        lambda x: (pd.Series(x).rolling(self.cfg.fast_ma).mean().iloc[-1]
                                   - pd.Series(x).rolling(self.cfg.slow_ma).mean().iloc[-1]), raw=False
                    ).dropna()
                )
                mu = float(cvd_acc_series.mean()) if len(cvd_acc_series) else 0.0
                sd = float(cvd_acc_series.std())  if len(cvd_acc_series) else 1.0
                cvd_acc_z = (cvd_accel - mu) / (sd if sd else 1.0)

                # Update `passes` check
                passes = passes and (oi_z > self.cfg.gate_z) and (cvd_acc_z > self.cfg.gate_z)
                
                score  = self.cfg.oi_w * oi_z + self.cfg.cvd_w * cvd_acc_z + self.cfg.vol_w * vol_z
                if not np.isfinite(score) or (passes is False and score > 0):
                    score = -1e9 # Set score to minimum if it fails any pass

                trade = None
                
                # --- (FIX 13) POC LOCK ---
                # Check if we should use locked levels for an active signal
                locked = self._locked_levels.get(symbol)
                if locked and self._signals.get(symbol, {}).get("active"):
                    vp = locked # Use the locked profile
                else:
                    vp = vol_profile(df.tail(self.cfg.z_period)) # Calculate new profile
                    locked = None # Ensure locked is None if not active
                # --- END (FIX 13) ---

                if passes and vp:
                    atr = calc_atr(df, 14)
                    if len(atr) and np.isfinite(atr.iloc[-1]) and atr.iloc[-1] > 0:
                        atr_v = float(atr.iloc[-1])
                        
                        # --- (FIX 13) Use locked or new levels ---
                        if locked:
                            poc, vah, val = locked["poc"], locked["vah"], locked["val"]
                            hvn_up = locked["hvn_up"]
                        else:
                            poc, vah, val = vp["poc"], vp["vah"], vp["val"]
                            hvn_up = [h for h in vp["hvns"] if h > vah] # Get HVNs *before* lock
                        # --- END (FIX 13) ---

                        entry = float(poc)
                        risk = atr_v * self.cfg.risk_mult 
                        sl = max(0.0, float(val) - self.cfg.sl_buf * atr_v)
                        tp1 = float(vah)
                        
                        has_confluence = self.check_liq_confluence(symbol, tp1)
                        
                        # Calculate TP2
                        if hvn_up:
                            tp2 = float(hvn_up[0])
                        else:
                            tp2 = tp1 + (risk * self.cfg.tp2_mul) 
                        
                        # Calculate TP3
                        if len(hvn_up) > 1:
                            tp3 = float(hvn_up[1])
                        else:
                            tp3 = tp2 + (risk * self.cfg.tp3_mul)
                        
                        if tp1 > entry > sl > 0:
                            trade = {
                                "price": float(df["close"].iloc[-1]),
                                "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3,
                                "confluence": has_confluence, 
                                "poc": poc, "vah": vah, "val": val, "hvn_up": hvn_up
                            }

                # *** FIX 3 & 4: Stale Trade Invalidation ***
                if trade:
                    current_price = trade["price"]
                    sl_price = trade["sl"]
                    tp1_price = trade["tp1"]

                    if current_price < sl_price:
                        trade = None 
                        passes = False
                    
                    elif current_price > tp1_price:
                        trade = None 
                        passes = False

                    elif (tp1_price - entry) > 0: 
                        move_pct = (current_price - entry) / (tp1_price - entry)
                        if move_pct > 0.40: 
                            trade = None
                            passes = False
                # *** END FIX 3 & 4 ***

            # --- (UPGRADE) Modified Signal Logic ---
            async with self.lock:
                now = int(_time.time())
                sig = self._signals.get(symbol) or {"active": False, "since": 0, "cooldown_until": 0}

                # 1. Define thresholds first
                if "1h" in self.cfg.timeframe:
                    open_thr, close_thr = OPEN_THR_1H, CLOSE_THR_1H
                else:
                    open_thr, close_thr = OPEN_THR_4H, CLOSE_THR_4H
                
                # 2. Check for cooldown
                if now < sig.get("cooldown_until", 0):
                    passes = False

                # 3. Apply POC Lock and signal logic
                if not sig["active"]:
                    if passes and score >= open_thr:
                        sig["active"] = True
                        sig["since"] = now
                        # LOCK the levels
                        if trade:
                            self._locked_levels[symbol] = {
                                "poc": trade["poc"], "vah": trade["vah"], "val": trade["val"], "hvn_up": trade["hvn_up"]
                            }
                    else:
                        passes = False
                        if symbol in self._locked_levels:
                            del self._locked_levels[symbol] # Unlock
                else: # Signal is already active
                    if now - sig.get("since", now) < MIN_HOLD_SEC:
                        passes = True # Enforce hold time
                    elif score < close_thr:
                        sig["active"] = False
                        sig["cooldown_until"] = now + COOLDOWN_SEC
                        passes = False
                        if symbol in self._locked_levels:
                            del self._locked_levels[symbol] # Unlock
                    # else: signal stays active (passes=True)
                        
                self._signals[symbol] = sig
                
                # --- (UPGRADE) Send signal to TradeManager ---
                if passes and trade:
                    # This signal is active and valid
                    signal_data = {
                        "symbol": symbol, "score": float(score),
                        "oi_z": float(oi_z), "cvd_z": float(cvd_acc_z), "vol_z": float(vol_z),
                        "confluence": trade.get("confluence", False), 
                        **trade
                    }
                    await self.trade_manager.process_signal(self.cfg.name, self.cfg.timeframe, signal_data)
                
                elif not passes and sig["active"] is False:
                    # Signal has just become invalid, tell manager
                    await self.trade_manager.remove_invalid_signal(self.cfg.name, symbol)
                # --- END (UPGRADE) ---

        except Exception as e:
            print(f"[{self.cfg.name}] update_and_rank {symbol}: {e}")

    async def watch_candles(self, ex, symbol: str):
        retry = 5.0
        while True:
            try:
                candles = await ex.watch_ohlcv(symbol, self.cfg.timeframe)  # -> [ts,o,h,l,c,v]
                if symbol not in self.state:
                    continue
                    
                # (UPGRADE) Get price from the candle
                live_price = None
                
                async with self.state[symbol]["lock"]:
                    dq = self.state[symbol]["df_deque"]
                    if not dq or len(candles) < 2:
                        continue

                    raw = candles[-2]
                    if not valid_ohlcv_tuple(raw):
                        continue
                    t, o, h, l, c, v = raw
                    now_ms = int(_time.time() * 1000) # (FIX 12)
                    
                    live_price = safe_float(c, default=None) # (UPGRADE)

                    last_ts = dq[-1]["timestamp"]
                    if t > last_ts:
                        last = dq[-1] if dq else {}
                        dq.append({
                            "timestamp": int(t),
                            "open":  safe_float(o, default=last.get("close", 0.0)),
                            "high":  safe_float(h, default=last.get("high", 0.0)),
                            "low":   safe_float(l, default=last.get("low", 0.0)),
                            "close": safe_float(c, default=last.get("close", 0.0)),
                            "volume": safe_float(v, default=0.0),
                            "openInterestValue": last.get("openInterestValue", 0.0),
                            "cvd": last.get("cvd", 0.0), 
                            "funding": last.get("funding", 0.0),
                            # (FIX 12) Carry over staleness timestamps
                            "last_oi_update": last.get("last_oi_update", 0),
                            "last_cvd_update": last.get("last_cvd_update", 0)
                        })
                        self.state[symbol]["last_update"] = int(t)
                        _async.create_task(self.update_and_rank(symbol))
                    else:
                        dq[-1]["volume"] = safe_float(v, default=dq[-1].get("volume", 0.0))
                
                # --- (UPGRADE) ---
                # After lock, update the trade manager with the new price
                if live_price is not None:
                    await self.trade_manager.update_live_price(symbol, live_price)
                # --- END (UPGRADE) ---

                retry = 5.0
            except Exception as e:
                print(f"[{self.cfg.name}] {ex.id} candles {symbol}: {e}")
                await _async.sleep(retry + _random.uniform(0, 3))
                retry = min(retry * 2, 300)

    async def watch_oi(self, ex, symbol: str):
        if not getattr(ex, "has", {}).get("watchOpenInterest", False):
            return
        retry = 5.0
        while True:
            try:
                oi = await ex.watch_open_interest(symbol)
                if symbol not in self.state:
                    continue
                async with self.state[symbol]["lock"]:
                    dq = self.state[symbol]["df_deque"]
                    if dq:
                        val = float(oi.get("openInterestValue") or oi.get("openInterestUsd", 0.0) or 0.0)
                        dq[-1]["openInterestValue"] = max(dq[-1].get("openInterestValue", 0.0), val)
                        dq[-1]["last_oi_update"] = int(_time.time() * 1000) # (FIX 12)
                retry = 5.0
            except Exception:
                await _async.sleep(retry + _random.uniform(0, 3))
                retry = min(retry * 2, 300)

    async def watch_funding(self, ex, symbol: str):
        if not getattr(ex, "has", {}).get("watchTicker", False):
            return
        retry = 5.0
        while True:
            try:
                ticker = await ex.watch_ticker(symbol)
                if symbol not in self.state:
                    continue
                
                fr = ticker.get('fundingRate')
                if fr is not None:
                    async with self.state[symbol]["lock"]:
                        dq = self.state[symbol]["df_deque"]
                        if dq:
                            dq[-1]["funding"] = safe_float(fr, 0.0)
                retry = 5.0
            except Exception:
                await _async.sleep(retry + _random.uniform(0, 3))
                retry = min(retry * 2, 300)

    async def watch_trades(self, ex, symbol: str):
        if not getattr(ex, "has", {}).get("watchTrades", False):
            return
        retry = 5.0
        while True:
            try:
                trades = await ex.watch_trades(symbol)
                if symbol not in self.state:
                    continue
                
                live_price = None

                async with self.state[symbol]["lock"]:
                    dq = self.state[symbol]["df_deque"]
                    if not dq:
                        continue
                    
                    if trades:
                        live_price = safe_float(trades[-1].get("price"), default=None) # (UPGRADE)
                
                    base = float(dq[-1].get("cvd", 0.0))
                    for t in trades:
                        amt = float(t.get("amount", 0.0) or 0.0)
                        side = (t.get("side") or "").lower()
                        info = t.get("info") or {}

                        taker_signed = None
                        if "m" in info or "isBuyerMaker" in info:
                            is_buyer_maker = info.get("m", info.get("isBuyerMaker"))
                            if isinstance(is_buyer_maker, str):
                                is_buyer_maker = is_buyer_maker.lower() in ("true","1","yes")
                            if is_buyer_maker is True:
                                taker_signed = -amt 
                            elif is_buyer_maker is False:
                                taker_signed = +amt 
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
                    dq[-1]["last_cvd_update"] = int(_time.time() * 1000) # (FIX 12)
                
                # --- (UPGRADE) ---
                if live_price is not None:
                    await self.trade_manager.update_live_price(symbol, live_price)
                
                if trades and trades[-1].get("timestamp", 0) > self.state[symbol]["last_update"]:
                    _async.create_task(self.update_and_rank(symbol))
                # --- END (UPGRADE) ---

                retry = 5.0
            except Exception:
                await _async.sleep(retry + _random.uniform(0, 3))
                retry = min(retry * 2, 300)

    async def subscribe_all(self):
        tasks = []
        syms = list(self.symbols)
        _random.shuffle(syms)
        for symbol in syms:
            if symbol not in self.state:
                continue
            for ex_id, cfg in EXCHANGES_CFG.items():
                ex = self.exchanges[ex_id]
                if getattr(ex, "has", {}).get("watchOHLCV", False):
                    tasks.append(_async.create_task(self.watch_candles(ex, symbol)))
                if cfg["oi_stream"] and getattr(ex, "has", {}).get("watchOpenInterest", False):
                    tasks.append(_async.create_task(self.watch_oi(ex, symbol)))
                
                if ex_id == "bybit" and getattr(ex, "has", {}).get("watchTicker", False):
                     tasks.append(_async.create_task(self.watch_funding(ex, symbol)))
                     
                if cfg["cvd_stream"] and ex_id == "bybit":
                    tasks.append(_async.create_task(self.watch_trades(ex, symbol)))
                
                # --- (FIX) for Bitget rate-limit ---
                # Stagger subscriptions *per exchange*
                await _async.sleep(0.1) 
                # --- END (FIX) ---
                
            await _async.sleep(SUBSCRIPTION_DELAY) # This is now per-symbol
        print(f"[{self.cfg.name}] Subscriptions ready. Streams running: {len(tasks)}")
        await _async.gather(*tasks)

    # --- (UPGRADE) Removed old `printer` function ---
    # The new dashboard is a standalone function below


async def init_exchanges():
    exchanges = {}
    for ex_id, c in EXCHANGES_CFG.items():
        klass = getattr(ccxt, ex_id)
        ex = klass({"enableRateLimit": True, "options": {"defaultType": c["defaultType"]}})
        await ex.load_markets()
        exchanges[ex_id] = ex
        print(f"Loaded markets: {ex_id} → {len(ex.markets)}")
    return exchanges


async def common_usdt_symbols(exchanges: dict) -> list[str]:
    universe_per_ex = []
    for ex in exchanges.values():
        syms = [
            s for s, m in ex.markets.items()
            if isinstance(s, str) and "/USDT" in s and m.get("active", True)
        ]
        universe_per_ex.append(set(syms))
    common = set.intersection(*universe_per_ex) if universe_per_ex else set()
    
    print(f"Common /USDT symbols across venues: {len(common)}")
    lst = sorted(common)
    
    # Shuffle to avoid early alphabetical bias during subscription warmup
    _random.shuffle(lst)
    return lst

# --- PHASE 2: NEW LIQUIDATION CONSUMER ---
async def liquidation_consumer(queue: _async.Queue, storage: dict):
    """
    Consumes liquidations from the queue and stores them
    in a per-symbol rolling deque.
    """
    print(f"[MAIN] Liquidation consumer started.")
    while True:
        try:
            # (timestamp, symbol, side, price, size)
            liq_data = await queue.get()
            timestamp, symbol, side, price, size = liq_data
            
            # Use the correct symbol format (e.g., BTCUSDT:USDT)
            # This handles both "BTCUSDT" (from Binance) and "BTCUSDT" (from Bybit)
            if ":USDT" not in symbol:
                 symbol = f"{symbol}:USDT"

            if symbol not in storage:
                storage[symbol] = deque(maxlen=5000) # Store last 5000 liqs per symbol
            
            storage[symbol].append((timestamp, price, size))
            
        except Exception as e:
            print(f"[LIQ-CONSUMER] Error processing liquidation: {e}")
# --- END PHASE 2 ---

# --- (UPGRADE) NEW DASHBOARD FUNCTIONS ---
def build_layout() -> Layout:
    """Defines the layout of the Rich dashboard."""
    layout = Layout(name="root")
    layout.split(
        Layout(name="header", size=1),
        Layout(name="main", ratio=1),
        Layout(name="footer", size=5)
    )
    layout["main"].split_row(
        Layout(name="live_trades", ratio=2),
        Layout(name="recent_closes", ratio=3)
    )
    layout["footer"].split_row(
        Layout(name="stats"),
        Layout(name="logs") # We can add logs here later
    )
    return layout

def get_live_trades_table(trades: List[dict]) -> Table:
    """Creates the 'Live Trade Log' table."""
    table = Table(title="Trade Log — LIVE", border_style="bold green")
    table.add_column("Open", style="dim")
    table.add_column("Sym", style="bold yellow")
    table.add_column("Side", style="green")
    table.add_column("TF", style="dim")
    table.add_column("Conf", style="magenta")
    table.add_column("Entry")
    table.add_column("SL", style="red")
    table.add_column("TP1", style="green")
    table.add_column("TP2")
    table.add_column("TP3")
    table.add_column("Outcome")

    for r in trades:
        pr = 2 if r["entry"] >= 10 else (4 if r["entry"] >= 0.1 else (6 if r["entry"] >= 0.001 else 8))
        table.add_row(
            time.strftime('%H:%M:%S', time.localtime(r['open_ts'])),
            r['symbol'].split('/')[0], # Show 'BTC' not 'BTC/USDT:USDT'
            r['side'],
            r['tf'],
            f"{r['conf']:.2f}",
            f"{r['entry']:.{pr}f}",
            f"{r['sl']:.{pr}f}",
            f"{r['tp1']:.{pr}f}",
            f"{r['tp2']:.{pr}f}",
            f"{r['tp3']:.{pr}f}",
            f"[bold green]{r['outcome']}[/]"
        )
    return table

def get_closed_trades_table(trades: List[dict]) -> Table:
    """Creates the 'Recent Closes' table."""
    table = Table(title="Recent Closes — Last 20", border_style="dim")
    table.add_column("Close", style="dim")
    table.add_column("Sym", style="bold yellow")
    table.add_column("Side")
    table.add_column("TF", style="dim")
    table.add_column("Entry")
    table.add_column("Exit")
    table.add_column("PnL %")
    table.add_column("R net")
    table.add_column("Hit")
    table.add_column("Duration", style="dim")
    
    for r in trades:
        pr = 2 if r["entry"] >= 10 else (4 if r["entry"] >= 0.1 else (6 if r["entry"] >= 0.001 else 8))
        
        if r['outcome'] == "SL" or r['outcome'] == "Fail":
            pnl_style = "red"
            r_style = "red"
            outcome_style = "bold red"
        else:
            pnl_style = "green"
            r_style = "green"
            outcome_style = "bold green"
            
        table.add_row(
            time.strftime('%H:%M:%S', time.localtime(r['close_ts'])),
            r['symbol'].split('/')[0],
            r['side'],
            r['tf'],
            f"{r['entry']:.{pr}f}",
            f"{r['exit']:.{pr}f}",
            f"[{pnl_style}]{r['pnl_pct']:+.2f}%[/]",
            f"[{r_style}]{r['r_net']:+.2f}R[/]",
            f"[{outcome_style}]{r['outcome']}[/]",
            r['duration']
        )
    return table

def get_stats_panel(stats: dict) -> Panel:
    """Creates the 'Overall Stats' panel."""
    win_rate = stats.get('win_rate', 0.0) * 100
    expectancy = stats.get('expectancy', 0.0)
    avg_r = stats.get('avg_r', 0.0)
    
    win_style = "green" if win_rate > 50 else "red"
    exp_style = "green" if expectancy > 0 else "red"
    avg_r_style = "green" if avg_r > 0 else "red"

    grid = Table.grid(expand=True, padding=(0, 2))
    grid.add_column(ratio=1)
    grid.add_column(ratio=1)
    grid.add_column(ratio=1)
    grid.add_column(ratio=1)
    
    grid.add_row(
        Text("Total Trades", style="dim"),
        Text("Win Rate", style="dim"),
        Text("Avg. R-Unit", style="dim"),
        Text("Expectancy (R)", style="dim"),
    )
    grid.add_row(
        Text(f"{stats.get('total_trades', 0)}", style="bold"),
        Text(f"{win_rate:.2f}%", style=f"bold {win_style}"),
        Text(f"{avg_r:+.2f}R", style=f"bold {avg_r_style}"),
        Text(f"{expectancy:+.3f}R", style=f"bold {exp_style}")
    )
    return Panel(grid, title="Overall Stats (from DB)", border_style="dim")


async def run_dashboard(manager: TradeManager, console: Console):
    """
    Replaces the old `printer` function. Runs a live-updating
    Rich dashboard.
    """
    layout = build_layout()
    layout["header"].update(Align.center(f"[bold cyan]FRESH MONEY DUAL SCREENER[/] (1h + 4h) — UTC {_time.strftime('%Y-%m-d %H:%M:%S')}", vertical="middle"))
    
    # We can pipe logs here later
    log_panel = Panel("", title="Logs", border_style="dim")
    layout["footer"]["logs"].update(log_panel)
    
    with Live(layout, console=console, refresh_per_second=1, screen=True) as live:
        while True:
            await asyncio.sleep(PRINT_INTERVAL)
            
            # Update header time
            layout["header"].update(Align.center(f"[bold cyan]FRESH MONEY DUAL SCREENER[/] (1h + 4h) — UTC {_time.strftime('%Y-%m-d %H:%M:%S')}", vertical="middle"))
            
            # Get data from manager
            live_trades = await manager.get_live_trades()
            closed_trades = await manager.get_closed_trades()
            stats = await manager.get_stats()
            
            # Update tables
            layout["main"]["live_trades"].update(get_live_trades_table(live_trades))
            layout["main"]["recent_closes"].update(get_closed_trades_table(closed_trades))
            layout["footer"]["stats"].update(get_stats_panel(stats))

# --- END (UPGRADE) DASHBOARD FUNCTIONS ---

async def main():
    # (UPGRADE) Use rich console
    console = Console()
    print(f"[bold green]--- Fresh Money Dual Screener (1h + 4h) ---[/]")
    
    # --- (UPGRADE) Init TradeLogger and TradeManager ---
    trade_logger = TradeLogger()
    await trade_logger.init_db()
    
    trade_manager = TradeManager(trade_logger)
    await trade_manager.init()
    # --- END (UPGRADE) ---
    
    # --- PHASE 2: LIQ STORAGE ---
    liq_queue = _async.Queue()
    liq_storage = {} # Dict of deques, keyed by symbol
    # --- END PHASE 2 ---

    exchanges = await init_exchanges()
    symbols_full = await common_usdt_symbols(exchanges) # e.g., ["BTCUSDT:USDT", ...]
    
    # --- PHASE 2: Create cleaned symbol list for collectors ---
    symbols_collector = [s.split(":")[0].replace("/", "") for s in symbols_full]
    # --- END PHASE 2 ---

    # --- (UPGRADE) Pass manager to Screeners ---
    oneh  = Screener(CFG_1H, exchanges, symbols_full, liq_storage, trade_manager)
    fourh = Screener(CFG_4H, exchanges, symbols_full, liq_storage, trade_manager)
    # --- END (UPGRADE) ---

    ok1 = await oneh.initialize()
    ok2 = await fourh.initialize()
    if not ok1 and not ok2:
        print(f"[bold red]Initialization failed for both timeframes. Exiting.[/]")
        for ex in exchanges.values():
            try:
                await ex.close()
            except Exception:
                pass
        return
    
    # --- PHASE 2: Start Collectors ---
    _async.create_task(liquidation_consumer(liq_queue, liq_storage))
    # --- (MERGE) Call local functions ---
    _async.create_task(connect_binance_liquidations(liq_queue, symbols_collector))
    _async.create_task(connect_bybit_liquidations(liq_queue, symbols_collector))
    # --- END (MERGE) ---

    # --- (UPGRADE) Start the new dashboard ---
    _async.create_task(run_dashboard(trade_manager, console))
    # --- END (UPGRADE) ---

    try:
        await _async.gather(oneh.subscribe_all(), fourh.subscribe_all())
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
    except Exception as e:
        # (UPGRADE) Log critical errors
        console = Console()
        console.print_exception()
        print(f"\n[bold red]A critical error occurred: {e}[/]")