import asyncio
import json
import logging
import math
import os
import random
import re
import threading
import requests  # type: ignore
import websockets  # type: ignore
import time as t
import aiohttp
from datetime import datetime, timezone, timedelta
from enum import Enum, auto
from typing import Deque, Dict, Any, Optional, cast, List, Tuple
from dotenv import load_dotenv  # type: ignore
from web3 import Web3  # type: ignore
from web3.types import TxParams, Wei  # type: ignore
from collections import deque
from zoneinfo import ZoneInfo

from py_clob_client_v2.client import ClobClient
from py_clob_client_v2.clob_types import OrderArgs, OrderPayload, OrderType, ApiCreds
from py_clob_client_v2.order_builder.constants import BUY, SELL
from py_clob_client_v2 import Side, SignatureTypeV2

from rich.console import Console
from rich.panel import Panel
from rich.layout import Layout
from rich.live import Live
from rich import box
from rich.text import Text

from config import (
    ASSET_COOLDOWN_MINUTES,
    ASSET_COOLDOWN_SECONDS,
    ASSET_MAX_CUMULATIVE_LOSS,
    DRY_RUN_DEFAULT,
    MIN_SHARES,
    PNL_FILES,
    TOTAL_BOTS,
    TRADING_ASSETS,
    TRADING_ASSETS_UPPER,
    WORKER_CONFIGS,
    WorkerConfig,
    asset_pnl_filename,
    validate_asset_cooldown_config,
    validate_trading_assets,
    worker_key,
)
from strategies.base import SpreadDecision
from strategies.spread_capture import SpreadCaptureStrategy
from utils.spread_inventory import SpreadInventory
from utils.clob_helpers import clamp_buy_price, clamp_sell_price, parse_order_type

console = Console()
load_dotenv()
validate_trading_assets()
validate_asset_cooldown_config()

# ── Structured trade logger ──────────────────────────────────────────────────
_exec_logger = logging.getLogger("emiliano.execution")
_exec_logger.setLevel(logging.DEBUG)
if not _exec_logger.handlers:
    _fh = logging.FileHandler("emiliano_execution.log", encoding="utf-8")
    _fh.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    _exec_logger.addHandler(_fh)

def exec_log(event: str, **kwargs):
    payload = {"event": event, "ts": t.time(), **kwargs}
    _exec_logger.info(json.dumps(payload))


# ── Configuration ────────────────────────────────────────────────────────────

DRY_MODE = DRY_RUN_DEFAULT
LOG_FILE  = "emiliano_trades.txt"

HOST        = "https://clob.polymarket.com"
WS_URL      = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA_URL   = "https://gamma-api.polymarket.com/markets"
POLYGON_RPC = os.getenv("POLYGON_RPC")

PUSD_ADDRESS      = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"
USDC_E            = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
CTF_CONTRACT      = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
CTF_EXCHANGE      = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
NEG_RISK_EXCHANGE = "0xC5d563A36AE78145C45a50134d48A1215220f80a"
STANDARD_EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
NEG_RISK_ADAPTER  = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"

BOLD   = "\033[1m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
RED    = "\033[91m"
RESET  = "\033[0m"

# ── Market interval (5m vs 15m crypto up/down markets) ────────────────────────
# MARKET_INTERVAL : "5m" | "5" | "15m" | "15"   (default "5m")
#   Controls slug format ({asset}-updown-{5m|15m}-{ts}), timestamp rounding,
#   and default listener/entry windows (full market duration unless overridden).
#
# LISTENER_ACTIVATE_SECONDS / ENTRY_SECONDS_LEFT :
#   Default to the full market duration for the selected interval.
#   Set explicitly in .env to override (e.g. ENTRY_SECONDS_LEFT=60).

def _parse_market_interval(raw: str) -> Tuple[int, str]:
    """Return (duration_seconds, slug_label) e.g. (300, '5m') or (900, '15m')."""
    token = (raw or "5m").strip().lower().rstrip("m")
    if token in ("15",):
        return 900, "15m"
    if token in ("5",):
        return 300, "5m"
    raise ValueError(f"unsupported MARKET_INTERVAL={raw!r} — use 5m or 15m")


_raw_market_interval = os.getenv("MARKET_INTERVAL", "5m").strip()
try:
    MARKET_INTERVAL_SECONDS, MARKET_INTERVAL_SLUG = _parse_market_interval(_raw_market_interval)
except ValueError as _interval_err:
    print(f"⚠️  {_interval_err} — falling back to 5m.")
    MARKET_INTERVAL_SECONDS, MARKET_INTERVAL_SLUG = 300, "5m"

LISTENER_ACTIVATE_SECONDS = int(
    os.getenv("LISTENER_ACTIVATE_SECONDS", str(MARKET_INTERVAL_SECONDS))
)
ENTRY_SECONDS_LEFT = int(
    os.getenv("ENTRY_SECONDS_LEFT", str(MARKET_INTERVAL_SECONDS))
)

print(f"📊 MARKET_INTERVAL={MARKET_INTERVAL_SLUG} ({MARKET_INTERVAL_SECONDS}s) | "
      f"listener={LISTENER_ACTIVATE_SECONDS}s | entry_window={ENTRY_SECONDS_LEFT}s")


def _interval_start(ts: int) -> int:
    """Snap a unix timestamp to the start of its market window."""
    return (ts // MARKET_INTERVAL_SECONDS) * MARKET_INTERVAL_SECONDS


def market_slug(asset: str, start_ts: int) -> str:
    """Polymarket slug for a crypto up/down market, e.g. btc-updown-5m-1710000000."""
    return f"{asset.lower()}-updown-{MARKET_INTERVAL_SLUG}-{start_ts}"


def current_interval_starts(now_ts: Optional[int] = None) -> Tuple[int, int]:
    """Return (current_window_start, next_window_start) unix timestamps."""
    now = now_ts if now_ts is not None else int(datetime.now(timezone.utc).timestamp())
    base = _interval_start(now)
    return base, base + MARKET_INTERVAL_SECONDS


# Locked-price sentinels. Entry is skipped when the price equals either value.
LOCKED_LOW  = 0.01  # 1c  — market resolved or buy-side liquidity exhausted
LOCKED_HIGH = 1.00  # 100c — market fully resolved

FINAL_PRICE   = float(os.getenv("FINAL_PRICE", "0.70"))

BINANCE_PRIME_THRESHOLD   = float(os.getenv("BINANCE_PRIME_THRESHOLD",   "0.20"))
BINANCE_STALE_CUTOFF_SECS = float(os.getenv("BINANCE_STALE_CUTOFF_SECS", "5.0"))
BINANCE_DEPTH_LIMIT       = int(os.getenv("BINANCE_DEPTH_LIMIT",          "20"))

MIN_FILL_DELTA   = float(os.getenv("MIN_FILL_DELTA",   "0.05"))


# ═════════════════════════════════════════════════════════════════════════════
# TRADING SCHEDULE  — weekday-only entry gate
#
# Philosophy
# ──────────
# Only NEW trade entries are blocked on weekends. Everything else continues
# running normally: existing positions are monitored and exited via TP/SL,
# the portfolio chart keeps updating, Redis persistence keeps writing, wallet
# audits and PnL merges keep running, and the WebSocket price listener stays
# connected so the bot can react immediately when Monday arrives.
#
# Design
# ──────
# A single module-level function `is_trading_allowed()` is the ONLY place
# the weekday check lives. It is called exactly once per check_logic() tick,
# right before the IDLE entry branch. No other code path is touched because:
#   - execute_order() handles both BUY and SELL → cannot be gated
#   - confirmed_execute() handles both BUY and SELL → cannot be gated
#   - start() handles market scanning, not entry decisions → not touched
#   - price_listener() handles WS plumbing and expiry → not touched
#   - TP/SL exit path (_check_single_side_exit → market_exit) → not touched
#
# Timezone
# ────────
# All datetime arithmetic in this codebase uses UTC (datetime.now(timezone.utc)
# throughout). The ZoneInfo("America/New_York") reference in the terminal
# dashboard is display-only and does not affect trading decisions.
#
# The trading-schedule timezone is separately configurable via the
# TRADING_TIMEZONE environment variable (default "UTC"). This makes weekday
# calculation deterministic and independent of the Render server's locale.
#
# Configuration (all optional — sensible defaults work without any .env change)
# ───────────────────────────────────────────────────────────────────────────────
# TRADING_TIMEZONE   : IANA timezone string for weekday calculation.
#                      Default: "UTC"
#                      Example: TRADING_TIMEZONE=America/New_York
#
# TRADING_DAYS       : Comma-separated list of 0-indexed weekday numbers
#                      (Monday=0 … Sunday=6) that are allowed for new entries.
#                      Default: "0,1,2,3,4"  (Monday through Friday)
#                      Example: TRADING_DAYS=0,1,2,3,4,5  (adds Saturday)
#
# WEEKEND_TRADING    : Set to "true" to bypass the schedule entirely.
#                      Useful for testing, or if you decide to trade weekends.
#                      Default: "false"
# ═════════════════════════════════════════════════════════════════════════════

# Parse TRADING_TIMEZONE (validated once at import time).
def resolve_timezone(name: str):
    """
    Return tzinfo for an IANA timezone name.

    UTC/GMT use datetime.timezone.utc so the app starts on Windows even when
    the optional tzdata package is not installed. Other zones need tzdata
    (listed in requirements.txt).
    """
    key = (name or "UTC").strip()
    if key.upper() in ("UTC", "GMT", "ETC/UTC", "ETC/GMT", "Z"):
        return timezone.utc
    try:
        return ZoneInfo(key)
    except Exception:
        print(
            f"⚠️  [schedule] Unknown or unavailable TRADING_TIMEZONE={key!r} "
            f"(install tzdata on Windows) — falling back to UTC."
        )
        return timezone.utc


_TRADING_TZ_NAME: str = os.getenv("TRADING_TIMEZONE", "UTC").strip()
TRADING_TZ = resolve_timezone(_TRADING_TZ_NAME)
if TRADING_TZ is timezone.utc and _TRADING_TZ_NAME.upper() not in (
    "UTC", "GMT", "ETC/UTC", "ETC/GMT", "Z",
):
    _TRADING_TZ_NAME = "UTC"

# Display-only ET zone for terminal dashboard labels (optional tzdata).
try:
    ET_ZONE = ZoneInfo("America/New_York")
except Exception:
    ET_ZONE = timezone.utc

# Parse TRADING_DAYS. Validated once at import time. Stored as a frozenset
# for O(1) membership tests on every price tick.
_raw_trading_days: str = os.getenv("TRADING_DAYS", "0,1,2,3,4").strip()
try:
    TRADING_DAYS: frozenset = frozenset(
        int(d.strip()) for d in _raw_trading_days.split(",") if d.strip().isdigit()
    )
    if not TRADING_DAYS:
        raise ValueError("empty set")
except Exception:
    print(f"⚠️  [schedule] Invalid TRADING_DAYS={_raw_trading_days!r} — defaulting to Mon-Fri.")
    TRADING_DAYS = frozenset({0, 1, 2, 3, 4})

# Kill-switch: set WEEKEND_TRADING=true to bypass the schedule entirely.
WEEKEND_TRADING_ENABLED: bool = os.getenv("WEEKEND_TRADING", "false").lower() == "true"

# Human-readable day names used in log messages.
_DAY_NAMES: Dict[int, str] = {
    0: "Monday", 1: "Tuesday",  2: "Wednesday",
    3: "Thursday", 4: "Friday", 5: "Saturday", 6: "Sunday",
}

# Throttle the "entry blocked" log message to at most once per minute so the
# log file / stdout is not flooded on every WebSocket tick (which can fire
# hundreds of times per minute).
_last_weekend_log_ts: float = 0.0
_WEEKEND_LOG_INTERVAL_SEC: float = 60.0


def is_trading_allowed() -> bool:
    """
    Return True when new trade entries are permitted, False when they must be
    blocked (weekend / non-trading day).

    THIS IS THE SINGLE, CENTRALIZED TRADING-HOURS CHECK FOR THE ENTIRE BOT.
    No other function, class, or module contains weekday logic. Every code
    path that needs to know "can we open a new position right now?" calls
    this function — there are zero inline weekday checks anywhere else.

    Behavior
    ────────
    • Returns True on Mon–Fri (weekday numbers 0–4 by default, or whatever
      is configured in TRADING_DAYS).
    • Returns False on Sat–Sun unless WEEKEND_TRADING=true is set.
    • The weekday is evaluated in TRADING_TZ (default UTC), not the server's
      local timezone, so the result is deterministic regardless of where
      Render hosts the bot.
    • When WEEKEND_TRADING=true the function always returns True (full bypass).

    Does NOT affect
    ───────────────
    • TP/SL monitoring on existing positions (those always run).
    • SELL / exit order execution (those always run).
    • Portfolio chart updates (those always run).
    • Redis persistence, PnL merges, wallet audits (those always run).
    • The WebSocket price listener (stays connected through weekends).
    """
    if WEEKEND_TRADING_ENABLED:
        return True

    now_in_tz = datetime.now(TRADING_TZ)
    weekday   = now_in_tz.weekday()   # 0=Monday … 6=Sunday
    return weekday in TRADING_DAYS


def log_weekend_block(asset_type: str, side: str, price_cents: int) -> None:
    """
    Emit a throttled, audit-friendly log entry when a new-position entry is
    blocked because trading is not currently permitted (weekend / non-trading
    day).

    Throttled to at most one message per _WEEKEND_LOG_INTERVAL_SEC across
    the entire process (not per-worker) so the logs stay readable during the
    ~48 hours of a full weekend.

    Parameters
    ──────────
    asset_type  : e.g. "btc", "eth" — the worker's asset identifier.
    side        : "YES" or "NO" — the side that would have been entered.
    price_cents : integer cents price that triggered the 90c threshold
                  (e.g. 92 meaning the market was at 92c).
    """
    global _last_weekend_log_ts
    now = t.time()
    if now - _last_weekend_log_ts < _WEEKEND_LOG_INTERVAL_SEC:
        return   # throttled — already logged within the last minute
    _last_weekend_log_ts = now

    now_in_tz   = datetime.now(TRADING_TZ)
    weekday     = now_in_tz.weekday()
    day_name    = _DAY_NAMES.get(weekday, f"day-{weekday}")
    ts_str      = now_in_tz.strftime("%Y-%m-%d %H:%M:%S")
    tz_label    = _TRADING_TZ_NAME
    allowed_names = ", ".join(
        _DAY_NAMES[d] for d in sorted(TRADING_DAYS) if d in _DAY_NAMES
    )

    msg = (
        f"🚫 [SCHEDULE] [{asset_type.upper()}] New-position entry BLOCKED — "
        f"it is {day_name} ({ts_str} {tz_label}). "
        f"Trading permitted only on: {allowed_names}. "
        f"Would have bought {side} @ {price_cents}c. "
        f"Existing TP/SL monitoring and portfolio tracking continue normally."
    )
    print(msg)
    exec_log(
        "entry_blocked_schedule",
        asset=asset_type,
        side=side,
        price_cents=price_cents,
        day=day_name,
        timestamp_tz=ts_str,
        timezone=tz_label,
        allowed_trading_days=sorted(TRADING_DAYS),
    )


# Throttle per-asset cooldown block logs (same pattern as weekend gate).
_last_cooldown_log_ts: Dict[str, float] = {}
_COOLDOWN_LOG_INTERVAL_SEC: float = 60.0


def log_cooldown_block(asset_type: str, window: str, side: str, price_cents: int) -> None:
    """Emit a throttled log when new entries are blocked by asset cooldown."""
    global _last_cooldown_log_ts
    block_key = worker_key(asset_type, window)
    now = t.time()
    last = _last_cooldown_log_ts.get(block_key, 0.0)
    if now - last < _COOLDOWN_LOG_INTERVAL_SEC:
        return
    _last_cooldown_log_ts[block_key] = now

    status = asset_cooldown.get_status(asset_type, window)
    until  = status.get("cooldown_until_utc") or "unknown"
    remaining = status.get("cooldown_remaining_sec", 0)
    window_pnl  = status.get("cooldown_window_pnl", 0.0)

    msg = (
        f"🚫 [COOLDOWN] [{asset_type.upper()} {window}] New-position entry BLOCKED — "
        f"cooldown window PnL ${window_pnl:.2f} (limit -${ASSET_MAX_CUMULATIVE_LOSS:.2f}). "
        f"Trading disabled until {until} UTC "
        f"({remaining // 60}m {remaining % 60}s remaining). "
        f"Would have bought {side} @ {price_cents}c. "
        f"Existing TP/SL monitoring continues normally."
    )
    print(msg)
    exec_log(
        "entry_blocked_cooldown",
        asset=asset_type,
        window=window,
        side=side,
        price_cents=price_cents,
        cooldown_window_pnl=window_pnl,
        cooldown_until_utc=until,
        cooldown_remaining_sec=remaining,
    )


# ═════════════════════════════════════════════════════════════════════════════
# TRADE STATE MACHINE
# ═════════════════════════════════════════════════════════════════════════════

class OrderState(Enum):
    CREATED          = auto()
    SUBMITTED        = auto()
    OPEN             = auto()
    PARTIALLY_FILLED = auto()
    FILLED           = auto()
    CANCEL_PENDING   = auto()
    CANCELLED        = auto()
    REJECTED         = auto()
    FAILED           = auto()


class TradeState(Enum):
    # No position open. Bot monitors YES and NO for a qualifying ≥90c entry.
    IDLE    = auto()
    # Single directional position is held. Entry logic is fully suppressed.
    # Only TP/SL monitoring runs from this state.
    FILLED  = auto()
    # TP or SL triggered. Sell order is in flight.
    EXITING = auto()
    # Trade cycle complete. reset_state() returns to IDLE for the next market.
    CLOSED  = auto()
    # Unexpected failure. Logged; no further orders until reset_state().
    ERROR   = auto()


class SpreadState(Enum):
    IDLE    = auto()
    PENDING = auto()


# ─────────────────────────────────────────────────────────────────────────────
# LOCKED-PRICE GUARD
# ─────────────────────────────────────────────────────────────────────────────

def is_locked_price(price: float) -> bool:
    # Round to 4 d.p. to absorb floating-point noise from the WebSocket feed.
    rounded = round(price, 4)
    return rounded <= LOCKED_LOW or rounded >= LOCKED_HIGH


# ═════════════════════════════════════════════════════════════════════════════
# REDIS PERSISTENCE LAYER
# ═════════════════════════════════════════════════════════════════════════════

UPSTASH_URL   = os.getenv("UPSTASH_REDIS_REST_URL", "").rstrip("/")
UPSTASH_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN", "")
_redis_available = bool(UPSTASH_URL and UPSTASH_TOKEN)

def _redis_headers() -> dict:
    return {"Authorization": f"Bearer {UPSTASH_TOKEN}"}

def redis_get(key: str) -> Optional[str]:
    if not _redis_available:
        return None
    try:
        resp = requests.get(f"{UPSTASH_URL}/get/{key}", headers=_redis_headers(), timeout=4)
        return resp.json().get("result")
    except Exception as e:
        print(f"⚠️ Redis GET error ({key}): {e}")
        return None

def redis_set(key: str, value: str) -> bool:
    if not _redis_available:
        return False
    try:
        resp = requests.get(
            f"{UPSTASH_URL}/set/{key}/{requests.utils.quote(value, safe='')}",  # type: ignore
            headers=_redis_headers(), timeout=4,
        )
        return resp.json().get("result") == "OK"
    except Exception as e:
        print(f"⚠️ Redis SET error ({key}): {e}")
        return False

def redis_set_json(key: str, obj: Any) -> bool:
    return redis_set(key, json.dumps(obj))

def redis_get_json(key: str) -> Optional[Any]:
    raw = redis_get(key)
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


# ═════════════════════════════════════════════════════════════════════════════
# ASSET-LEVEL CIRCUIT BREAKER / COOLDOWN
#
# Risk-management tracker ONLY — separate from lifetime cumulative_pnl stored in
# emiliano:{asset}:stats, emiliano:{asset}:trades, portfolio history, and local
# {asset}_pnl_history.json. Those historical values are never read or modified
# by this module.
#
# Redis key   : emiliano:{asset}:cooldown
# Local fallback : {asset}_cooldown_state.json
#
# Cooldown-window PnL (Option A — reset after cooldown expires)
# ───────────────────────────────────────────────────────────────
# • window_pnl starts at 0 when the bot starts or when a cooldown expires.
# • Each completed trade adds its realized pnl_amount (same value passed to
#   log_pnl() on TP/SL/HODL exit — not unrealized mark-to-market).
# • Breach when window_pnl <= -ASSET_MAX_CUMULATIVE_LOSS → trigger cooldown.
# • When cooldown expires, window_pnl resets to 0 (fresh risk window).
#
# Events that update window_pnl
# ─────────────────────────────
# • MarketWorker.log_pnl() after every validated trade close (STOP_LOSS,
#   TAKE_PROFIT, HODL settlement, manual cashout).
#
# Events that do NOT update window_pnl
# ────────────────────────────────────
# • Opening a position (BUY fill) — no realized PnL yet.
# • Unrealized position PnL while FILLED.
# • Any writes to lifetime stats / trade history / portfolio chart.
# ═════════════════════════════════════════════════════════════════════════════


def _cooldown_redis_key(asset: str, window: str) -> str:
    return f"emiliano:{asset.lower()}:{window.lower()}:cooldown"


def _cooldown_local_path(asset: str, window: str) -> str:
    return f"{asset.lower()}_{window.lower()}_cooldown_state.json"


def _cooldown_state_key(asset: str, window: str) -> str:
    return worker_key(asset, window)


class AssetCooldownManager:
    """Per (asset, window) circuit breaker with Redis + local JSON persistence."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._state: Dict[str, Dict[str, Any]] = {}

    @staticmethod
    def _empty_state() -> Dict[str, Any]:
        return {
            "window_pnl":         0.0,
            "disabled_until_ts":  None,
            "breach_at":          None,
            "last_updated":       None,
        }

    def _load_state(self, asset: str, window: str) -> Dict[str, Any]:
        key = _cooldown_state_key(asset, window)
        with self._lock:
            if key in self._state:
                return dict(self._state[key])

        loaded: Optional[Dict[str, Any]] = None
        if _redis_available:
            raw = redis_get_json(_cooldown_redis_key(asset, window))
            if isinstance(raw, dict):
                loaded = raw

        if loaded is None:
            path = _cooldown_local_path(asset, window)
            try:
                if os.path.exists(path):
                    with open(path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    if isinstance(data, dict):
                        loaded = data
            except Exception as e:
                print(f"⚠️ [COOLDOWN] Could not read {path}: {e}")

        state = self._empty_state()
        if loaded:
            wp = loaded.get("window_pnl", 0.0)
            state["window_pnl"] = float(wp) if _is_finite_number(wp) else 0.0
            dut = loaded.get("disabled_until_ts")
            if dut is not None and _is_finite_number(dut):
                state["disabled_until_ts"] = float(dut)
            state["breach_at"]    = loaded.get("breach_at")
            state["last_updated"] = loaded.get("last_updated")

        self._expire_if_needed(asset, window, state, persist=False)
        with self._lock:
            self._state[key] = state
        return dict(state)

    def _persist_state(self, asset: str, window: str, state: Dict[str, Any]) -> None:
        key = _cooldown_state_key(asset, window)
        payload = {
            "window_pnl":        round(float(state.get("window_pnl", 0.0)), 4),
            "disabled_until_ts": state.get("disabled_until_ts"),
            "breach_at":         state.get("breach_at"),
            "last_updated":      datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        }
        state["last_updated"] = payload["last_updated"]
        if _redis_available:
            redis_set_json(_cooldown_redis_key(asset, window), payload)
        try:
            path = _cooldown_local_path(asset, window)
            tmp  = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
        except Exception as e:
            print(f"⚠️ [COOLDOWN] Local persist failed for {key} (non-fatal): {e}")

    def _expire_if_needed(
        self, asset: str, window: str, state: Dict[str, Any], *, persist: bool = True,
    ) -> bool:
        """If cooldown has expired, reset window_pnl and re-enable trading."""
        dut = state.get("disabled_until_ts")
        if dut is None or not _is_finite_number(dut):
            return False
        now = t.time()
        if now < float(dut):
            return False

        label = f"{asset.upper()} {window}"
        print(f"[COOLDOWN] {label} cooldown expired")
        print(f"[COOLDOWN] {label} trading re-enabled")
        exec_log(
            "cooldown_expired",
            asset=asset,
            window=window,
            previous_window_pnl=state.get("window_pnl"),
        )

        state["window_pnl"]        = 0.0
        state["disabled_until_ts"] = None
        state["breach_at"]         = None
        if persist:
            self._persist_state(asset, window, state)
        return True

    def record_realized_pnl(self, asset: str, window: str, pnl_amount: float) -> None:
        """Add realized PnL to the cooldown window; trigger cooldown on breach."""
        if not _is_finite_number(pnl_amount):
            return

        key = _cooldown_state_key(asset, window)
        with self._lock:
            state = self._load_state(asset, window)
            self._expire_if_needed(asset, window, state)

            if state.get("disabled_until_ts") is not None:
                self._state[key] = state
                return

            state["window_pnl"] = round(state["window_pnl"] + float(pnl_amount), 4)
            self._state[key]  = state

            if state["window_pnl"] > -ASSET_MAX_CUMULATIVE_LOSS:
                self._persist_state(asset, window, state)
                return

            until_ts = t.time() + ASSET_COOLDOWN_SECONDS
            until_dt = datetime.fromtimestamp(until_ts, tz=timezone.utc)
            until_str = until_dt.strftime("%Y-%m-%d %H:%M:%S")

            state["disabled_until_ts"] = until_ts
            state["breach_at"]         = until_str
            self._state[key]         = state
            self._persist_state(asset, window, state)

        label = f"{asset.upper()} {window}"
        print(
            f"[COOLDOWN] {label} cumulative PnL reached "
            f"-${abs(state['window_pnl']):.2f}"
        )
        print(f"[COOLDOWN] {label} trading disabled until {until_str} UTC")
        exec_log(
            "cooldown_triggered",
            asset=asset,
            window=window,
            window_pnl=state["window_pnl"],
            disabled_until_utc=until_str,
            max_loss=ASSET_MAX_CUMULATIVE_LOSS,
            cooldown_minutes=ASSET_COOLDOWN_MINUTES,
        )

    def is_entry_blocked(self, asset: str, window: str) -> bool:
        """Return True when new entries must be blocked for this worker."""
        key = _cooldown_state_key(asset, window)
        with self._lock:
            state = self._load_state(asset, window)
            expired = self._expire_if_needed(asset, window, state)
            if expired:
                self._state[key] = state
            blocked = state.get("disabled_until_ts") is not None
            self._state[key] = state
            return blocked

    def get_status(self, asset: str, window: str) -> Dict[str, Any]:
        key = _cooldown_state_key(asset, window)
        with self._lock:
            state = self._load_state(asset, window)
            self._expire_if_needed(asset, window, state)
            self._state[key] = state

        dut = state.get("disabled_until_ts")
        now = t.time()
        active = dut is not None and _is_finite_number(dut) and now < float(dut)
        remaining = max(0, int(float(dut) - now)) if active and dut is not None else 0
        until_utc = None
        if active and dut is not None:
            until_utc = datetime.fromtimestamp(float(dut), tz=timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S"
            )

        return {
            "cooldown_active":          active,
            "cooldown_window_pnl":      round(float(state.get("window_pnl", 0.0)), 2),
            "cooldown_until_utc":       until_utc,
            "cooldown_remaining_sec":   remaining,
            "cooldown_max_loss":        ASSET_MAX_CUMULATIVE_LOSS,
            "cooldown_minutes":         ASSET_COOLDOWN_MINUTES,
            "entries_blocked_cooldown": active,
        }

    def get_all_statuses(self) -> Dict[str, Dict[str, Any]]:
        return {wc.key: self.get_status(wc.asset, wc.window) for wc in WORKER_CONFIGS}


asset_cooldown = AssetCooldownManager()


# ═════════════════════════════════════════════════════════════════════════════
# PORTFOLIO HISTORY — persistent equity-curve storage
#
# Redis key  : emiliano:portfolio:history
# Value      : JSON list of {"t": unix_ms, "v": portfolio_pnl_dollars}
#              sorted oldest-first, capped at MAX_HISTORY_POINTS entries.
#
# Backfill   : On startup (called from main.py's startup_event) we inspect
#              all emiliano:{asset}:trades keys already in Redis, reconstruct
#              the full cross-asset portfolio equity curve from those trade
#              records, and persist it to emiliano:portfolio:history.
#              This means the chart shows real history from the very first
#              trade — not just from the day this feature was deployed.
#
# Idempotent : The backfill compares the number of trade records in Redis
#              against the existing history length. If history already covers
#              every trade it does nothing. If new trades exist beyond what is
#              already stored it merges only the missing tail. Safe to rerun.
#
# Live feed  : log_pnl() calls portfolio_history_snapshot() immediately after
#              every completed trade so the curve updates in real time without
#              waiting for any background flush interval.
# ═════════════════════════════════════════════════════════════════════════════

PORTFOLIO_HISTORY_KEY = "emiliano:portfolio:history"
MAX_HISTORY_POINTS    = 60_000          # ~1 year at 1 pt per 10 min

# Tracked assets — loaded once from config.py (ASSET / TRADING_ASSETS env vars).

# ── Flat-period snapshot throttling ───────────────────────────────────────
# ROOT CAUSE OF "CHART KEEPS GROWING WITH IDENTICAL VALUES":
# portfolio_history_snapshot() used to unconditionally append a brand new
# {t, v} point every time it was called — once per completed trade (every
# market round) AND once every 60 s from main.py's idle-period heartbeat
# loop — even when the value had not changed at all since the previous
# point. Over weeks of mostly-flat PnL this writes thousands of points that
# are visually and informationally identical, bloating Redis storage and the
# /api/history payload without adding any real information.
#
# Fix: a new point is only persisted when the value changed by more than
# PORTFOLIO_FLAT_EPSILON. Optional PORTFOLIO_HEARTBEAT_SEC (>0) may append
# same-value points after that interval (default 0 = disabled). The chart
# extends flat segments to "now" at render time — no heartbeat spam in Redis.
PORTFOLIO_FLAT_EPSILON  = float(os.getenv("PORTFOLIO_FLAT_EPSILON",  "0.005"))   # half a cent
PORTFOLIO_HEARTBEAT_SEC = int(os.getenv("PORTFOLIO_HEARTBEAT_SEC", "0"))          # 0 = off

CHART_PERIOD_MS: Dict[str, int] = {
    "1D":  24 * 3600 * 1000,
    "1W":  7  * 24 * 3600 * 1000,
    "1M":  30 * 24 * 3600 * 1000,
    "1Y":  365 * 24 * 3600 * 1000,
}

# ── Process-wide worker registry ──────────────────────────────────────────
# ROOT CAUSE OF CHART SPIKES (see notes above portfolio_history_snapshot):
# log_pnl() used to push *one asset's* cumulative_pnl into the shared,
# cross-asset portfolio history key. That single-asset value would land
# right next to genuinely correct cross-asset totals (written every 60 s by
# main.py's portfolio_snapshot_loop, and by the startup backfill), producing
# a sharp up/down spike on every single trade close.
#
# Fix: every MarketWorker registers itself here at construction time. Any
# code that needs "the total portfolio PnL right now" sums cumulative_pnl
# across every registered worker instead of using one worker's own value.
_worker_registry: List["MarketWorker"] = []
_worker_registry_lock = threading.Lock()


def _register_worker(worker: "MarketWorker") -> None:
    with _worker_registry_lock:
        if worker not in _worker_registry:
            _worker_registry.append(worker)


def _portfolio_total_pnl() -> float:
    """Sum cumulative_pnl across every live MarketWorker in this process.

    This is the single source of truth for 'total portfolio PnL right now' —
    every write into emiliano:portfolio:history must go through this (or the
    equivalent get_global_stats()-based total in main.py) rather than any
    one worker's own self.cumulative_pnl.
    """
    with _worker_registry_lock:
        workers = list(_worker_registry)
    total = 0.0
    for w in workers:
        try:
            v = getattr(w, "cumulative_pnl", 0.0)
            if _is_finite_number(v):
                total += v
        except Exception:
            continue
    return round(total, 4)


# ── Value / timestamp validation ──────────────────────────────────────────

def _is_finite_number(v: Any) -> bool:
    """True only for real, finite int/float values. Rejects None, NaN, Inf,
    bool, strings, etc. Used to keep corrupted values out of persisted
    history and to reject them again at read time as a second line of
    defense."""
    if isinstance(v, bool):
        return False
    if not isinstance(v, (int, float)):
        return False
    try:
        return math.isfinite(float(v))
    except Exception:
        return False


def sanitize_portfolio_history(
    points: List[Dict],
    *,
    drop_isolated_spikes: bool = True,
    collapse_flat_runs: bool = False,
    flat_epsilon: float = PORTFOLIO_FLAT_EPSILON,
    min_flat_run: int = 2,
) -> List[Dict]:
    """
    Single source of truth for cleaning a portfolio-history point list before
    it is either persisted to Redis or served to the chart.

    Guarantees on the returned list:
      • Every point has a finite numeric 't' (int, unix ms) and 'v' (float).
      • No two points share the same 't' (last-write-wins on duplicates).
      • Strictly increasing 't' (i.e. chronological order is enforced, not
        just sorted — true duplicates are already gone by the time we sort).
      • Optionally drops "isolated spikes": a single point whose value jumps
        far away from both neighbors and then jumps right back, which is the
        exact signature of a one-off corrupted write landing between two
        otherwise-correct totals.
      • Optionally collapses "flat runs": two or more consecutive points
        whose values are all within flat_epsilon of each other are reduced
        to just their first and last point. Two endpoints fully describe a
        flat horizontal segment, so this is lossless for charting
        while sharply bounding storage/payload growth during long periods
        with no PnL change.

    This function is intentionally conservative — genuine, large, sustained
    portfolio swings are never removed or altered, only redundant duplicate
    points and one-off corrupted spikes.
    """
    cleaned: Dict[int, float] = {}
    for p in points:
        if not isinstance(p, dict):
            continue
        t_raw = p.get("t")
        v_raw = p.get("v")
        if t_raw is None or v_raw is None:
            continue
        try:
            t_ms = int(t_raw)
        except (TypeError, ValueError):
            continue
        if not _is_finite_number(v_raw):
            continue
        if t_ms <= 0:
            continue
        v = round(float(v_raw), 4)
        # Last write wins for exact-duplicate timestamps (e.g. a retried
        # write, or a 60-s loop tick that lands on the same millisecond as a
        # trade-close write).
        cleaned[t_ms] = v

    ordered = [{"t": ts, "v": v} for ts, v in sorted(cleaned.items())]

    if drop_isolated_spikes and len(ordered) >= 3:
        ordered = _drop_isolated_spikes(ordered)

    if collapse_flat_runs and len(ordered) >= min_flat_run:
        ordered = compress_flat_runs(ordered, epsilon=flat_epsilon, min_run=min_flat_run)

    return ordered


def compress_flat_runs(points: List[Dict], epsilon: float = PORTFOLIO_FLAT_EPSILON,
                        min_run: int = 2) -> List[Dict]:
    """
    Collapse runs of 2-or-more consecutive points whose values are all within
    `epsilon` of the run's first value down to just the first and last point
    of that run.

    Why this is safe / lossless for rendering: a straight horizontal line
    segment is fully described by its two endpoints. Any interior points
    with (effectively) the same value add nothing visually — the SVG line
    drawn through 50 identical points looks pixel-identical to the line
    drawn through just the first and last of them. Short runs (1-2 points)
    are left untouched since there's nothing to compress.

    This is applied both when persisting to Redis (keeps stored history
    compact as it grows) and when serving /api/history (keeps the JSON
    payload small even if older, pre-compaction data is still in Redis).
    """
    n = len(points)
    if n < min_run:
        return list(points)

    out: List[Dict] = []
    i = 0
    while i < n:
        j = i
        base_v = points[i]["v"]
        while j + 1 < n and abs(points[j + 1]["v"] - base_v) <= epsilon:
            j += 1
        run_len = j - i + 1
        if run_len >= min_run:
            out.append(points[i])
            out.append(points[j])
        else:
            out.extend(points[i:j + 1])
        i = j + 1
    return out


def _drop_isolated_spikes(points: List[Dict]) -> List[Dict]:
    """
    Remove single-point spikes: a point whose value jumps far from BOTH
    neighbors, where the neighbors themselves are close to each other (i.e.
    the series jumps away and immediately jumps back). This is the exact
    shape produced by a stray bad write landing between two correct points,
    and it is conservative enough to leave real, sustained PnL moves intact.
    """
    if len(points) < 3:
        return points

    values = [p["v"] for p in points]
    diffs  = [abs(values[i] - values[i - 1]) for i in range(1, len(values))]
    diffs_sorted = sorted(diffs)
    mid = len(diffs_sorted) // 2
    median_step = diffs_sorted[mid] if diffs_sorted else 0.0

    # Floor so that a perfectly flat or near-flat series doesn't make the
    # spike threshold collapse to (near) zero.
    floor = max(5.0, median_step * 4)

    keep = [True] * len(points)
    for i in range(1, len(points) - 1):
        prev_v, cur_v, next_v = values[i - 1], values[i], values[i + 1]
        jump_in  = abs(cur_v - prev_v)
        jump_out = abs(next_v - cur_v)
        settle   = abs(next_v - prev_v)
        if jump_in < floor or jump_out < floor:
            continue
        # Both surrounding jumps are large, but the series basically returns
        # to where it started → this point is an isolated spike.
        if settle <= max(jump_in, jump_out) * 0.35:
            keep[i] = False

    return [p for p, k in zip(points, keep) if k]

# Timestamp format written by log_pnl() via datetime.now().strftime(...)
_TS_PRIMARY = "%Y-%m-%d %H:%M:%S"

# Additional formats found in older records or alternative paths.
_TS_FORMATS: List[str] = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S%z",
]


def _parse_ts(ts_str: str) -> Optional[int]:
    """Parse a trade timestamp string → Unix milliseconds. Returns None on failure."""
    if not ts_str:
        return None
    for fmt in _TS_FORMATS:
        try:
            dt = datetime.strptime(ts_str, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue
    try:
        dt = datetime.fromisoformat(ts_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


def _load_trades_for_asset(asset: str, window: str = "5m") -> List[Dict]:
    """
    Return the full trade list for one worker from Redis (primary) or local JSON.
    Falls back to legacy per-asset keys when window-scoped data is absent.
    """
    redis_key = f"emiliano:{asset.lower()}:{window.lower()}:trades"
    if _redis_available:
        trades = redis_get_json(redis_key)
        if trades and isinstance(trades, list) and len(trades) > 0:
            return trades
        legacy = redis_get_json(f"emiliano:{asset.lower()}:trades")
        if legacy and isinstance(legacy, list) and len(legacy) > 0:
            return legacy

    fp = asset_pnl_filename(asset, window)
    try:
        if os.path.exists(fp):
            with open(fp, "r", encoding="utf-8") as f:
                data = json.load(f)
            trades = data.get("trades", [])
            if trades:
                return trades
    except Exception as e:
        print(f"⚠️ [backfill] Could not read {fp}: {e}")

    legacy_fp = f"{asset.lower()}_pnl_history.json"
    try:
        if os.path.exists(legacy_fp):
            with open(legacy_fp, "r", encoding="utf-8") as f:
                data = json.load(f)
            trades = data.get("trades", [])
            if trades:
                return trades
    except Exception as e:
        print(f"⚠️ [backfill] Could not read {legacy_fp}: {e}")
    return []


def _build_equity_curve(all_trades_by_asset: Dict[str, List[Dict]]) -> List[Dict]:
    """
    Given {asset: [trade, ...]}, build a time-sorted portfolio equity curve.

    Algorithm:
      1. Tag every trade with its asset and parse its timestamp.
      2. Sort all events globally by timestamp.
      3. Walk the events, maintaining each asset's last known cumulative PnL.
      4. At each event emit {t: unix_ms, v: sum_of_all_asset_pnls}.

    The result is a list of {"t": unix_ms, "v": float} dicts, oldest-first.
    Duplicate timestamps are de-duplicated (last value wins).
    """
    events: List[Tuple[int, str, float]] = []  # (unix_ms, asset, cumulative_pnl)

    for asset, trades in all_trades_by_asset.items():
        for trade in trades:
            cum = trade.get("cumulative_pnl")
            if cum is None or not _is_finite_number(cum):
                continue
            ts_ms = _parse_ts(trade.get("timestamp", ""))
            if ts_ms is None:
                continue
            events.append((ts_ms, asset, float(cum)))

    if not events:
        return []

    events.sort(key=lambda e: e[0])

    asset_pnl: Dict[str, float] = {wc.key: 0.0 for wc in WORKER_CONFIGS}
    seen_ts: Dict[int, float]   = {}

    for ts_ms, asset, cum_pnl in events:
        asset_pnl[asset] = cum_pnl
        total = round(sum(asset_pnl.values()), 4)
        seen_ts[ts_ms] = total          # last-write-wins for duplicate timestamps

    curve = [{"t": ts, "v": v} for ts, v in sorted(seen_ts.items())]
    curve = sanitize_portfolio_history(curve, drop_isolated_spikes=False)
    return curve


def portfolio_history_backfill() -> int:
    """
    Idempotent backfill: reads all existing emiliano:{asset}:trades data from
    Redis and reconstructs emiliano:portfolio:history.

    Returns the number of backfill points written (0 = nothing to do / no data).

    Idempotency guarantee
    ─────────────────────
    • Count total trade records across all assets in Redis (N_trades).
    • Read existing portfolio history length (N_hist).
    • If N_hist >= N_trades → history already covers every trade → skip.
    • Otherwise rebuild the full curve and MERGE:
        – Points whose timestamps already exist in history are NOT overwritten
          so any manually-entered or live-snapshot points are preserved.
        – New trade-derived points are inserted into the correct chronological
          position.
    """
    if not _redis_available:
        print("ℹ️  [backfill] Redis not available — skipping portfolio history backfill.")
        return 0

    print("🔄 [backfill] Inspecting existing Redis trade data...")

    # Step 1: load all trade records
    all_trades: Dict[str, List[Dict]] = {}
    total_trade_count = 0
    for wc in WORKER_CONFIGS:
        trades = _load_trades_for_asset(wc.asset, wc.window)
        all_trades[wc.key] = trades
        print(f"  [{wc.asset.upper()} {wc.window}] {len(trades)} trade records found")
        total_trade_count += len(trades)

    if total_trade_count == 0:
        print("ℹ️  [backfill] No trade records found in Redis — nothing to backfill.")
        return 0

    # Step 2: check how many history points already exist
    existing_history: List[Dict] = redis_get_json(PORTFOLIO_HISTORY_KEY) or []
    existing_count = len(existing_history)

    if existing_count >= total_trade_count:
        print(f"✅ [backfill] Portfolio history already has {existing_count} points "
              f"covering {total_trade_count} trades — skipping rebuild.")
        return 0

    print(f"📊 [backfill] History has {existing_count} pts, trades have {total_trade_count} "
          f"records — rebuilding equity curve...")

    # Step 3: build the full curve from trade records
    backfill_curve = _build_equity_curve(all_trades)
    if not backfill_curve:
        print("⚠️  [backfill] Could not build equity curve (no parseable timestamps).")
        return 0

    # Step 4: merge with existing (already-sanitized) history
    # Existing points that post-date the last backfill point (live snapshots
    # recorded since previous deploy) are preserved; backfill replaces older pts.
    existing_history = sanitize_portfolio_history(
        existing_history, drop_isolated_spikes=False, collapse_flat_runs=True,
    )
    last_backfill_t = backfill_curve[-1]["t"] if backfill_curve else 0
    live_tail = [p for p in existing_history if p["t"] > last_backfill_t]

    merged: Dict[int, float] = {}
    for p in backfill_curve:
        merged[p["t"]] = p["v"]
    for p in live_tail:
        merged[p["t"]] = p["v"]    # live points win on any overlap

    final_curve = sanitize_portfolio_history(
        [{"t": ts, "v": v} for ts, v in sorted(merged.items())],
        drop_isolated_spikes=False,
        collapse_flat_runs=True,
    )

    # Cap to max points
    if len(final_curve) > MAX_HISTORY_POINTS:
        final_curve = final_curve[-MAX_HISTORY_POINTS:]

    # Step 5: write back (best-effort backup of the pre-backfill state so a
    # bad rebuild can always be rolled back manually)
    if existing_history:
        redis_set_json(f"{PORTFOLIO_HISTORY_KEY}:backup:pre_backfill", existing_history)

    ok = redis_set_json(PORTFOLIO_HISTORY_KEY, final_curve)
    if ok:
        print(f"✅ [backfill] Wrote {len(final_curve)} portfolio history points to Redis "
              f"({len(backfill_curve)} from trades + {len(live_tail)} live tail).")
    else:
        print("❌ [backfill] Redis write failed.")
        return 0

    return len(backfill_curve)


# Serializes all read-modify-write cycles against PORTFOLIO_HISTORY_KEY within
# this process. log_pnl() (per-trade writes) and portfolio_snapshot_loop in
# main.py (60-s heartbeat writes) both call portfolio_history_snapshot(), and
# without this lock two near-simultaneous calls could each read the same
# "existing" list, then write back, with one call's point silently lost
# (a classic lost-update race). A threading.Lock (not asyncio.Lock) is used
# deliberately: log_pnl is a plain synchronous function and may be called
# from sync contexts (e.g. the cleanup/migration script) as well as from
# inside async methods, so the lock must work in both.
#
# Note on scope: this protects against races *within a single process*. If
# this dashboard is ever scaled to more than one Render instance writing the
# same Redis key, a true cross-process lock (Redis MULTI/Lua, or a proper
# distributed mutex) would be required — the lightweight Upstash REST client
# used here only exposes plain GET/SET, not atomic compare-and-swap.
_history_write_lock = threading.Lock()

# Tracks the most recent round-key written per (asset, slug) so a duplicate
# log_pnl() call for the same market round (e.g. a retried exit handler)
# cannot double-write a snapshot for that round.
_recent_round_writes: Dict[str, float] = {}
_ROUND_DEDUP_TTL_SEC = 600  # 10 minutes — far longer than one 5-min round


def _round_already_written(round_key: Optional[str]) -> bool:
    if not round_key:
        return False
    now = t.time()
    # Opportunistically prune old entries so this dict never grows unbounded.
    expired = [k for k, ts in _recent_round_writes.items() if now - ts > _ROUND_DEDUP_TTL_SEC]
    for k in expired:
        _recent_round_writes.pop(k, None)
    return round_key in _recent_round_writes


def _mark_round_written(round_key: Optional[str]) -> None:
    if round_key:
        _recent_round_writes[round_key] = t.time()


def _is_flat_update(last_point: Optional[Dict], new_v: float, now_ms: int) -> bool:
    """
    True when this write would be a redundant duplicate of the last stored
    point: the value hasn't meaningfully changed AND the heartbeat interval
    hasn't elapsed yet. This is the core of the "only write when something
    actually changed" fix — see PORTFOLIO_FLAT_EPSILON / PORTFOLIO_HEARTBEAT_SEC
    above for the full rationale.
    """
    if not last_point:
        return False
    value_unchanged = abs(new_v - last_point["v"]) <= PORTFOLIO_FLAT_EPSILON
    heartbeat_due = (
        PORTFOLIO_HEARTBEAT_SEC > 0
        and (now_ms - last_point["t"]) >= (PORTFOLIO_HEARTBEAT_SEC * 1000)
    )
    return value_unchanged and not heartbeat_due


def portfolio_history_snapshot(total_pnl: float, round_key: Optional[str] = None) -> bool:
    """
    Record one live portfolio-equity observation.

    Called by MarketWorker.log_pnl() right after every completed trade so the
    chart updates the moment a market round closes — no background flush delay.

    Also called by main.py's portfolio_snapshot_loop every 60 s during normal
    operation so the curve stays continuous even in idle periods with no trades.

    Parameters
    ──────────
    total_pnl : the TOTAL portfolio PnL across every asset right now — never
                a single asset's own cumulative_pnl. Callers must use
                _portfolio_total_pnl() (or main.py's get_global_stats()
                total) to compute this.
    round_key : optional unique key (e.g. "{asset}:{slug}") identifying the
                market round this write corresponds to. When provided, a
                second call with the same round_key within
                _ROUND_DEDUP_TTL_SEC is ignored — this is the "only one valid
                snapshot per market round" safeguard.

    IMPORTANT — this does NOT always write a new point. A new {t, v} point is
    only ever persisted to Redis when:
        (a) the value changed by more than PORTFOLIO_FLAT_EPSILON since the
            last stored point, or
        (b) at least PORTFOLIO_HEARTBEAT_SEC has elapsed since the last
            stored point (so the curve keeps visibly extending to "now"
            during long flat stretches instead of stopping dead).
    Calls that are pure no-ops because of (a)/(b) failing still return True —
    "nothing needed to change" is a success, not a failure.

    Returns True on success (including no-op skips for duplicate round_keys
    or flat/unchanged values), False on a real failure.
    """
    if not _is_finite_number(total_pnl):
        print(f"⚠️ portfolio_history_snapshot rejected non-finite value: {total_pnl!r}")
        return False

    if _round_already_written(round_key):
        print(f"ℹ️ portfolio_history_snapshot skipped duplicate round write for {round_key!r}")
        return True

    if not _redis_available:
        _mark_round_written(round_key)
        return False

    with _history_write_lock:
        try:
            existing: List[Dict] = redis_get_json(PORTFOLIO_HISTORY_KEY) or []
            existing = sanitize_portfolio_history(
                existing, drop_isolated_spikes=False, collapse_flat_runs=True,
            )
            now_ms = int(t.time() * 1000)
            new_v  = round(float(total_pnl), 4)
            last_point = existing[-1] if existing else None

            # Avoid near-duplicate timestamps: if the last point is within
            # 2 s just update it in place rather than adding a near-zero-
            # width vertical segment. This takes priority over the flat-
            # update check below since it represents the same instant, not
            # a separate observation.
            if last_point and (now_ms - last_point["t"]) < 2000:
                existing[-1]["v"] = new_v
                changed = True
            elif _is_flat_update(last_point, new_v, now_ms):
                # ── THE FIX ───────────────────────────────────────────────
                # Value is unchanged from the last stored point and the
                # heartbeat interval hasn't elapsed — skip the write
                # entirely. No Redis round-trip, no new duplicate point.
                changed = False
            else:
                existing.append({"t": now_ms, "v": new_v})
                changed = True

            if not changed:
                _mark_round_written(round_key)
                return True

            # Re-sanitize AND collapse any flat runs before writing, so
            # stored history stays compact as it grows even in edge cases
            # the write-gate above doesn't catch (e.g. historical data
            # merged in from elsewhere).
            existing = sanitize_portfolio_history(
                existing, drop_isolated_spikes=False, collapse_flat_runs=True,
            )

            if len(existing) > MAX_HISTORY_POINTS:
                existing = existing[-MAX_HISTORY_POINTS:]

            ok = redis_set_json(PORTFOLIO_HISTORY_KEY, existing)
            if ok:
                _mark_round_written(round_key)
            return ok
        except Exception as e:
            print(f"⚠️ portfolio_history_snapshot error: {e}")
            return False


def filter_points_for_period(
    points: List[Dict],
    period: str,
    *,
    now_ms: Optional[int] = None,
) -> List[Dict]:
    """
    Rolling-window filter for chart data.

    Includes points within the period lookback (24h / 7d / 30d / 365d).
    Does NOT prepend anchor points before the window — x-axis starts at the
    first real data timestamp so new sessions are not padded with empty time.
    """
    now_ms = now_ms if now_ms is not None else int(t.time() * 1000)
    cleaned = sanitize_portfolio_history(
        points, drop_isolated_spikes=True, collapse_flat_runs=True,
    )
    if not cleaned:
        return []

    period_key = period.upper()
    if period_key == "ALL":
        return cleaned

    period_ms = CHART_PERIOD_MS.get(period_key)
    if period_ms is None:
        return cleaned

    cutoff = now_ms - period_ms
    filtered = [p for p in cleaned if p["t"] >= cutoff]
    if not filtered:
        return [cleaned[-1]]
    return filtered


def chart_time_domain(points: List[Dict]) -> Tuple[int, int]:
    """Data-driven x-axis bounds: [first timestamp, last timestamp]."""
    if not points:
        return (0, 0)
    return (int(points[0]["t"]), int(points[-1]["t"]))


def downsample_chart_points(points: List[Dict], max_points: int = 600) -> List[Dict]:
    """Downsample while preserving every value-change anchor."""
    n = len(points)
    if n <= max_points:
        return list(points)

    keep_idx = {0, n - 1}
    for i in range(1, n):
        if abs(points[i]["v"] - points[i - 1]["v"]) > PORTFOLIO_FLAT_EPSILON:
            keep_idx.add(i)

    anchors = [points[i] for i in sorted(keep_idx)]
    if len(anchors) <= max_points:
        return anchors

    step = max(1, len(anchors) // max_points)
    sampled = anchors[::step]
    if sampled[-1]["t"] != points[-1]["t"]:
        sampled.append(points[-1])
    return sampled


def prepare_chart_history(
    points: List[Dict],
    period: str = "1D",
    *,
    now_ms: Optional[int] = None,
    max_points: int = 600,
) -> List[Dict]:
    """
    Sanitize, period-filter, and downsample for /api/history.

    No synthetic gap-fill. X-axis domain is derived client-side from the
    first and last returned timestamps (data-driven, not fixed 24h padding).
    """
    filtered = filter_points_for_period(points, period, now_ms=now_ms)
    return downsample_chart_points(filtered, max_points=max_points)


def portfolio_history_get(period: str = "ALL") -> List[Dict]:
    """
    Fetch portfolio history filtered to the requested period.
    period: '1D' | '1W' | '1M' | '1Y' | 'ALL'
    Returns list of {t: unix_ms, v: pnl} dicts, oldest-first, fully
    sanitized (validated, de-duplicated, strictly chronological, isolated
    single-point spikes removed, and long flat runs collapsed to their two
    boundary points).
    Always returns at least the most recent point as a baseline.
    """
    raw_pts: List[Dict] = redis_get_json(PORTFOLIO_HISTORY_KEY) or []
    all_pts = sanitize_portfolio_history(
        raw_pts, drop_isolated_spikes=True, collapse_flat_runs=True,
    )

    if not all_pts or period.upper() == "ALL":
        return all_pts

    period_ms: Optional[int] = {
        "1D":  24 * 3600 * 1000,
        "1W":  7  * 24 * 3600 * 1000,
        "1M":  30 * 24 * 3600 * 1000,
        "1Y":  365 * 24 * 3600 * 1000,
    }.get(period.upper())

    if period_ms is None:
        return all_pts

    cutoff   = int(t.time() * 1000) - period_ms
    filtered = [p for p in all_pts if p["t"] >= cutoff]

    # Always return at least one point so the chart has a left-edge anchor
    if not filtered and all_pts:
        filtered = [all_pts[-1]]

    return filtered


# ═════════════════════════════════════════════════════════════════════════════
# TRADE HISTORY — persistent BUY/SELL execution log (Positions / History tabs)
#
# Redis key  : emiliano:trade:history
# Value      : JSON list of execution records, oldest-first, capped at
#              MAX_TRADE_HISTORY entries.
#
# Each record:
#   { id, timestamp, timestamp_ms, asset, market, slug,
#     action ("buy"|"sell"|"redeem"), side ("YES"|"NO"),
#     price, size }
#
# Action semantics
# ────────────────
# buy    — opening entry (confirmed BUY fill)
# sell   — early exit (stop-loss, take-profit, manual cashout)
# redeem — held to market resolution / settlement
#
# Live writes : log_trade() → append_trade_history()
# Backfill    : trade_history_backfill() reads emiliano:{asset}:trades and
#               local {asset}_pnl_history.json to synthesize BUY/SELL rows
#               from stored round details on startup.
# ═════════════════════════════════════════════════════════════════════════════

TRADE_HISTORY_KEY     = "emiliano:trade:history"
TRADE_HISTORY_LOCAL   = "trade_history.json"
MAX_TRADE_HISTORY     = 500
_trade_history_lock   = threading.Lock()
_trade_history_ids: set = set()   # in-process dedup cache


def _normalize_trade_action(action: str) -> str:
    """Normalize to buy | sell | redeem (lowercase)."""
    a = (action or "").lower().strip()
    if a in ("redeem", "redemption", "hodl", "settle", "settlement", "resolved"):
        return "redeem"
    if a in (
        "sell", "sell_exit", "exit", "cashout",
        "stop_loss", "take_profit", "manual_cashout",
    ):
        return "sell"
    if a in ("buy", "purchase", "entry"):
        return "buy"
    a_up = (action or "").upper()
    if a_up == "SELL":
        return "sell"
    if a_up == "BUY":
        return "buy"
    return "buy"


def _trade_record_id(
    asset: str, slug: str, action: str, side: str,
    timestamp_ms: int, price: float, size: float,
) -> str:
    return (
        f"{asset.lower()}:{slug}:{_normalize_trade_action(action)}:"
        f"{side}:{timestamp_ms}:{round(price, 4)}:{round(size, 4)}"
    )


def _load_trade_history_raw() -> List[Dict]:
    if _redis_available:
        data = redis_get_json(TRADE_HISTORY_KEY)
        if data and isinstance(data, list):
            return data
    try:
        if os.path.exists(TRADE_HISTORY_LOCAL):
            with open(TRADE_HISTORY_LOCAL, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                return data
    except Exception as e:
        print(f"⚠️ Could not read {TRADE_HISTORY_LOCAL}: {e}")
    return []


def _persist_trade_history(records: List[Dict]) -> bool:
    capped = records[-MAX_TRADE_HISTORY:]
    ok = False
    if _redis_available:
        ok = redis_set_json(TRADE_HISTORY_KEY, capped)
    try:
        temp = TRADE_HISTORY_LOCAL + ".tmp"
        with open(temp, "w", encoding="utf-8") as f:
            json.dump(capped, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp, TRADE_HISTORY_LOCAL)
        ok = True
    except Exception as e:
        print(f"⚠️ Local trade history write failed: {e}")
    return ok


def append_trade_history(record: Dict) -> bool:
    """
    Append one BUY/SELL execution record. Idempotent by record id.
    Returns True if stored (or already present), False on hard failure.
    """
    required = ("asset", "action", "side", "price", "size")
    if not all(record.get(k) is not None for k in required):
        return False

    ts_ms = record.get("timestamp_ms")
    if not ts_ms:
        ts_str = record.get("timestamp") or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        parsed = _parse_ts(ts_str)
        ts_ms = parsed if parsed else int(t.time() * 1000)

    asset  = str(record["asset"]).upper()
    slug   = str(record.get("slug") or asset.lower())
    action = _normalize_trade_action(str(record["action"]))
    side   = str(record["side"]).upper()
    price  = round(float(record["price"]), 4)
    size   = round(float(record["size"]), 4)

    if side not in ("YES", "NO") or size <= 0:
        return False
    if price < 0:
        return False
    # Losing redemption settles at $0 — valid for redeem only.
    if price <= 0 and action != "redeem":
        return False

    rec_id = record.get("id") or _trade_record_id(
        asset, slug, action, side, int(ts_ms), price, size,
    )

    entry = {
        "id":            rec_id,
        "timestamp":     record.get("timestamp") or datetime.fromtimestamp(
            ts_ms / 1000, tz=timezone.utc,
        ).strftime("%Y-%m-%d %H:%M:%S"),
        "timestamp_ms":  int(ts_ms),
        "asset":         asset,
        "market":        record.get("market") or f"{asset} Up or Down",
        "slug":          slug,
        "action":        action,
        "side":          side,
        "price":         price,
        "size":          size,
    }

    with _trade_history_lock:
        if rec_id in _trade_history_ids:
            return True
        existing = _load_trade_history_raw()
        if any(r.get("id") == rec_id for r in existing):
            _trade_history_ids.add(rec_id)
            return True
        existing.append(entry)
        if _persist_trade_history(existing):
            _trade_history_ids.add(rec_id)
            return True
    return False


def _normalize_history_record(rec: Dict) -> Dict:
    """Ensure every stored/read record uses buy | sell | redeem."""
    out = dict(rec)
    out["action"] = _normalize_trade_action(rec.get("action", "buy"))
    return out


def get_trade_history(limit: int = 10) -> List[Dict]:
    """Return the most recent `limit` trade records, newest first."""
    with _trade_history_lock:
        records = _load_trade_history_raw()
    for r in records:
        if r.get("id"):
            _trade_history_ids.add(r["id"])
    sorted_recs = sorted(records, key=lambda x: x.get("timestamp_ms", 0), reverse=True)
    return [_normalize_history_record(r) for r in sorted_recs[: max(1, limit)]]


def _synthesize_history_from_pnl_trade(trade: Dict, asset: str, window: str = "5m") -> List[Dict]:
    """Build BUY (+ optional SELL) rows from one emiliano:{asset}:{window}:trades entry."""
    out: List[Dict] = []
    details = trade.get("details") or {}
    side    = details.get("side") or details.get("bought_side") or trade.get("side")
    if not side:
        return out

    slug    = trade.get("slug") or asset
    market  = trade.get("market") or f"{asset.upper()} Up or Down"
    ts_str  = trade.get("timestamp") or ""
    ts_ms   = _parse_ts(ts_str) or int(t.time() * 1000)
    size    = float(details.get("size") or details.get("shares") or MIN_SHARES)

    entry_price_raw = details.get("entry_price")
    entry_price = None
    if entry_price_raw is not None:
        entry_price = float(entry_price_raw)
        if entry_price > 1.0:
            entry_price = entry_price / 100.0

    if entry_price and entry_price > 0:
        out.append({
            "asset": asset.upper(), "window": window, "slug": slug, "market": market,
            "action": "buy", "side": str(side).upper(),
            "price": entry_price, "size": size,
            "timestamp": ts_str, "timestamp_ms": max(1, ts_ms - 1000),
        })

    exit_price_raw = details.get("exit_price")
    exit_price = None
    if exit_price_raw is not None:
        exit_price = float(exit_price_raw)
        if exit_price > 1.0:
            exit_price = exit_price / 100.0

    outcome = (trade.get("type") or "").upper()
    if exit_price is not None and outcome in (
        "TAKE_PROFIT", "STOP_LOSS", "MANUAL_CASHOUT",
    ):
        out.append({
            "asset": asset.upper(), "window": window, "slug": slug, "market": market,
            "action": "sell", "side": str(side).upper(),
            "price": max(exit_price, 0.0), "size": size,
            "timestamp": ts_str, "timestamp_ms": ts_ms,
        })
    elif outcome == "HODL":
        market_outcome = details.get("outcome")
        won = market_outcome and str(market_outcome).upper() == str(side).upper()
        settle = 1.0 if won else 0.0
        out.append({
            "asset": asset.upper(), "window": window, "slug": slug, "market": market,
            "action": "redeem", "side": str(side).upper(),
            "price": settle, "size": size,
            "timestamp": ts_str, "timestamp_ms": ts_ms,
        })

    return out


def trade_history_backfill() -> int:
    """
    Idempotent backfill of emiliano:trade:history from existing PnL trade
    records. Returns count of newly appended records.
    """
    if not _redis_available and not os.path.exists(TRADE_HISTORY_LOCAL):
        pass  # still attempt local-only backfill below

    print("🔄 [trade-history] Inspecting existing trade records for backfill...")
    synthesized: List[Dict] = []
    for wc in WORKER_CONFIGS:
        for trade in _load_trades_for_asset(wc.asset, wc.window):
            synthesized.extend(_synthesize_history_from_pnl_trade(trade, wc.asset, wc.window))

    if not synthesized:
        print("ℹ️  [trade-history] No trade records to backfill from.")
        return 0

    synthesized.sort(key=lambda x: x.get("timestamp_ms", 0))
    added = 0
    for rec in synthesized:
        before = len(_trade_history_ids)
        if append_trade_history(rec):
            with _trade_history_lock:
                if len(_trade_history_ids) > before:
                    added += 1
    print(f"✅ [trade-history] Backfill complete — {added} new records added.")
    return added


def collect_open_positions(workers: List["MarketWorker"]) -> List[Dict]:
    """
    Return one snapshot per open position (FILLED state only).
    Duplicate ids are suppressed — at most one row per asset/market round.
    """
    seen: set = set()
    out: List[Dict] = []
    for worker in workers:
        snap = worker.get_position_snapshot()
        if not snap:
            continue
        pid = snap["id"]
        if pid in seen:
            continue
        seen.add(pid)
        out.append(snap)
    return out


def find_worker(
    workers: List["MarketWorker"], asset: str, window: str,
) -> Optional["MarketWorker"]:
    a = asset.strip().lower()
    w = window.strip().lower()
    for worker in workers:
        if worker.asset_type == a and worker.window_slug == w:
            return worker
    return None


def find_worker_by_asset(workers: List["MarketWorker"], asset: str) -> Optional["MarketWorker"]:
    """Backward-compatible lookup — returns first worker matching asset."""
    key = asset.strip().lower()
    for worker in workers:
        if worker.asset_type == key:
            return worker
    return None


# ═════════════════════════════════════════════════════════════════════════════
# BINANCE DEPTH SIGNAL  (display / context only — not a trade gate)
# ═════════════════════════════════════════════════════════════════════════════

class BinanceDepthSignal:
    _instances: Dict[str, "BinanceDepthSignal"] = {}
    _instance_lock = asyncio.Lock()

    @classmethod
    async def get_or_create(cls, symbol: str) -> "BinanceDepthSignal":
        async with cls._instance_lock:
            sym = symbol.upper()
            if sym not in cls._instances:
                inst = cls(sym)
                cls._instances[sym] = inst
                asyncio.create_task(inst._run())
            return cls._instances[sym]

    def __init__(self, symbol: str):
        self.symbol      = symbol
        self.imbalance   = 0.0
        self.momentum    = 0.0
        self.last_update = 0.0
        self._history: deque = deque(maxlen=8)
        self._running    = False

    @property
    def is_fresh(self) -> bool:
        return (t.time() - self.last_update) < BINANCE_STALE_CUTOFF_SECS

    @property
    def is_primed(self) -> bool:
        return self.is_fresh and abs(self.imbalance) >= BINANCE_PRIME_THRESHOLD

    @property
    def signal_label(self) -> str:
        if not self.is_fresh:
            return "STALE"
        if abs(self.imbalance) >= BINANCE_PRIME_THRESHOLD:
            return "STRONGLY BULL ↑" if self.imbalance > 0 else "STRONGLY BEAR ↓"
        if abs(self.imbalance) >= 0.10:
            return "MILDLY BULL ↑"   if self.imbalance > 0 else "MILDLY BEAR ↓"
        return "NEUTRAL"

    async def _run(self):
        if self._running:
            return
        self._running = True
        stream = f"{self.symbol.lower()}usdt@depth{BINANCE_DEPTH_LIMIT}@100ms"
        url    = f"wss://fstream.binance.com/stream?streams={stream}"
        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=15) as ws:
                    print(f"📡 [Binance WS] Connected: {stream}")
                    async for raw in ws:
                        msg  = json.loads(raw)
                        data = msg.get("data", msg)
                        bids = data.get("b", [])
                        asks = data.get("a", [])
                        if bids and asks:
                            self._process(bids, asks)
            except Exception as e:
                print(f"⚠️ [Binance WS] {self.symbol}: {e} — reconnecting in 3s")
                await asyncio.sleep(3)

    def _process(self, bids: list, asks: list):
        def weighted_vol(levels: list) -> float:
            tw = tv = 0.0
            for i, item in enumerate(levels[:20]):
                qty = float(item[1])
                w   = 1.0 / (i + 1) ** 0.6
                tv += qty * w
                tw += w
            return tv / tw if tw > 0 else 0.0

        bid_v = weighted_vol(bids)
        ask_v = weighted_vol(asks)
        total = bid_v + ask_v
        if total <= 0:
            return
        raw = (bid_v - ask_v) / total
        self._history.append(raw)
        self.last_update = t.time()
        if len(self._history) >= 4:
            recent         = list(self._history)[-4:]
            self.imbalance = sum(recent) / len(recent)
            self.momentum  = recent[-1] - recent[0]
        else:
            self.imbalance = raw
            self.momentum  = 0.0


# ═════════════════════════════════════════════════════════════════════════════
# ACCOUNT SERVICE — GLOBAL, SINGLE-INSTANCE, BOT-LEVEL
# ═════════════════════════════════════════════════════════════════════════════
#
# Everything in this class represents work that is tied to the WALLET / ACCOUNT,
# not to any individual market. There is exactly one AccountService for the
# entire bot process, constructed once in main() and shared by reference into
# every MarketWorker. Nothing in here is duplicated per asset.
#
# Responsibilities:
#   • Single Web3 connection + single ClobClient (auth derived/created once)
#   • One-time startup wallet audit (balances + on-chain approvals)
#   • Shared order-placement / order-status / cancel helpers used by all workers
#   • Shared on-chain share-balance lookups used by all workers
#   • Global periodic PnL merge background task (+ on-demand merge_now())
#
# Per-market logic (price listening, entry/exit, TP/SL, per-asset PnL bookkeeping)
# stays in MarketWorker — see below.
# ═════════════════════════════════════════════════════════════════════════════

class AccountService:
    def __init__(self):
        pk     = os.getenv("PRIVATE_KEY")
        funder = os.getenv("FUNDER_ADDRESS")
        if not pk or not funder:
            raise ValueError("Missing PRIVATE_KEY or FUNDER_ADDRESS in .env")

        self.w3             = Web3(Web3.HTTPProvider(POLYGON_RPC))
        self.wallet_address = self.w3.to_checksum_address(funder)
        self.signer_address = self.w3.eth.account.from_key(pk).address
        self.private_key    = pk

        print(f"Signer : {self.signer_address}")
        print(f"Funder : {self.wallet_address}")

        _l1_client = ClobClient(
            host=HOST, key=pk, chain_id=137, funder=funder, signature_type=3  # type: ignore
        )

        print("🔑 Authenticating with Polymarket (V2)...")
        try:
            raw_creds = _l1_client.derive_api_key()
            if raw_creds is None or not getattr(raw_creds, 'api_key', None):
                print("⚠️  No existing key found — creating new one...")
                raw_creds = _l1_client.create_api_key()
            print("✅ API Authentication Successful")
        except Exception as e:
            print(f"❌ Authentication Failed: {e}")
            raise

        # Single shared ClobClient — used by every MarketWorker for order
        # placement / status / cancellation. There is only ever one of these
        # for the whole process, regardless of how many assets are tracked.
        self.client = ClobClient(
            host=HOST, key=pk, chain_id=137, funder=funder,
            signature_type=3, creds=raw_creds,  # type: ignore
        )

        # Guards so audit/init work can never accidentally run twice even if
        # something calls these methods more than once.
        self._audited = False
        self._merge_task: Optional[asyncio.Task] = None

    # ── On-chain approvals (account-level — run once for the whole wallet) ──

    def set_approvals(self, operator_address: str, label: str):
        print(f"⏳ Sending approval for {label}...")
        ctf_abi = [
            {"inputs": [{"name": "operator", "type": "address"}, {"name": "approved", "type": "bool"}],
             "name": "setApprovalForAll", "outputs": [], "type": "function"}
        ]
        ctf_contract   = self.w3.eth.contract(
            address=self.w3.to_checksum_address(CTF_CONTRACT), abi=ctf_abi)
        signer_account = self.w3.eth.account.from_key(os.getenv("PRIVATE_KEY"))
        signer_address = signer_account.address
        try:
            gas_balance = self.w3.eth.get_balance(signer_address)
            if gas_balance < self.w3.to_wei(0.01, 'ether'):
                print(f"❌ Signer ({signer_address}) needs at least 0.01 POL for gas.")
                return False
            current_gas_price   = self.w3.eth.gas_price
            increased_gas_price = Wei(int(current_gas_price * 1.2))
            tx_params: TxParams = {
                'from':     signer_address,
                'nonce':    self.w3.eth.get_transaction_count(signer_address, "pending"),
                'gas':      100000,
                'gasPrice': increased_gas_price,
                'chainId':  137,
            }
            tx = ctf_contract.functions.setApprovalForAll(
                self.w3.to_checksum_address(operator_address), True
            ).build_transaction(tx_params)
            signed_tx = self.w3.eth.account.sign_transaction(tx, os.getenv("PRIVATE_KEY"))
            tx_hash   = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)
            print(f"✅ Approval sent! Hash: {tx_hash.hex()}")
            self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120, poll_latency=2.0)
            return True
        except Exception as e:
            print(f"❌ Approval failed for {label}: {e}")
            return False

    def get_pol_balance(self):
        balance_wei = self.w3.eth.get_balance(self.wallet_address)
        return float(self.w3.from_wei(balance_wei, 'ether'))

    def check_and_approve_pusd(self, spender_address: str, label: str):
        pusd_abi = [
            {"inputs": [{"name": "owner",   "type": "address"}, {"name": "spender", "type": "address"}],
             "name": "allowance", "outputs": [{"name": "", "type": "uint256"}], "type": "function"},
            {"inputs": [{"name": "spender", "type": "address"}, {"name": "amount",  "type": "uint256"}],
             "name": "approve",   "outputs": [{"name": "", "type": "bool"}],    "type": "function"},
        ]
        pusd_contract = self.w3.eth.contract(
            address=self.w3.to_checksum_address(PUSD_ADDRESS), abi=pusd_abi)
        spender           = self.w3.to_checksum_address(spender_address)
        current_allowance = pusd_contract.functions.allowance(self.wallet_address, spender).call()
        if current_allowance < 1_000_000:
            print(f"🔓 [GAS] Approving pUSD for {label}...")
            tx_params = cast(TxParams, {
                'from':     self.signer_address,
                'nonce':    self.w3.eth.get_transaction_count(self.signer_address, "pending"),
                'gas':      60000,
                'gasPrice': int(self.w3.eth.gas_price * 1.2),
                'chainId':  137,
            })
            raw_tx    = pusd_contract.functions.approve(spender, 2**256 - 1).build_transaction(tx_params)
            signed_tx = self.w3.eth.account.sign_transaction(raw_tx, os.getenv("FUNDER_PRIVATE_KEY"))
            tx_hash   = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)
            self.w3.eth.wait_for_transaction_receipt(tx_hash, poll_latency=2.0)
            print(f"✅ {label} pUSD: Approved.")
        else:
            print(f"✅ {label} pUSD: Already Approved")

    def check_and_approve_shares(self, operator_address: str, label: str):
        ctf_abi = [
            {"inputs": [{"name": "account",  "type": "address"}, {"name": "operator", "type": "address"}],
             "name": "isApprovedForAll", "outputs": [{"name": "", "type": "bool"}], "type": "function"},
            {"inputs": [{"name": "operator", "type": "address"}, {"name": "approved", "type": "bool"}],
             "name": "setApprovalForAll", "outputs": [], "type": "function"},
        ]
        ctf_contract = self.w3.eth.contract(
            address=self.w3.to_checksum_address(CTF_CONTRACT), abi=ctf_abi)
        operator    = self.w3.to_checksum_address(operator_address)
        is_approved = ctf_contract.functions.isApprovedForAll(self.wallet_address, operator).call()
        if not is_approved:
            print(f"🔓 [POL TX] Funder granting {label} permission to handle shares...")
            funder_pk = os.getenv("FUNDER_PRIVATE_KEY")
            if not funder_pk:
                print(f"❌ Cannot approve {label}. Add FUNDER_PRIVATE_KEY to .env.")
                return
            tx_params: TxParams = {
                'from':     self.signer_address,
                'nonce':    self.w3.eth.get_transaction_count(self.signer_address, "pending"),
                'gas':      120000,
                'gasPrice': Wei(int(self.w3.eth.gas_price * 1.5)),
                'chainId':  137,
            }
            raw_tx    = ctf_contract.functions.setApprovalForAll(operator, True).build_transaction(tx_params)
            signed_tx = self.w3.eth.account.sign_transaction(raw_tx, funder_pk)
            tx_hash   = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)
            print(f"⏳ Confirming {label} approval... Hash: {tx_hash.hex()}")
            self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
            print(f"✅ {label} Shares Enabled.")
        else:
            print(f"✅ {label} Shares: Already Approved")

    def run_wallet_audit(self) -> bool:
        """
        Account-level wallet audit: balance check + (if not DRY_MODE) on-chain
        approvals. This runs EXACTLY ONCE for the entire bot process — at
        startup, before any MarketWorker begins trading — regardless of how
        many assets are being tracked. It must never be called per-asset.
        """
        if self._audited:
            print("ℹ️  Wallet audit already completed this session — skipping duplicate run.")
            return True

        min_abi = [
            {"constant": True, "inputs": [{"name": "_owner", "type": "address"}],
             "name": "balanceOf", "outputs": [{"name": "balance", "type": "uint256"}], "type": "function"},
            {"constant": True, "inputs": [], "name": "decimals",
             "outputs": [{"name": "", "type": "uint8"}], "type": "function"},
        ]
        pusd_contract  = self.w3.eth.contract(
            address=self.w3.to_checksum_address(PUSD_ADDRESS), abi=min_abi)
        usdce_contract = self.w3.eth.contract(
            address=self.w3.to_checksum_address(USDC_E), abi=min_abi)

        signer_pusd  = pusd_contract.functions.balanceOf(self.signer_address).call() / 10**6
        funder_pusd  = pusd_contract.functions.balanceOf(self.wallet_address).call() / 10**6
        funder_usdce = usdce_contract.functions.balanceOf(self.wallet_address).call() / 10**6

        print(f"\n💵 Signer pUSD Balance : {signer_pusd:.2f} pUSD")
        print(f"💵 Funder pUSD Balance : {funder_pusd:.2f} pUSD")
        print(f"💵 Funder USDC.e       : {funder_usdce:.2f} USDC.e  (legacy — not used as collateral)")

        if not DRY_MODE:
            operators = [
                (STANDARD_EXCHANGE, "Main Exchange"),
                (NEG_RISK_EXCHANGE, "Neg-Risk Exchange"),
                (NEG_RISK_ADAPTER,  "Neg-Risk Adapter"),
            ]
            for addr, label in operators:
                self.check_and_approve_pusd(addr, label)
                self.check_and_approve_shares(addr, label)

        self._audited = True
        return True

    # ── Shared on-chain balance helpers (used by every MarketWorker) ────────

    async def get_onchain_share_balance_async(self, token_id: str, retries: int = 3) -> float:
        abi = [{"inputs": [{"name": "account", "type": "address"}, {"name": "id", "type": "uint256"}],
                "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}], "type": "function"}]
        contract = self.w3.eth.contract(
            address=self.w3.to_checksum_address(CTF_CONTRACT), abi=abi)
        for attempt in range(retries):
            try:
                raw_balance = contract.functions.balanceOf(
                    self.wallet_address, int(token_id)).call()
                return float(raw_balance / 10**6)
            except Exception as e:
                if attempt < retries - 1:
                    wait = (attempt + 1) * 2
                    print(f"⚠️ RPC Glitch: {e}. Retrying in {wait}s...")
                    await asyncio.sleep(wait)
                else:
                    print(f"❌ CRITICAL: On-chain balance check failed after {retries} attempts.")
                    return -1.0
        return -1.0

    def get_onchain_share_balance(self, token_id: str, retries: int = 3) -> float:
        """Synchronous version kept for non-async call sites (approvals, audits)."""
        abi = [{"inputs": [{"name": "account", "type": "address"}, {"name": "id", "type": "uint256"}],
                "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}], "type": "function"}]
        contract = self.w3.eth.contract(
            address=self.w3.to_checksum_address(CTF_CONTRACT), abi=abi)
        for attempt in range(retries):
            try:
                raw_balance = contract.functions.balanceOf(
                    self.wallet_address, int(token_id)).call()
                return float(raw_balance / 10**6)
            except Exception as e:
                if attempt < retries - 1:
                    wait = (attempt + 1) * 2
                    print(f"⚠️ RPC Glitch: {e}. Retrying in {wait}s...")
                    t.sleep(wait)
                else:
                    print(f"❌ CRITICAL: On-chain balance check failed after {retries} attempts.")
                    return -1.0
        return -1.0

    async def merge_shares(self, active_market: Optional[Dict[str, Any]], amount_to_merge: float):
        """Merge YES+NO shares back into pUSD on-chain. Account-level operation —
        takes the market dict explicitly since the position itself is per-market."""
        if active_market is None:
            print("❌ Merge aborted: No active market metadata found.")
            return
        CTF_MAIN = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
        ctf_abi = [{
            "name": "mergePositions", "type": "function",
            "inputs": [
                {"name": "collateralToken",    "type": "address"},
                {"name": "parentCollectionId", "type": "bytes32"},
                {"name": "conditionId",        "type": "bytes32"},
                {"name": "partition",          "type": "uint256[]"},
                {"name": "amount",             "type": "uint256"},
            ],
            "outputs": [],
        }]
        target_address = self.w3.to_checksum_address(CTF_MAIN)
        contract       = self.w3.eth.contract(address=target_address, abi=ctf_abi)
        raw_amount     = int(amount_to_merge * 10**6)
        try:
            parent_id = "0x" + "0" * 64
            partition = [1, 2]
            cond_id   = active_market.get('condition_id')
            if not cond_id:
                print("❌ Market metadata missing condition_id.")
                return
            nonce = self.w3.eth.get_transaction_count(self.signer_address, "pending")
            tx = contract.functions.mergePositions(
                PUSD_ADDRESS, parent_id, cond_id, partition, raw_amount
            ).build_transaction({
                'from':     self.signer_address,
                'gas':      180000,
                'gasPrice': self.w3.eth.gas_price,
                'nonce':    nonce,
            })
            signed  = self.w3.eth.account.sign_transaction(tx, self.private_key)
            tx_hash = self.w3.eth.send_raw_transaction(signed.rawTransaction)
            print(f"♻️ Capital Recycled (pUSD)! Hash: {tx_hash.hex()}")
            return tx_hash
        except Exception as e:
            print(f"❌ On-chain Merge Error: {e}")

    # ── Shared order execution helpers (used by every MarketWorker) ─────────

    def create_and_post_order(
        self,
        side_str: str,
        price: float,
        size: float,
        token_id: str,
        order_type: str = "GTC",
    ):
        """Build + submit an order through the single shared ClobClient."""
        order_args   = OrderArgs(price=price, size=size, side=side_str, token_id=token_id)
        signed_order = self.client.create_order(order_args)
        ot = parse_order_type(order_type)
        resp         = self.client.post_order(signed_order, cast(OrderType, ot))
        return resp

    def get_order_status(self, order_id: str):
        return self.client.get_order(order_id)

    def cancel_order(self, order_id: str):
        payload = OrderPayload(orderID=order_id)
        return self.client.cancel_order(payload)

    # ── Global periodic PnL merge (background scheduler, single instance) ───

    def start_pnl_merge_scheduler(self):
        """
        Launches the recurring PnL-merge background task exactly once for the
        whole bot. This task runs on a fixed interval (MERGE_INTERVAL_SECONDS)
        independent of any individual trade completing — it is the bot-level
        equivalent of "account/PnL updates" and must not be created per asset.
        """
        if self._merge_task is not None and not self._merge_task.done():
            print("ℹ️  PnL merge scheduler already running — skipping duplicate start.")
            return self._merge_task
        self._merge_task = asyncio.create_task(self._pnl_merge_loop())
        return self._merge_task

    async def _pnl_merge_loop(self):
        while True:
            await asyncio.sleep(MERGE_INTERVAL_SECONDS)
            try:
                merge_all_pnl(send_telegram_notify=False)
            except Exception as e:
                print(f"⚠️ Scheduled PnL merge failed: {e}")

    def merge_now(self, send_telegram_notify: bool = False):
        """On-demand merge, called by a MarketWorker right after a trade closes."""
        merge_all_pnl(send_telegram_notify=send_telegram_notify)

# ═════════════════════════════════════════════════════════════════════════════
# MARKET WORKER — ONE INSTANCE PER TRACKED ASSET/MARKET
# ═════════════════════════════════════════════════════════════════════════════
#
# Everything in this class is scoped to a single asset (e.g. "btc", "eth").
# All wallet/account-level concerns (Web3 connection, ClobClient, wallet audit,
# on-chain approvals, the global PnL-merge scheduler) have been moved OUT of
# this class and into AccountService, which is constructed once in main() and
# passed in here by reference (`account`). MarketWorker never creates its own
# Web3 connection, never calls derive_api_key/create_api_key, and never runs
# the wallet audit — it borrows the shared AccountService for all of that.
#
# What legitimately stays per-instance here:
#   • Order book / price-listener WebSocket subscription for this asset
#   • Entry signal generation, TP/SL exit logic for this asset's position
#   • Per-asset PnL bookkeeping (own Redis keys / own JSON history file)
#   • Per-asset dashboard state
# ═════════════════════════════════════════════════════════════════════════════

class MarketWorker:
    def __init__(self, worker_config: WorkerConfig, account: "AccountService"):
        self.worker_config = worker_config
        self.account = account

        self.w3             = account.w3
        self.client         = account.client
        self.wallet_address = account.wallet_address
        self.signer_address = account.signer_address
        self.private_key    = account.private_key

        self.asset_type    = worker_config.asset.lower()
        self.window_slug   = worker_config.window
        self.active_market: Optional[Dict[str, Any]] = None
        self.prices: Dict[str, float] = {"YES": 0.0, "NO": 0.0}
        self.bids: Dict[str, float] = {"YES": 0.0, "NO": 0.0}
        self.transitioning = False

        self.session_profit = 0.0
        self.trade_count    = 0

        _saved              = self._load_pnl_stats(self.asset_type, self.window_slug)
        self.cumulative_pnl: float = _saved["total_pnl"]
        self.wins: int             = _saved["wins"]
        self.losses: int           = _saved["losses"]

        self.trade_state: TradeState = TradeState.IDLE
        self.position_side: Optional[str] = None
        self.position_size: float = 0.0
        self.entry_price:   float = 0.0

        self.exited            = False
        self.processed_markets = set()
        self.entry_timestamp   = None
        self.start_delay_met   = False
        self.market_start_time: Optional[float] = None

        self.dummy_balance  = float(worker_config.spread_size)
        self.last_trade_time = 0
        self.logged_markets  = set()

        self.spread_state: SpreadState = SpreadState.IDLE
        self.spread_inventory = SpreadInventory()
        self.spread_captures = 0

        self.last_yes_update = 0.0
        self.last_no_update  = 0.0

        self.price_history: deque = deque(maxlen=30)
        self.binance = None

        self.market_outcome  = None
        self.final_yes_price = 0.0
        self.final_no_price  = 0.0

        self.market_slug = None
        self.seen_markets = set()

        self.market_exit_reasons: Dict[str, str] = {}
        self._order_lock = asyncio.Lock()

        label = f"{self.asset_type.upper()} {self.window_slug}"
        self.dashboard = {
            "asset":              self.asset_type.upper(),
            "window":             self.window_slug,
            "yes":                0,
            "no":                 0,
            "timer":              "--:--",
            "listener":           "--:--",
            "status":             "WAITING",
            "outcome":            "PENDING",
            "combined_bid_c":     0,
            "spread_edge":        0.0,
            "yes_bid_c":          0,
            "no_bid_c":           0,
            "yes_shares":         0.0,
            "no_shares":          0.0,
            "yes_avg_price_c":    0.0,
            "no_avg_price_c":     0.0,
            "pair_avg_price_c":   0.0,
        }
        self.recent_logs: Deque[str] = deque(maxlen=4)

        self._last_logged_slug: Optional[str] = None
        self._cashout_in_progress: bool = False
        self._history_exit_action: str = "sell"

        self.strategy = SpreadCaptureStrategy()

        _register_worker(self)

    # ═════════════════════════════════════════════════════════════════════
    # REDIS-BACKED PnL PERSISTENCE  (per-asset — each market keeps its own
    # trade history / win-loss record, which is intentionally NOT shared)
    # ═════════════════════════════════════════════════════════════════════

    def _redis_stats_key(self) -> str:
        return f"emiliano:{self.asset_type}:{self.window_slug}:stats"

    def _redis_trades_key(self) -> str:
        return f"emiliano:{self.asset_type}:{self.window_slug}:trades"

    @staticmethod
    def _load_pnl_stats(asset_type: str, window: str) -> dict:
        redis_key = f"emiliano:{asset_type}:{window}:stats"
        if _redis_available:
            data = redis_get_json(redis_key)
            if data:
                print(f"✅ [{asset_type.upper()} {window}] Loaded PnL stats from Redis: "
                      f"PnL=${data.get('total_pnl', 0):.2f} "
                      f"W{data.get('wins', 0)}/L{data.get('losses', 0)}")
                return {
                    "total_pnl": float(data.get("total_pnl", 0.0)),
                    "wins":      int(data.get("wins", 0)),
                    "losses":    int(data.get("losses", 0)),
                }
            legacy = redis_get_json(f"emiliano:{asset_type}:stats")
            if legacy:
                print(f"✅ [{asset_type.upper()} {window}] Loaded legacy PnL stats from Redis")
                return {
                    "total_pnl": float(legacy.get("total_pnl", 0.0)),
                    "wins":      int(legacy.get("wins", 0)),
                    "losses":    int(legacy.get("losses", 0)),
                }

        file_path = asset_pnl_filename(asset_type, window)
        try:
            if os.path.exists(file_path):
                with open(file_path, "r") as f:
                    data = json.load(f)
                result = {
                    "total_pnl": float(data.get("total_pnl", 0.0)),
                    "wins":      int(data.get("wins", 0)),
                    "losses":    int(data.get("losses", 0)),
                }
                if _redis_available:
                    print(f"📤 [{asset_type.upper()} {window}] Migrating local stats → Redis...")
                    redis_set_json(redis_key, result)
                return result
        except Exception:
            pass

        legacy_path = f"{asset_type}_pnl_history.json"
        try:
            if os.path.exists(legacy_path):
                with open(legacy_path, "r") as f:
                    data = json.load(f)
                return {
                    "total_pnl": float(data.get("total_pnl", 0.0)),
                    "wins":      int(data.get("wins", 0)),
                    "losses":    int(data.get("losses", 0)),
                }
        except Exception:
            pass

        return {"total_pnl": 0.0, "wins": 0, "losses": 0}

    def _save_stats_to_redis(self):
        if not _redis_available:
            return
        payload = {
            "total_pnl":    round(self.cumulative_pnl, 4),
            "wins":         self.wins,
            "losses":       self.losses,
            "win_rate":     (f"{round((self.wins / (self.wins + self.losses)) * 100, 2)}%"
                             if (self.wins + self.losses) > 0 else "0%"),
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        ok = redis_set_json(self._redis_stats_key(), payload)
        if ok:
            print(f"💾 [{self.asset_type.upper()}] Stats saved to Redis: "
                  f"PnL=${self.cumulative_pnl:.2f} W{self.wins}/L{self.losses}")
        else:
            print(f"⚠️ [{self.asset_type.upper()}] Redis save failed — in-memory state preserved.")

    def _append_trade_to_redis(self, entry: dict):
        if not _redis_available:
            return
        existing = redis_get_json(self._redis_trades_key()) or []
        existing.append(entry)
        if len(existing) > 500:
            existing = existing[-500:]
        redis_set_json(self._redis_trades_key(), existing)

    # ═════════════════════════════════════════════════════════════════════
    # DASHBOARD HELPERS
    # ═════════════════════════════════════════════════════════════════════

    def add_log(self, message: str):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.recent_logs.append(f"[{timestamp}] {message}")

    def update_dashboard(self):
        inv = self.spread_inventory
        self.dashboard["outcome"] = self.market_outcome or "PENDING"
        if inv.yes_shares > 0 or inv.no_shares > 0:
            self.dashboard["status"] = (
                f"SPREAD Y{inv.yes_shares:.0f}/N{inv.no_shares:.0f}"
            )

    def get_listener_countdown(self) -> str:
        if not self.active_market:
            return "--:--"
        now          = datetime.now(timezone.utc)
        remaining    = self.active_market["expiry"] - now
        seconds_left = int(remaining.total_seconds())
        activate = self.worker_config.listener_activate_secs
        if seconds_left <= activate:
            return "00:00"
        wait_seconds = seconds_left - activate
        mins, secs   = divmod(wait_seconds, 60)
        return f"{mins:02d}:{secs:02d}"

    def is_dry_run(self) -> bool:
        return self.worker_config.dry_run

    def spread_order_size(self, legs: List[str]) -> Optional[float]:
        """Pick order size (random in configured range) and clamp to headroom."""
        wc = self.worker_config
        if wc.random_order_size:
            target = random.uniform(wc.spread_size_min, wc.spread_size_max)
            target = round(target, 1)
        else:
            target = float(wc.spread_size_max)
        per_leg: List[float] = []
        for side in legs:
            room = self.spread_inventory.headroom(side, wc.max_shares)
            if room < MIN_SHARES:
                return None
            per_leg.append(min(target, room, float(wc.max_order_size)))
        size = min(per_leg)
        if size < MIN_SHARES:
            return None
        return round(size, 4)

    def validate_spread_order_size(self, side: str, size: float) -> bool:
        wc = self.worker_config
        inv = self.spread_inventory
        projected = inv.shares(side) + size
        if size <= 0:
            print(f"❌ [SIZE CHECK] {side} rejected: size={size} <= 0")
            return False
        if size > wc.max_order_size + 1e-9:
            print(
                f"❌ [SIZE CHECK] {side} rejected: size={size} "
                f"> max_order_size={wc.max_order_size}"
            )
            exec_log(
                "spread_size_rejected", side=side, size=size,
                reason="max_order_size", asset=self.asset_type, window=self.window_slug,
            )
            return False
        if projected > wc.max_shares + 1e-9:
            print(
                f"❌ [SIZE CHECK] {side} rejected: projected inventory "
                f"{projected:.4f} > max_shares={wc.max_shares} "
                f"(current={inv.shares(side):.4f}, order={size:.4f})"
            )
            exec_log(
                "spread_size_rejected", side=side, size=size,
                reason="max_shares", projected=projected,
                asset=self.asset_type, window=self.window_slug,
            )
            return False
        return True

    def _update_spread_dashboard(self) -> None:
        up_bid = self.bids.get("YES", 0.0)
        down_bid = self.bids.get("NO", 0.0)
        combined = (up_bid + down_bid) if up_bid > 0 and down_bid > 0 else 0.0
        edge = round(1.0 - combined, 4) if combined > 0 else 0.0
        inv = self.spread_inventory
        self.dashboard["combined_bid_c"] = round(combined * 100)
        self.dashboard["spread_edge"] = edge
        self.dashboard["yes_bid_c"] = round(up_bid * 100)
        self.dashboard["no_bid_c"] = round(down_bid * 100)
        self.dashboard["yes_shares"] = round(inv.yes_shares, 2)
        self.dashboard["no_shares"] = round(inv.no_shares, 2)
        yes_avg = inv.avg_cost("YES")
        no_avg = inv.avg_cost("NO")
        self.dashboard["yes_avg_price"] = round(yes_avg, 4)
        self.dashboard["no_avg_price"] = round(no_avg, 4)
        self.dashboard["yes_avg_price_c"] = round(yes_avg * 100, 1) if inv.yes_shares > 0 else 0.0
        self.dashboard["no_avg_price_c"] = round(no_avg * 100, 1) if inv.no_shares > 0 else 0.0
        if inv.yes_shares > 0 and inv.no_shares > 0:
            self.dashboard["pair_avg_price_c"] = round((yes_avg + no_avg) * 100, 1)
        else:
            self.dashboard["pair_avg_price_c"] = 0.0
        if inv.yes_shares > 0 or inv.no_shares > 0:
            self.dashboard["status"] = (
                f"SPREAD Y{inv.yes_shares:.0f}/N{inv.no_shares:.0f}"
            )

    def _spread_rebalance_decision(
        self,
        *,
        underweight: str,
        up_bid: float,
        down_bid: float,
        edge: float,
        size: int,
    ) -> SpreadDecision:
        bias = self.worker_config.price_bias
        if underweight == "YES":
            ask = self.prices.get("YES", 1.0)
            yes_px = round(up_bid + bias, 2)
            if ask > 0.02:
                yes_px = min(yes_px, round(ask - 0.01, 2))
            return SpreadDecision(
                yes_price=yes_px,
                no_price=0.0,
                size=size,
                edge=edge,
                mode="rebalance",
                rebalance_side="YES",
            )
        ask = self.prices.get("NO", 1.0)
        no_px = round(down_bid + bias, 2)
        if ask > 0.02:
            no_px = min(no_px, round(ask - 0.01, 2))
        return SpreadDecision(
            yes_price=0.0,
            no_price=no_px,
            size=size,
            edge=edge,
            mode="rebalance",
            rebalance_side="NO",
        )

    def _log_spread_capture(
        self,
        decision: SpreadDecision,
        *,
        dry_run: bool = False,
        fills: Optional[Dict[str, Tuple[float, float]]] = None,
    ) -> None:
        if not fills:
            return
        self.spread_captures += 1
        inv = self.spread_inventory
        mode = "DRY" if dry_run else "LIVE"
        fill_parts = []
        if fills:
            for side, (sz, px) in fills.items():
                fill_parts.append(f"{side}={sz:.2f}@{round(px*100)}c")
        fill_str = " ".join(fill_parts) if fill_parts else "simulated"
        msg = (
            f"[SPREAD {mode}] {decision.mode} edge={decision.edge:.4f} "
            f"size={decision.size} | {fill_str} | "
            f"inv Y={inv.yes_shares:.1f} N={inv.no_shares:.1f}"
        )
        self.log_to_file(msg)
        self.add_log(msg[:120])

    async def place_spread_gtc(
        self, side: str, price: float, size: float,
    ) -> Tuple[Optional[str], float]:
        if not self.validate_spread_order_size(side, float(size)):
            return None, 0.0
        ok, order_id, filled = await self.place_order_raw(
            side, price, size, order_type="GTC",
        )
        if not ok:
            return None, 0.0
        if filled and order_id:
            return order_id, float(size)
        return order_id, 0.0

    async def poll_order_fill(self, order_id: str, requested: float) -> float:
        try:
            info = self.account.get_order_status(order_id)
            if isinstance(info, str):
                info = json.loads(info)
            if isinstance(info, dict):
                matched = (
                    info.get("size_matched")
                    or info.get("sizeMatched")
                    or info.get("matched_size")
                    or 0
                )
                return min(float(matched), float(requested))
        except Exception:
            pass
        return 0.0

    def get_spread_unrealized_pnl(self) -> Tuple[float, float]:
        """Mark-to-market for spread inventory (matched pairs @ $1, unpaired @ bid)."""
        inv = self.spread_inventory
        if inv.yes_shares <= MIN_FILL_DELTA and inv.no_shares <= MIN_FILL_DELTA:
            return 0.0, 0.0

        matched = inv.matched_pairs
        yes_unpaired = max(0.0, inv.yes_shares - matched)
        no_unpaired = max(0.0, inv.no_shares - matched)
        yes_bid = self.bids.get("YES", 0.0)
        no_bid = self.bids.get("NO", 0.0)

        mark_value = (
            matched * 1.0
            + yes_unpaired * yes_bid
            + no_unpaired * no_bid
        )
        total_cost = inv.yes_cost + inv.no_cost
        unrealized = mark_value - total_cost
        roi_pct = (unrealized / total_cost * 100) if total_cost > 0 else 0.0
        return round(unrealized, 4), round(roi_pct, 2)

    def get_current_pnl(self) -> Tuple[float, float, str]:
        pnl_dollars, pnl_pct = self.get_spread_unrealized_pnl()
        if pnl_dollars == 0.0 and pnl_pct == 0.0:
            inv = self.spread_inventory
            if inv.yes_shares <= MIN_FILL_DELTA and inv.no_shares <= MIN_FILL_DELTA:
                return 0.0, 0.0, "white"
        color = "red" if pnl_dollars < 0 else "green"
        return pnl_dollars, pnl_pct, color

    def log_to_file(self, message: str):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(f"{timestamp} {message}\n")
        except Exception:
            pass

    def log_trade(self, side: str, price: float, action: str = "BUY",
                  size: float = 0.0, order_id: Optional[str] = None):
        mode_label  = "[DRY]" if self.is_dry_run() else "[LIVE]"
        market_name = self.active_market['question'] if self.active_market else "Unknown Market"
        slug        = ((self.active_market.get("slug") if self.active_market else None)
                       or self.market_slug or self.asset_type)
        price_cents = f"{round(price * 100)}c"
        msg = (f"{mode_label} {action}: {side} @ {price_cents} "
               f"size={size:.4f} | Market: {market_name}")
        self.log_to_file(msg)
        exec_log(
            "trade", mode=mode_label, action=action, side=side,
            price=price, size=size, order_id=order_id, market=market_name,
            window=self.window_slug,
        )
        if size > 0 and price > 0 and side in ("YES", "NO"):
            append_trade_history({
                "asset":    self.asset_type.upper(),
                "window":   self.window_slug,
                "market":   market_name,
                "slug":     slug,
                "action":   action,
                "side":     side,
                "price":    price,
                "size":     size,
            })

    # ── Market fetching ────────────────────────────────────────────────

    def fetch_target_market(self, url: str) -> bool:
        try:
            match = re.search(r"/(?:event|market)/([^/?#]+)", url)
            if not match:
                print(f"❌ Could not parse slug from URL: {url}")
                return False
            slug = match.group(1)
            if slug in self.processed_markets:
                print(f"⏭️  BLOCKED: Already processed market: {slug}")
                return False
            resp = requests.get(f"{GAMMA_URL}?slug={slug}").json()
            if not resp:
                print(f"❌ API returned no data for slug: {slug}")
                return False
            m            = resp[0]
            end_date_str = m.get("endDate")
            end_dt       = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
            now          = datetime.now(timezone.utc)
            if now >= end_dt:
                print(f"🛑 Market EXPIRED.")
                return False
            clob_ids = m.get("clobTokenIds")
            if isinstance(clob_ids, str):
                clob_ids = json.loads(clob_ids)
            self.active_market = {
                "question": m.get('question'),
                "yes_id":   str(clob_ids[0]),
                "no_id":    str(clob_ids[1]),
                "expiry":   end_dt,
                "slug":     slug,
            }
            self.market_slug = slug
            print(f"🎯 Market: {self.active_market['question']} | Slug: {slug}")
            return True
        except Exception as e:
            print(f"Market Fetch Error: {e}")
            return False

    # ── WebSocket price listener ───────────────────────────────────────

    async def price_listener(self):
        if not self.active_market:
            return
        activate = self.worker_config.listener_activate_secs
        while True:
            now          = datetime.now(timezone.utc)
            remaining    = self.active_market["expiry"] - now
            seconds_left = int(remaining.total_seconds())
            if seconds_left <= activate:
                break
            sleep_time = min(5, seconds_left - activate)
            print(f"⏳ Waiting for WS window: {seconds_left}s left...", end="\r")
            await asyncio.sleep(max(1, sleep_time))

        print(f"\n\n📡 Starting WebSocket listener in final {activate}s window...")

        async for ws in websockets.connect(WS_URL, ping_interval=20, ping_timeout=20, close_timeout=10):
            try:
                sub_msg = {
                    "operation":  "subscribe",
                    "type":       "market",
                    "assets_ids": [self.active_market["yes_id"], self.active_market["no_id"]],
                }
                await ws.send(json.dumps(sub_msg))
                print(f"\n📡 Subscription active for: {self.active_market['question']}")

                async for message in ws:
                    if self.exited:
                        await ws.close()
                        break
                    now          = datetime.now(timezone.utc)
                    remaining    = self.active_market["expiry"] - now
                    seconds_left = int(remaining.total_seconds())
                    if seconds_left <= 0:
                        print("\n⌛ Market expired. Closing listener...")
                        await asyncio.sleep(3)
                        self.print_final_summary()
                        await ws.close()
                        return
                    mins, secs = divmod(seconds_left, 60)
                    timer_str  = f"{mins:02d}:{secs:02d}"

                    data   = json.loads(message)
                    events = data if isinstance(data, list) else [data]
                    for ev in events:
                        e_type = ev.get("event_type")
                        if e_type in ["book", "initial_state"]:
                            for asset in ev.get("assets", []):
                                aid = asset.get("asset_id")
                                ask = float(asset.get("best_ask", 0))
                                bid = float(asset.get("best_bid", 0))
                                if aid == self.active_market["yes_id"]:
                                    if ask > 0:
                                        self.prices["YES"] = ask
                                    if bid > 0:
                                        self.bids["YES"] = bid
                                elif aid == self.active_market["no_id"]:
                                    if ask > 0:
                                        self.prices["NO"] = ask
                                    if bid > 0:
                                        self.bids["NO"] = bid
                        elif e_type == "price_change":
                            for change in ev.get("price_changes", []):
                                aid = change.get("asset_id")
                                ask = float(change.get("best_ask") or change.get("price", 0))
                                bid = float(change.get("best_bid", 0))
                                if aid == self.active_market["yes_id"]:
                                    if ask > 0:
                                        self.prices["YES"] = ask
                                    if bid > 0:
                                        self.bids["YES"] = bid
                                elif aid == self.active_market["no_id"]:
                                    if ask > 0:
                                        self.prices["NO"] = ask
                                    if bid > 0:
                                        self.bids["NO"] = bid
                    if self.prices["YES"] > 0 and self.prices["NO"] > 0:
                        self.price_history.append({
                            "ts":  t.time(),
                            "YES": self.prices["YES"],
                            "NO":  self.prices["NO"],
                        })
                        await self.check_logic(timer_str)

            except websockets.exceptions.ConnectionClosed:
                print("\n⚠️ Connection lost. Reconnecting in 3s...")
                await asyncio.sleep(3)
                continue
            except Exception as e:
                print(f"\n❌ Listener Error: {e}")
                await asyncio.sleep(5)
                continue

    # ═════════════════════════════════════════════════════════════════════
    # CORE ENTRY LOGIC — SINGLE-LEG DIRECTIONAL
    # ═════════════════════════════════════════════════════════════════════

    async def check_logic(self, timer: str):
        """Spread capture entry — evaluates edge every tick."""
        y = self.prices.get("YES", 0.0)
        n = self.prices.get("NO",  0.0)
        y_c = round(y * 100) if y > 0 else 0
        n_c = round(n * 100) if n > 0 else 0
        self.dashboard["yes"]   = y_c
        self.dashboard["no"]    = n_c
        self.dashboard["timer"] = timer
        self._update_spread_dashboard()
        self.update_dashboard()

        if y <= 0 or n <= 0:
            return

        await self._check_spread_logic(y_c, n_c)

    async def _check_spread_logic(self, y_c: int, n_c: int) -> None:
        if self.spread_state == SpreadState.PENDING:
            return
        if self._order_lock.locked():
            return

        if not is_trading_allowed():
            log_weekend_block(self.asset_type, "YES/NO", round(max(y_c, n_c)))
            return

        if asset_cooldown.is_entry_blocked(self.asset_type, self.window_slug):
            log_cooldown_block(
                self.asset_type, self.window_slug, "YES/NO", round(max(y_c, n_c)),
            )
            return

        inv = self.spread_inventory
        at_cap = (
            inv.headroom("YES", self.worker_config.max_shares) < MIN_SHARES
            and inv.headroom("NO", self.worker_config.max_shares) < MIN_SHARES
        )
        if at_cap:
            return

        decision = await self.strategy.evaluate(self)
        if decision:
            mode = decision.mode.upper()
            print(
                f"[SPREAD] {self.asset_type.upper()} {self.window_slug} | "
                f"{mode} edge={decision.edge:.4f} size={decision.size}"
            )
            async with self._order_lock:
                if self.spread_state != SpreadState.IDLE:
                    return
                await self.strategy.execute(self, decision)
            return

        comb = self.dashboard.get("combined_bid_c", 0)
        edge = self.dashboard.get("spread_edge", 0.0)
        thr = self.worker_config.spread_threshold
        inv = self.spread_inventory
        print(
            f"⏳ [IDLE] {self.asset_type.upper()} {self.window_slug} | "
            f"YES={y_c}c NO={n_c}c | bids={comb}c edge={edge:.4f} "
            f"(need>{thr:.4f}) | inv Y={inv.yes_shares:.0f} N={inv.no_shares:.0f} "
            f"caps={self.spread_captures}"
        )

    # ── Order execution ────────────────────────────────────────────────

    async def place_order_raw(
        self,
        side: str,
        price: float,
        size: float,
        *,
        order_type: str = "GTC",
    ) -> Tuple[bool, Optional[str], bool]:
        """Submit one order; return (accepted, order_id, filled_immediately)."""
        if not self.active_market:
            return False, None, False

        token_id = (self.active_market["yes_id"] if side == "YES"
                    else self.active_market["no_id"])
        clean_price = max(0.01, min(0.99, round(price, 2)))

        if not self.validate_spread_order_size(side, float(size)):
            return False, None, False

        if self.is_dry_run():
            print(f"\n🧪 [DRY] BUY {size} {side} @ {round(clean_price*100)}c "
                  f"mode={order_type}")
            exec_log("dry_run_order", side=side, price=clean_price, size=size,
                     order_type=order_type, window=self.window_slug)
            return True, "dry-run", True

        try:
            resp = self.account.create_and_post_order(
                "BUY", clean_price, float(size), token_id, order_type=order_type,
            )
        except Exception as e:
            exec_log("order_failed", side=side, error=str(e), order_type=order_type)
            print(f"❌ Order placement failed: {e}")
            return False, None, False

        order_id = None
        if isinstance(resp, dict):
            order_id = resp.get("orderID") or resp.get("order_id")
        elif isinstance(resp, str):
            try:
                parsed = json.loads(resp)
                order_id = parsed.get("orderID") or parsed.get("order_id")
            except Exception:
                pass

        filled = False
        if order_type.upper() == "FOK" and order_id:
            await asyncio.sleep(0.5)
            try:
                info = self.account.get_order_status(order_id)
                if isinstance(info, str):
                    info = json.loads(info)
                if isinstance(info, dict):
                    st = str(info.get("status", "")).lower()
                    filled = st in ("filled", "matched", "closed")
            except Exception:
                pass

        exec_log("order_submit", side=side, price=clean_price, size=size,
                 order_type=order_type, order_id=order_id, filled=filled)
        return True, order_id, filled


    def _try_cancel_order(self, order_id: str):
        try:
            result = self.account.cancel_order(order_id)
            print(f"🚫 Cancel sent for {order_id}: {result}")
            exec_log("cancel_sent", order_id=order_id, result=str(result)[:100])
        except Exception as e:
            print(f"⚠️ Cancel failed for {order_id}: {e}")
            exec_log("cancel_failed", order_id=order_id, error=str(e))

    # ── On-chain balance helpers (thin pass-throughs to AccountService) ────

    async def get_onchain_share_balance_async(self, token_id: str, retries: int = 3) -> float:
        return await self.account.get_onchain_share_balance_async(token_id, retries=retries)

    def get_onchain_share_balance(self, token_id: str, retries: int = 3) -> float:
        return self.account.get_onchain_share_balance(token_id, retries=retries)

    async def merge_shares(self, amount_to_merge: float):
        """Merge YES+NO shares back into pUSD on-chain for this worker's active market."""
        return await self.account.merge_shares(self.active_market, amount_to_merge)

    def get_position_snapshot(self) -> Optional[Dict]:
        """Live spread inventory row for the dashboard Positions tab."""
        inv = self.spread_inventory
        if inv.yes_shares <= MIN_FILL_DELTA and inv.no_shares <= MIN_FILL_DELTA:
            return None
        slug = ((self.active_market.get("slug") if self.active_market else None)
                or self.market_slug or self.asset_type)
        market_name = ((self.active_market.get("question") if self.active_market else None)
                       or f"{self.asset_type.upper()} Up or Down")
        pnl_dollars, pnl_pct = self.get_spread_unrealized_pnl()
        total_cost = inv.yes_cost + inv.no_cost
        yes_avg_c = round(inv.avg_cost("YES") * 100, 1) if inv.yes_shares > 0 else 0.0
        no_avg_c = round(inv.avg_cost("NO") * 100, 1) if inv.no_shares > 0 else 0.0
        pair_avg_c = (
            round((inv.avg_cost("YES") + inv.avg_cost("NO")) * 100, 1)
            if inv.matched_pairs > 0 else 0.0
        )
        return {
            "id":                  f"{self.asset_type}:{self.window_slug}:{slug}:spread",
            "asset":               self.asset_type.upper(),
            "window":              self.window_slug,
            "market":              market_name,
            "slug":                slug,
            "side":                "SPREAD",
            "strategy":            "spread_capture",
            "yes_shares":          round(inv.yes_shares, 4),
            "no_shares":           round(inv.no_shares, 4),
            "yes_avg_price_c":     yes_avg_c,
            "no_avg_price_c":      no_avg_c,
            "pair_avg_price_c":    pair_avg_c,
            "matched_pairs":       round(inv.matched_pairs, 4),
            "spread_imbalance":    round(inv.imbalance, 4),
            "entry_price":         round(inv.avg_cost("YES") + inv.avg_cost("NO"), 4)
                                   if inv.matched_pairs > 0 else 0.0,
            "current_price":       0.0,
            "entry_price_cents":   pair_avg_c,
            "current_price_cents": 100.0 if inv.matched_pairs > 0 else 0.0,
            "roi_pct":             pnl_pct,
            "unrealized_pnl":      pnl_dollars,
            "size":                round(inv.yes_shares + inv.no_shares, 4),
            "size_usd":            round(total_cost, 2),
            "cashout_available":   False,
        }

    # ── State reset ────────────────────────────────────────────────────

    async def reset_state(self):
        self.trade_state     = TradeState.IDLE
        self.position_side   = None
        self.position_size   = 0.0
        self.entry_price     = 0.0
        self.entry_timestamp = None
        self.market_start_time = None
        self.price_history.clear()
        self.market_outcome    = None
        self.final_yes_price   = 0.0
        self.final_no_price    = 0.0
        self.exited            = False
        self.market_slug       = None
        self.prices            = {"YES": 0.0, "NO": 0.0}
        self.bids              = {"YES": 0.0, "NO": 0.0}
        self.dashboard["yes"]               = 0
        self.dashboard["no"]                = 0
        self.dashboard["timer"]             = "--:--"
        self.dashboard["listener"]          = "--:--"
        self.dashboard["outcome"]           = "PENDING"
        self.dashboard["bought_side"]       = "-"
        self.dashboard["entry_price"]       = 0.0
        self.dashboard["status"]            = "WAITING"
        self.dashboard["profit"]            = 0.0
        self.dashboard["price_delta"]       = 0.0
        self.dashboard["signal_stale"]      = True
        self.dashboard["combined_bid_c"]    = 0
        self.dashboard["spread_edge"]       = 0.0
        self.dashboard["yes_bid_c"]         = 0
        self.dashboard["no_bid_c"]          = 0
        self.dashboard["yes_shares"]        = 0.0
        self.dashboard["no_shares"]         = 0.0
        self.dashboard["yes_avg_price"]     = 0.0
        self.dashboard["no_avg_price"]      = 0.0
        self.dashboard["yes_avg_price_c"]   = 0.0
        self.dashboard["no_avg_price_c"]    = 0.0
        self.dashboard["pair_avg_price_c"]  = 0.0
        self.spread_state = SpreadState.IDLE
        self.spread_inventory.reset()
        self.spread_captures = 0
        self.recent_logs.clear()
        self.update_dashboard()
        print("\n♻️ Full state reset after trade/exit. Ready for next market.")

    # ── PnL logging ────────────────────────────────────────────────────

    def log_pnl(self, outcome_type: str, pnl_amount: float, details: dict):
        file_path       = asset_pnl_filename(self.asset_type, self.window_slug)
        slug            = ((self.active_market.get("slug") if self.active_market else None)
                           or self.market_slug or "unknown")
        market_question = ((self.active_market.get("question", "Unknown")
                            if self.active_market else "Unknown Market"))

        # ── Validation: reject corrupted PnL values outright ─────────────
        # A NaN/Inf/None pnl_amount (e.g. from a divide-by-zero upstream, or
        # a malformed fill response) must never be allowed to corrupt
        # cumulative_pnl or the persisted history — once a bad value is
        # added in, every later point is wrong forever.
        if not _is_finite_number(pnl_amount):
            print(f"❌ [{self.asset_type.upper()}] log_pnl rejected non-finite pnl_amount "
                  f"({pnl_amount!r}) for market {slug} — trade NOT recorded.")
            return

        # ── Per-round dedup guard ──────────────────────────────────────────
        # "Only one valid snapshot per market round": if this exact slug was
        # already logged (e.g. a retried exit path firing twice), skip the
        # second call entirely rather than double-counting the trade.
        round_key = f"{self.asset_type}:{self.window_slug}:{slug}"
        if slug != "unknown" and slug == self._last_logged_slug:
            print(f"ℹ️ [{self.asset_type.upper()}] log_pnl skipped duplicate call for "
                  f"already-logged market {slug}.")
            return

        self.cumulative_pnl = round(self.cumulative_pnl + pnl_amount, 4)
        if pnl_amount > 0:
            self.wins   += 1
        else:
            self.losses += 1
        self.trade_count = self.wins + self.losses
        total_trades     = self.trade_count
        win_rate_str     = (f"{round((self.wins / total_trades) * 100, 2)}%"
                            if total_trades > 0 else "0%")

        duration = round(t.time() - (self.entry_timestamp or t.time()), 2)
        entry = {
            "timestamp":      datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "market":         market_question,
            "slug":           slug,
            "type":           outcome_type,
            "pnl":            round(pnl_amount, 4),
            "cumulative_pnl": self.cumulative_pnl,
            "details":        {**details, "duration_seconds": duration},
        }

        self._save_stats_to_redis()
        self._append_trade_to_redis(entry)
        self._last_logged_slug = slug

        # Cooldown risk tracker — separate from lifetime cumulative_pnl above.
        asset_cooldown.record_realized_pnl(self.asset_type, self.window_slug, pnl_amount)

        # ── Live portfolio history snapshot ──────────────────────────────
        # Push a portfolio-total point to emiliano:portfolio:history right
        # now so the chart updates the moment this trade closes, without
        # waiting for any background flush interval.
        #
        # IMPORTANT (this was the root cause of the chart spikes): this MUST
        # be the TOTAL PnL across every asset, never self.cumulative_pnl for
        # this asset alone. Using a single asset's own PnL here produced a
        # sharp spike/drop on every trade close, because that value would be
        # written right next to genuinely correct cross-asset totals from
        # the 60-s background loop and the startup backfill.
        portfolio_history_snapshot(_portfolio_total_pnl(), round_key=round_key)

        default_data = {"total_pnl": 0.0, "wins": 0, "losses": 0,
                        "win_rate": "0%", "trades": []}
        try:
            if os.path.exists(file_path):
                with open(file_path, 'r') as f:
                    data = json.load(f)
            else:
                data = default_data
        except Exception:
            data = default_data

        data["total_pnl"] = self.cumulative_pnl
        data["wins"]      = self.wins
        data["losses"]    = self.losses
        data["win_rate"]  = win_rate_str
        data["trades"].append(entry)

        try:
            temp_file = file_path + ".tmp"
            with open(temp_file, 'w') as f:
                json.dump(data, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_file, file_path)
        except Exception as e:
            print(f"⚠️ Local JSON write failed (non-fatal, Redis has the data): {e}")

    # ── Dashboard data ─────────────────────────────────────────────────

    def get_dashboard_data(self) -> dict:
        pnl_dollars, pnl_pct, _ = self.get_current_pnl()
        inv = self.spread_inventory

        if inv.yes_shares > 0 or inv.no_shares > 0:
            leg_parts: List[str] = []
            yes_cost = 0.0
            no_cost = 0.0
            if inv.yes_shares > 0:
                yes_cost = inv.yes_shares * inv.avg_cost("YES")
                leg_parts.append(f"YES @ ${yes_cost:.2f}")
            if inv.no_shares > 0:
                no_cost = inv.no_shares * inv.avg_cost("NO")
                leg_parts.append(f"NO @ ${no_cost:.2f}")
            position_text = " ".join(leg_parts)
            if inv.yes_shares > 0 and inv.no_shares > 0:
                position_text += f" (pair=${yes_cost + no_cost:.1f})"
        else:
            position_text = "-"

        spread_status = (
            "PENDING" if self.spread_state == SpreadState.PENDING else "HUNTING"
        )
        if inv.yes_shares > 0 or inv.no_shares > 0:
            spread_status = f"INVENTORY {spread_status}"

        market_start_iso = None
        market_end_iso   = None
        if self.active_market and self.active_market.get("expiry"):
            expiry           = self.active_market["expiry"]
            start            = expiry - timedelta(seconds=self._interval_seconds())
            market_end_iso   = expiry.isoformat()
            market_start_iso = start.isoformat()

        cd = asset_cooldown.get_status(self.asset_type, self.window_slug)
        schedule_ok = is_trading_allowed()
        edge = self.dashboard.get("spread_edge", 0.0)
        edge_cents = round(edge * 100, 2)
        thr_cents = round(self.worker_config.spread_threshold * 100, 2)

        return {
            "asset":              self.asset_type.upper(),
            "window":             self.window_slug,
            "strategy":           "spread_capture",
            "yes":                round(self.prices.get("YES", 0) * 100),
            "no":                 round(self.prices.get("NO",  0) * 100),
            "yes_bid_c":          self.dashboard.get("yes_bid_c", 0),
            "no_bid_c":           self.dashboard.get("no_bid_c", 0),
            "combined_bid_c":     self.dashboard.get("combined_bid_c", 0),
            "spread_edge":        edge,
            "spread_edge_cents":  edge_cents,
            "spread_threshold":   self.worker_config.spread_threshold,
            "spread_threshold_cents": thr_cents,
            "edge_above_threshold": edge > self.worker_config.spread_threshold,
            "max_shares":         self.worker_config.max_shares,
            "yes_shares":         self.dashboard.get("yes_shares", 0.0),
            "no_shares":          self.dashboard.get("no_shares", 0.0),
            "yes_avg_price":      self.dashboard.get("yes_avg_price", 0.0),
            "no_avg_price":       self.dashboard.get("no_avg_price", 0.0),
            "yes_avg_price_c":    self.dashboard.get("yes_avg_price_c", 0.0),
            "no_avg_price_c":     self.dashboard.get("no_avg_price_c", 0.0),
            "pair_avg_price_c":   self.dashboard.get("pair_avg_price_c", 0.0),
            "spread_captures":    self.spread_captures,
            "spread_state":       spread_status,
            "timer":              self.dashboard.get("timer",    "--:--"),
            "listener":           self.get_listener_countdown(),
            "status":             spread_status,
            "position":           position_text,
            "outcome":            self.dashboard.get("outcome", "PENDING"),
            "dry_run":            self.is_dry_run(),
            "pnl_dollars":        pnl_dollars,
            "pnl_pct":            pnl_pct,
            "cumulative_pnl":     round(self.cumulative_pnl, 2),
            "wins":               self.wins,
            "losses":             self.losses,
            "trade_count":        self.trade_count,
            "win_rate":           (round((self.wins / self.trade_count) * 100, 1)
                                   if self.trade_count > 0 else 0.0),
            "market_start_iso":   market_start_iso,
            "market_end_iso":     market_end_iso,
            "locked_low_c":       round(LOCKED_LOW  * 100),
            "locked_high_c":      round(LOCKED_HIGH * 100),
            "trading_allowed":    schedule_ok,
            "trading_tz":         _TRADING_TZ_NAME,
            "entry_allowed":      schedule_ok and not cd.get("cooldown_active", False),
            "cooldown_active":          cd.get("cooldown_active", False),
            "cooldown_window_pnl":      cd.get("cooldown_window_pnl", 0.0),
            "cooldown_until_utc":       cd.get("cooldown_until_utc"),
            "cooldown_remaining_sec":   cd.get("cooldown_remaining_sec", 0),
            "cooldown_max_loss":        cd.get("cooldown_max_loss", ASSET_MAX_CUMULATIVE_LOSS),
            "entries_blocked_cooldown": cd.get("entries_blocked_cooldown", False),
        }

    def print_final_summary(self):
        self.final_yes_price = self.prices.get("YES", 0.0)
        self.final_no_price  = self.prices.get("NO",  0.0)
        if self.final_yes_price > FINAL_PRICE:
            self.market_outcome = "YES"
        elif self.final_no_price > FINAL_PRICE:
            self.market_outcome = "NO"
        else:
            self.market_outcome = "UNKNOWN"
        self.dashboard["outcome"] = self.market_outcome
        self._settle_spread_market()

    def _settle_spread_market(self) -> None:
        inv = self.spread_inventory
        outcome = self.market_outcome or "UNKNOWN"
        price_in_cents = lambda p: f"{round(p * 100)}c"

        if inv.yes_shares <= MIN_FILL_DELTA and inv.no_shares <= MIN_FILL_DELTA:
            print(f"\n{BOLD}{YELLOW}ℹ️  No spread inventory this market "
                  f"({self.spread_captures} capture attempts).{RESET}")
            self._finish_market_merge()
            return

        matched = inv.matched_pairs
        yes_unpaired = max(0.0, inv.yes_shares - matched)
        no_unpaired = max(0.0, inv.no_shares - matched)
        total_cost = inv.yes_cost + inv.no_cost

        settlement = matched * 1.0
        if outcome == "YES":
            settlement += yes_unpaired * 1.0
        elif outcome == "NO":
            settlement += no_unpaired * 1.0

        actual_profit = round(settlement - total_cost, 4)

        print(f"\n\n{BOLD}{GREEN}--- 📊 SPREAD SETTLEMENT ---{RESET}")
        print(f"⏱️ Market Outcome:      {outcome}")
        print(f"📦 Captures this round: {self.spread_captures}")
        print(f"📊 YES shares:          {inv.yes_shares:.4f} "
              f"(avg {price_in_cents(inv.avg_cost('YES'))})")
        print(f"📊 NO shares:           {inv.no_shares:.4f} "
              f"(avg {price_in_cents(inv.avg_cost('NO'))})")
        print(f"🔗 Matched pairs:       {matched:.4f}")
        print(f"📉 Total invested:      ${total_cost:.4f}")
        print(f"💵 Settlement value:    ${settlement:.4f}")
        print(f"📈 Net Profit:          {GREEN if actual_profit >= 0 else RED}"
              f"${actual_profit:.4f}{RESET}")

        if matched > 0:
            self.log_trade("YES", 1.0, "redeem", size=matched)
        if outcome == "YES" and yes_unpaired > 0:
            self.log_trade("YES", 1.0, "redeem", size=yes_unpaired)
        elif outcome == "NO" and no_unpaired > 0:
            self.log_trade("NO", 1.0, "redeem", size=no_unpaired)

        self.log_pnl("SPREAD_SETTLE", actual_profit, {
            "outcome":          outcome,
            "yes_shares":       inv.yes_shares,
            "no_shares":        inv.no_shares,
            "matched_pairs":    matched,
            "yes_unpaired":     yes_unpaired,
            "no_unpaired":      no_unpaired,
            "total_cost":       total_cost,
            "settlement_value": settlement,
            "spread_captures":  self.spread_captures,
        })
        self._finish_market_merge()

    def _finish_market_merge(self) -> None:
        global completed_markets
        completed_markets += 1
        if completed_markets >= TOTAL_BOTS:
            self.account.merge_now(send_telegram_notify=True)
            completed_markets = 0
        else:
            self.account.merge_now(send_telegram_notify=False)

    # ── Market scanner ─────────────────────────────────────────────────

    def _interval_seconds(self) -> int:
        return self.worker_config.interval_seconds

    def _interval_start(self, ts: int) -> int:
        sec = self._interval_seconds()
        return (ts // sec) * sec

    def current_interval_starts(self, now_ts: Optional[int] = None) -> Tuple[int, int]:
        now = now_ts if now_ts is not None else int(datetime.now(timezone.utc).timestamp())
        base = self._interval_start(now)
        return base, base + self._interval_seconds()

    def get_candidate_markets(self, asset: Optional[str] = None) -> list:
        slug_asset = (asset or self.asset_type).lower()
        now       = int(datetime.now(timezone.utc).timestamp())
        intervals = list(self.current_interval_starts(now))
        markets   = []
        sec = self._interval_seconds()
        for ts in intervals:
            slug = self.worker_config.market_slug(ts)
            try:
                resp = requests.get(f"{GAMMA_URL}?slug={slug}").json()
                if resp:
                    markets.append({
                        "slug":     slug,
                        "url":      f"https://polymarket.com/event/{resp[0]['slug']}",
                        "start_ts": ts,
                    })
            except Exception:
                pass
        return markets

    def pick_next_market(self, markets: list) -> Optional[dict]:
        now_ts  = int(datetime.now(timezone.utc).timestamp())
        sec = self._interval_seconds()
        current = [m for m in markets if m["start_ts"] <= now_ts < m["start_ts"] + sec]
        if current:
            return current[0]
        future = [m for m in markets if m["start_ts"] > now_ts]
        if not future:
            return None
        return min(future, key=lambda x: x["start_ts"])

    async def start(self):
        """Per-worker trading loop for one (asset, window) pair."""
        wc = self.worker_config
        print(f"🤖 EmilianoBot — SPREAD CAPTURE → "
              f"{self.asset_type.upper()} {self.window_slug} markets...")
        print(f"  Market interval   : {self.window_slug} ({wc.interval_seconds}s)")
        print(f"  Listener window   : final {wc.listener_activate_secs}s")
        print(f"  Spread threshold  : {wc.spread_threshold:.4f} ({wc.spread_threshold*100:.1f}c edge)")
        if wc.random_order_size:
            order_size_label = (
                f"{wc.spread_size_min}-{wc.spread_size_max} shares random "
                f"(max order {wc.max_order_size})"
            )
        else:
            order_size_label = f"{wc.spread_size_max} shares (max order {wc.max_order_size})"
        print(f"  Order size        : {order_size_label}")
        print(f"  Max inventory     : {wc.max_shares} shares per leg")
        print(f"  Cooldown          : {wc.trade_cooldown_ms}ms after dual leg")
        if self.is_dry_run():
            print(f"  Dry-run fill delay: {wc.dry_run_fill_delay_min_ms}-"
                  f"{wc.dry_run_fill_delay_max_ms}ms per leg")
        print(f"  Dry run           : {self.is_dry_run()}")
        session = requests.Session()
        sec = wc.interval_seconds
        while True:
            try:
                now_ts     = int(datetime.now(timezone.utc).timestamp())
                intervals  = list(self.current_interval_starts(now_ts))
                candidates = []
                for ts in intervals:
                    slug = wc.market_slug(ts)
                    try:
                        resp = session.get(f"{GAMMA_URL}?slug={slug}", timeout=2).json()
                        if resp:
                            candidates.append({
                                "url":      f"https://polymarket.com/event/{resp[0]['slug']}",
                                "start_ts": ts,
                                "slug":     slug,
                            })
                    except Exception:
                        continue

                current = [m for m in candidates if m["start_ts"] <= now_ts < m["start_ts"] + sec]
                if current:
                    target = current[0]
                else:
                    future = [m for m in candidates if m["start_ts"] > now_ts]
                    if not future:
                        print("⏳ No market → retrying...")
                        await asyncio.sleep(2)
                        continue
                    target = min(future, key=lambda x: x["start_ts"])

                print(f"\n🎯 Target: {target['slug']}")
                if not self.fetch_target_market(target["url"]):
                    print("❌ Market fetch failed → retry loop")
                    await asyncio.sleep(1)
                    continue

                await self.reset_state()
                now_ts = int(datetime.now(timezone.utc).timestamp())
                start  = target["start_ts"]
                if start <= now_ts < start + sec:
                    print("⚡ Already inside active market → starting immediately")

                print("📡 Starting price listener...")
                await self.price_listener()
                print("🏁 Market complete → instant rescan")

            except Exception as e:
                print(f"❌ Scheduler Error: {e}")
                await asyncio.sleep(2)


# Backward-compatible alias: existing external code/imports that reference
# `EmilianoBot` (e.g. main.py) keep working without modification.
EmilianoBot = MarketWorker

# ═════════════════════════════════════════════════════════════════════════════
# TERMINAL DASHBOARD
# ═════════════════════════════════════════════════════════════════════════════

def create_dashboard(bots):
    # Compute schedule status once for the whole render cycle.
    _trading_ok   = is_trading_allowed()
    _schedule_str = (
        "[bold green]ENTRIES: OPEN (weekday)[/bold green]"
        if _trading_ok else
        "[bold red]ENTRIES: BLOCKED (weekend / non-trading day)[/bold red]"
    )
    _tz_label = _TRADING_TZ_NAME

    layout = Layout()
    layout.split_column(
        Layout(
            Panel(
                f"[bold cyan]EMILIANO BOT — Spread Capture[/bold cyan]\n"
                f"Schedule ({_tz_label}): {_schedule_str}",
                style="bold green", box=box.ROUNDED,
            ),
            size=4,
        ),
        Layout(name="main"),
    )
    layout["main"].split_row(
        Layout(name="col1", ratio=1),
        Layout(name="col2", ratio=1),
    )

    for i, bot in enumerate(bots):
        d           = bot.dashboard
        pnl_dollars, pnl_pct, pnl_color = bot.get_current_pnl()
        listener_cd = bot.get_listener_countdown()

        display_status = d.get("status", "WAITING")
        cd = asset_cooldown.get_status(bot.asset_type, bot.window_slug)
        if cd.get("cooldown_active"):
            rem = cd.get("cooldown_remaining_sec", 0)
            display_status = f"COOLDOWN {rem // 60}m{rem % 60:02d}s"

        edge = d.get("spread_edge", 0.0)
        comb = d.get("combined_bid_c", 0)
        ratio_text = (
            f"[cyan]edge {edge*100:.2f}c[/cyan] "
            f"(bids {comb}c / need >{bot.worker_config.spread_threshold*100:.1f}c)"
        )
        strategy_text = ""

        inv_y = d.get("yes_shares", 0)
        inv_n = d.get("no_shares", 0)
        card_color = "cyan" if inv_y > 0 or inv_n > 0 else "blue"

        # Time window label
        if bot.active_market and bot.active_market.get("expiry"):
            expiry_utc  = bot.active_market["expiry"]
            et_zone     = ET_ZONE
            expiry_et   = expiry_utc.astimezone(et_zone)
            start_et    = expiry_et - timedelta(seconds=bot._interval_seconds())
            time_window = (f"{start_et.strftime('%b %d')}, "
                           f"{start_et.strftime('%I:%M%p')}-{expiry_et.strftime('%I:%M%p')} ET")
        else:
            time_window = "Waiting for market..."

        y_avg = d.get("yes_avg_price_c", 0)
        n_avg = d.get("no_avg_price_c", 0)
        bought_text = (
            f"[dim]spread | "
            + (f"Y{inv_y}@{y_avg}c " if inv_y > 0 else "")
            + (f"N{inv_n}@{n_avg}c " if inv_n > 0 else "")
            + f"| max={bot.worker_config.max_shares}/leg[/dim]"
        )

        cd_pnl_color = "red" if cd.get("cooldown_window_pnl", 0) < 0 else "green"
        cd_blocked   = " | [bold red]ENTRIES BLOCKED[/bold red]" if cd.get("cooldown_active") else ""

        card = Panel(
            Text.from_markup(
                f"""[yellow]YES:[/] {d.get('yes', 0):>3}c    [yellow]NO:[/] {d.get('no', 0):>3}c
[cyan]Timer:[/] {d.get('timer', '--:--')}
[cyan]Listener:[/] {listener_cd}
[magenta]Status:[/] {display_status}
{bought_text}
[bold]ROI:[/] [{pnl_color}]+${pnl_dollars:.2f} ({pnl_pct:+.2f}%)[/{pnl_color}]
[bold]Cooldown PnL:[/] [{cd_pnl_color}]${cd.get('cooldown_window_pnl', 0):+.2f}[/] (limit -${ASSET_MAX_CUMULATIVE_LOSS:.2f}){cd_blocked}
[bold]Spread:[/] {ratio_text}{strategy_text}
[bold]Outcome:[/] [bold {'green' if d.get('outcome') == 'YES' else 'red' if d.get('outcome') == 'NO' else 'white'}]{d.get('outcome', 'PENDING')}[/]"""
            ),
            title=f"{d.get('asset', 'UNKNOWN')} · {time_window}",
            border_style=card_color, box=box.HEAVY, padding=(1, 2),
        )
        if i == 0:
            layout["main"]["col1"].update(card)
        elif i == 1:
            layout["main"]["col2"].update(card)
        elif i == 2:
            if layout["main"]["col1"].renderable is None:
                layout["main"]["col1"].update(card)
            else:
                layout["main"]["col1"].split_column(
                    layout["main"]["col1"].renderable, Layout(card, ratio=1))
        elif i == 3:
            if layout["main"]["col2"].renderable is None:
                layout["main"]["col2"].update(card)
            else:
                layout["main"]["col2"].split_column(
                    layout["main"]["col2"].renderable, Layout(card, ratio=1))
    return layout


async def dashboard_loop(bots):
    with Live(create_dashboard(bots), console=console,
              refresh_per_second=2, screen=True) as live:
        while True:
            try:
                live.update(create_dashboard(bots))
            except Exception as e:
                console.print(f"[red]Dashboard error: {e}[/red]")
            await asyncio.sleep(0.8)


# ═════════════════════════════════════════════════════════════════════════════
# PNL MERGE + TELEGRAM
# ═════════════════════════════════════════════════════════════════════════════

# PNL merge reads per-asset local JSON files for configured TRADING_ASSETS only.
OUTPUT_FILE            = "bot_pnl.json"
MERGE_INTERVAL_SECONDS = 300
TELEGRAM_BOT_TOKEN     = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID       = os.getenv("TELEGRAM_CHAT_ID")

completed_markets      = 0
last_notification_time = 0


def send_telegram(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ Telegram credentials not set.")
        return
    try:
        url     = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "parse_mode": "HTML", "text": message}
        requests.post(url, json=payload, timeout=10)
        print("📨 Telegram notification sent.")
    except Exception as e:
        print(f"❌ Failed to send Telegram: {e}")


def get_pnl_emoji(pnl: float) -> str:
    return "🟢" if pnl >= 0 else "🔴"


def merge_all_pnl(send_telegram_notify: bool = False):
    global completed_markets, last_notification_time
    all_trades: List[Dict[str, Any]] = []
    total_pnl    = 0.0
    total_wins   = 0
    total_losses = 0
    asset_stats: Dict[str, Dict] = {}
    print(f"\n🔄 [{datetime.now().strftime('%H:%M:%S')}] Merging PNL files...")
    for file_path in PNL_FILES:
        if not os.path.exists(file_path):
            continue
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data   = json.load(f)
            trades     = data.get("trades", [])
            file_pnl   = data.get("total_pnl", 0.0)
            wins       = data.get("wins", 0)
            losses     = data.get("losses", 0)
            asset_name = file_path.replace("_pnl_history.json", "").upper()
            asset_stats[asset_name] = {
                "wins":         wins,
                "losses":       losses,
                "total_trades": wins + losses,
                "pnl":          file_pnl,
                "win_rate":     (round((wins / (wins + losses) * 100), 2)
                                 if (wins + losses) > 0 else 0.0),
            }
            all_trades.extend(trades)
            total_pnl    += file_pnl
            total_wins   += wins
            total_losses += losses
        except Exception as e:
            print(f"❌ Error reading {file_path}: {e}")

    total_trades     = total_wins + total_losses
    overall_win_rate = (round((total_wins / total_trades) * 100, 2)
                        if total_trades > 0 else 0.0)
    combined_data = {
        "total_pnl":    round(total_pnl, 4),
        "wins":         total_wins,
        "losses":       total_losses,
        "win_rate":     f"{overall_win_rate}%",
        "total_trades": total_trades,
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "assets":       asset_stats,
    }
    try:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(combined_data, f, indent=2)
    except Exception:
        pass

    ranked = sorted(asset_stats.items(), key=lambda x: x[1]["pnl"], reverse=True)
    print("\n" + "=" * 80)
    print(f"{BOLD}{CYAN}💰 PNL SUMMARY{RESET}")
    print("=" * 80)
    print(f"Total PNL       : ${total_pnl:.2f}")
    print(f"Total Trades    : {total_trades} ({total_wins}W | {total_losses}L)")
    print(f"Overall Win Rate: {overall_win_rate:.1f}%\n")
    print(f"{BOLD}Asset Ranking:{RESET}")
    for i, (asset, stats) in enumerate(ranked, 1):
        color = GREEN if stats["pnl"] >= 0 else RED
        print(f"{i}. {asset:<6} | {stats['total_trades']:>2} trades | "
              f"{stats['wins']:>3}W {stats['losses']:>3}L | "
              f"{stats['win_rate']:>5.1f}% | {color}${stats['pnl']:.2f}{RESET}")

    if send_telegram_notify:
        current_time = t.time()
        if current_time - last_notification_time > 60:
            telegram_msg = (
                f"<b>💰 EMILIANO PNL Summary</b>\n\n"
                f"<b>Total PNL:</b> {get_pnl_emoji(total_pnl)} "
                f"<b>${total_pnl:.2f}</b>\n"
                f"<b>Total Trades:</b> {total_trades} "
                f"(<b>{total_wins}W</b> - <b>{total_losses}L</b>)\n"
                f"<b>Overall Win Rate:</b> {overall_win_rate:.1f}%\n\n"
                f"<b>Ranking:</b>\n"
            )
            for i, (asset, stats) in enumerate(ranked, 1):
                emoji = get_pnl_emoji(stats["pnl"])
                telegram_msg += (
                    f"{i}. <b>{asset}</b>: {emoji} ${stats['pnl']:.2f} | "
                    f"{stats['total_trades']} trades "
                    f"(<b>{stats['wins']}W</b> - {stats['losses']}L) | "
                    f"{stats['win_rate']:.1f}%\n"
                )
            telegram_msg += f"\nLast Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            send_telegram(telegram_msg)
            last_notification_time = current_time


# ═════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═════════════════════════════════════════════════════════════════════════════
#
# Startup order (this is the fix for the duplicate-global-task issue):
#   1. Construct ONE AccountService — single Web3 connection, single ClobClient,
#      auth derived/created exactly once for the whole process.
#   2. Run the wallet audit ONCE, here, before any market worker exists.
#   3. Start the global PnL-merge scheduler ONCE.
#   4. Construct N MarketWorker instances (one per tracked asset), each
#      sharing the same AccountService by reference. Adding more assets here
#      never creates a second wallet audit, a second ClobClient, or a second
#      PnL-merge scheduler — those only ever exist once per process.
# ═════════════════════════════════════════════════════════════════════════════

async def main():
    account = AccountService()

    # Global, account-level, one-time startup work — never duplicated below.
    if not account.run_wallet_audit():
        console.print("[bold red]Wallet audit failed — aborting startup.[/bold red]")
        return
    account.start_pnl_merge_scheduler()

    # Per-asset market workers — concurrent, but each is purely market-scoped.
    bots = [MarketWorker(wc, account) for wc in WORKER_CONFIGS]

    await asyncio.gather(*[bot.start() for bot in bots], dashboard_loop(bots))


if __name__ == "__main__":
    try:
        print("🚀 Starting EmilianoBot — Spread Capture...")
        for wc in WORKER_CONFIGS:
            print(f"   {wc.asset.upper()} {wc.window}: edge>{wc.spread_threshold:.3f} "
                  f"| order={wc.spread_size} | max={wc.max_shares}/leg "
                  f"| dry_run={wc.dry_run}")
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[bold yellow]👋 EmilianoBot shutting down...[/bold yellow]")
    except Exception as e:
        console.print(f"[bold red]Fatal Error: {e}[/bold red]")