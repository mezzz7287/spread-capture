import asyncio
import json
import os
import time as _time
import traceback
from typing import TYPE_CHECKING, Any, Literal
import aiohttp
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import uvicorn
from dotenv import load_dotenv

load_dotenv()

from config import (  # noqa: E402 — env must be loaded first
    ASSET_COOLDOWN_MINUTES,
    ASSET_MAX_CUMULATIVE_LOSS,
    TRADING_ASSETS,
    TRADING_ASSETS_UPPER,
    WORKER_CONFIGS,
    trading_assets_label,
    validate_trading_assets,
)

validate_trading_assets()

if TYPE_CHECKING:
    from bot import AccountService, MarketWorker

# ── Core bot classes (always required) ───────────────────────────────────────
try:
    from bot import AccountService as _AccountService  # type: ignore
    from bot import MarketWorker   as _MarketWorker    # type: ignore
except Exception as _bot_import_err:
    print(f"❌ Failed to import AccountService/MarketWorker: {_bot_import_err}")
    traceback.print_exc()
    _AccountService = None  # type: ignore
    _MarketWorker   = None  # type: ignore

# ── Portfolio history helpers (graceful fallback for older bot.py) ────────────
try:
    from bot import (                              # type: ignore
        portfolio_history_backfill,
        portfolio_history_snapshot,
        portfolio_history_get,
        prepare_chart_history,
        sanitize_portfolio_history,
        PORTFOLIO_FLAT_EPSILON,
        is_trading_allowed,
        _TRADING_TZ_NAME,
        TRADING_DAYS,
        _DAY_NAMES,
        MARKET_INTERVAL_SLUG,
    )
    _portfolio_available = True
except ImportError:
    print("⚠️  Portfolio history / schedule functions not found in bot.py — "
          "persistent history and weekend gating disabled. Deploy the updated bot.py.")
    portfolio_history_backfill   = None   # type: ignore
    portfolio_history_snapshot   = None   # type: ignore
    portfolio_history_get        = None   # type: ignore
    prepare_chart_history        = None   # type: ignore
    sanitize_portfolio_history   = None   # type: ignore
    PORTFOLIO_FLAT_EPSILON       = 0.005
    _portfolio_available         = False

# ── Positions / trade-history helpers (graceful fallback) ────────────────────
try:
    from bot import (                              # type: ignore
        collect_open_positions,
        get_trade_history,
        trade_history_backfill,
        compute_biggest_realized_win,
        find_worker,
        find_worker_by_asset,
        asset_cooldown,
    )
    _positions_available = True
except ImportError:
    print("⚠️  Positions/trade-history functions not found in bot.py.")
    _positions_available = False

    def collect_open_positions(workers):  # type: ignore
        return []

    def get_trade_history(limit: int = 10):  # type: ignore
        return []

    def trade_history_backfill() -> int:  # type: ignore
        return 0

    def compute_biggest_realized_win() -> float:  # type: ignore
        return 0.0

    def find_worker(workers, asset, window):  # type: ignore
        return None

    def find_worker_by_asset(workers, asset) -> "MarketWorker | None":  # type: ignore
        return None

    asset_cooldown = None  # type: ignore

# MARKET_INTERVAL_SLUG comes from bot when available; config holds asset list only.
try:
    from bot import MARKET_INTERVAL_SLUG  # type: ignore
except Exception:
    if "MARKET_INTERVAL_SLUG" not in globals():
        MARKET_INTERVAL_SLUG = os.getenv("MARKET_INTERVAL", "5m")  # type: ignore


# ─────────────────────────────────────────────────────────────────────────────
# Display name derived from the Render URL
# ─────────────────────────────────────────────────────────────────────────────

def _bot_name_from_url(url: str) -> str:
    if not url:
        return "Emiliano"
    try:
        host = url.replace("https://", "").replace("http://", "").split(".")[0]
        return host.replace("-", " ").title()
    except Exception:
        return "Emiliano"


def _app_config_payload() -> dict:
    return {
        "trading_assets":           list(TRADING_ASSETS_UPPER),
        "workers":                  [
            {"asset": w.asset.upper(), "window": w.window} for w in WORKER_CONFIGS
        ],
        "market_interval":          "5m",
        "trading_assets_label":     trading_assets_label(),
        "asset_max_cumulative_loss": ASSET_MAX_CUMULATIVE_LOSS,
        "asset_cooldown_minutes":   ASSET_COOLDOWN_MINUTES,
    }


BOT_DISPLAY_NAME = _bot_name_from_url(os.getenv("RENDER_EXTERNAL_URL", ""))
TRADING_ASSETS_SUBTITLE = trading_assets_label(" · ")


app = FastAPI(title=f"{BOT_DISPLAY_NAME} Dashboard")

try:
    app.mount("/static", StaticFiles(directory="static"), name="static")
except Exception:
    pass

bots:               list[Any]      = []
account:            Any            = None
active_connections: list[WebSocket] = []

# ── In-memory snapshot buffer ─────────────────────────────────────────────────
# Trailing in-memory point for live flat extension (not persisted every 60s).
_snapshot_buffer:    list[dict] = []
_last_redis_flush:   float      = 0.0
_SNAPSHOT_EVERY_SEC: int        = 60   # record a portfolio snapshot this often
_FLUSH_EVERY_SEC:    int        = 60   # flush buffer → Redis this often

_biggest_win_cache: dict[str, float | int] = {'value': 0.0, 'last_total_trades': -1}


# ─────────────────────────────────────────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup_event():
    """
    Return immediately so uvicorn binds $PORT before Render's port scan.
    Bot/wallet init runs in a background task (can take 30s+ on RPC auth).
    """
    print("🚀 Emiliano Dashboard — HTTP server up, initializing bots in background...")
    asyncio.create_task(_initialize_bots())


async def _initialize_bots():
    global bots, account
    print("🚀 Emiliano Dashboard Starting on Render...")
    bots = []

    if _AccountService is None or _MarketWorker is None:
        print("❌ Cannot start bots — AccountService/MarketWorker failed to import.")
        return

    try:
        # ── One-time account-level init ───────────────────────────────────
        print("Initializing AccountService (wallet audit runs once here)...")
        account = _AccountService()
        if not account.run_wallet_audit():
            print("❌ Wallet audit failed — aborting bot startup.")
            return
        account.start_pnl_merge_scheduler()
        print("✅ AccountService ready.")

        # ── Per-asset market workers ──────────────────────────────────────
        for wc in WORKER_CONFIGS:
            print(f"Initializing {wc.asset.upper()} {wc.window} Bot...")
            bots.append(_MarketWorker(wc, account))
            print(f"✅ {wc.asset.upper()} {wc.window} Bot initialized")

        # ── Portfolio history backfill ────────────────────────────────────
        # Runs exactly once at startup.  Reads emiliano:{asset}:trades from
        # Redis for every asset, reconstructs the full historical equity curve,
        # and writes it to emiliano:portfolio:history so the chart immediately
        # shows all past performance — not just data from today.
        #
        # The backfill is idempotent: if history already covers every trade
        # record it does nothing.  It is safe to redeploy repeatedly.
        if _portfolio_available and portfolio_history_backfill:
            try:
                n = portfolio_history_backfill()
                if n > 0:
                    print(f"📈 [backfill] {n} historical portfolio points reconstructed "
                          "from existing Redis trade records.")
                else:
                    print("ℹ️  [backfill] History already up-to-date (or no trade records found).")
            except Exception as bf_err:
                print(f"⚠️  [backfill] Non-fatal error: {bf_err}")
                traceback.print_exc()

        if _positions_available and trade_history_backfill:
            try:
                n_th = trade_history_backfill()
                if n_th > 0:
                    print(f"📜 [trade-history] {n_th} execution records backfilled.")
            except Exception as th_err:
                print(f"⚠️  [trade-history] Non-fatal backfill error: {th_err}")
                traceback.print_exc()

        global _biggest_win_cache
        _biggest_win_cache['value'] = _compute_biggest_win()
        _biggest_win_cache['last_total_trades'] = (
            sum(getattr(b, "wins", 0) + getattr(b, "losses", 0) for b in bots)
        )

        asyncio.create_task(run_all_bots())
        print("🎉 All bots started successfully!")

    except Exception as e:
        print(f"❌ Bot init failed: {e}")
        traceback.print_exc()


# ─────────────────────────────────────────────────────────────────────────────
# BACKGROUND TASKS
# ─────────────────────────────────────────────────────────────────────────────

async def run_all_bots():
    if not bots:
        return
    try:
        await asyncio.gather(
            *[bot.start() for bot in bots],
            broadcast_loop(),
            keep_alive_heartbeat(),
            portfolio_snapshot_loop(),
        )
    except Exception as e:
        print(f"Background tasks error: {e}")
        traceback.print_exc()


async def keep_alive_heartbeat():
    render_url         = os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")
    ping_interval      = 10 * 60
    heartbeat_interval = 25
    seconds_since_ping = 0

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                loop_time = asyncio.get_running_loop().time()
                print(f"💓 Heartbeat [{loop_time:.0f}] — {len(bots)} bots active")
                seconds_since_ping += heartbeat_interval
                if render_url and seconds_since_ping >= ping_interval:
                    seconds_since_ping = 0
                    try:
                        async with session.get(
                            f"{render_url}/api/status",
                            timeout=aiohttp.ClientTimeout(total=10),
                        ) as resp:
                            print(f"🌐 Self-ping → {resp.status}")
                    except Exception as ping_err:
                        print(f"⚠️  Self-ping failed: {ping_err}")
            except Exception:
                pass
            await asyncio.sleep(heartbeat_interval)


async def portfolio_snapshot_loop():
    """Sync portfolio PnL to Redis only when value changes; buffer trailing t in memory."""
    global _last_redis_flush, _snapshot_buffer
    _flat_eps = PORTFOLIO_FLAT_EPSILON

    while True:
        await asyncio.sleep(_SNAPSHOT_EVERY_SEC)
        try:
            stats     = get_global_stats()
            total_pnl = stats.get("total_pnl", 0.0)
            try:
                total_pnl = round(float(total_pnl), 4)
                if total_pnl != total_pnl or total_pnl in (float("inf"), float("-inf")):
                    raise ValueError("non-finite total_pnl")
            except (TypeError, ValueError):
                print(f"⚠️ portfolio_snapshot_loop: rejected non-finite total_pnl={total_pnl!r}")
                continue

            now_ms = int(_time.time() * 1000)
            if _snapshot_buffer and abs(_snapshot_buffer[-1]["v"] - total_pnl) <= _flat_eps:
                _snapshot_buffer[-1]["t"] = now_ms
            else:
                _snapshot_buffer = [{"t": now_ms, "v": total_pnl}]

            now = _time.time()
            if _portfolio_available and portfolio_history_snapshot and (
                    now - _last_redis_flush >= _FLUSH_EVERY_SEC):
                _last_redis_flush = now
                portfolio_history_snapshot(total_pnl)
                print(f"💾 Portfolio history synced. Total PnL=${total_pnl:.2f}")

        except Exception as e:
            print(f"⚠️ portfolio_snapshot_loop error: {e}")


async def broadcast_loop():
    while True:
        try:
            bot_data = []
            for bot in bots:
                try:
                    bot_data.append(bot.get_dashboard_data())
                except Exception as e:
                    bot_data.append({
                        "asset":    getattr(bot, "asset_type", "ERROR").upper(),
                        "status":   "ERROR",
                        "position": str(e)[:80],
                    })

            data = {
                "bots":          bot_data,
                "global_stats":  get_global_stats(),
                "positions":     collect_open_positions(bots),
                "trade_history": get_trade_history(10),
                "config":        _app_config_payload(),
                "timestamp":     asyncio.get_running_loop().time(),
            }
            message = json.dumps(data)
            for ws in active_connections[:]:
                try:
                    await ws.send_text(message)
                except Exception:
                    if ws in active_connections:
                        active_connections.remove(ws)

            await asyncio.sleep(1.0)
        except Exception as e:
            print(f"Broadcast error: {e}")
            await asyncio.sleep(3)


def _compute_biggest_win() -> float:
    return compute_biggest_realized_win()


def get_global_stats() -> dict:
    global _biggest_win_cache
    # Compute trading schedule status once so it's consistent across the dict.
    _trading_ok = is_trading_allowed()

    if not bots:
        return {
            "total_bots": 0, "active_bots": 0, "total_pnl": 0.0,
            "in_profit": 0, "in_loss": 0, "total_trades": 0,
            "total_wins": 0, "total_losses": 0, "win_rate": 0.0,
            "trading_allowed": _trading_ok,
            "trading_tz":      _TRADING_TZ_NAME,
            "assets_in_cooldown": 0,
            "asset_max_loss": ASSET_MAX_CUMULATIVE_LOSS,
            "asset_cooldown_minutes": ASSET_COOLDOWN_MINUTES,
            "biggest_win": round(_biggest_win_cache['value'], 2),
        }

    total_pnl       = 0.0
    active_count    = 0
    total_wins      = 0
    total_losses    = 0
    in_profit_count = 0
    in_loss_count   = 0

    for bot in bots:
        try:
            total_pnl    += getattr(bot, "cumulative_pnl", 0.0)
            total_wins   += getattr(bot, "wins",    0)
            total_losses += getattr(bot, "losses",  0)
            in_position = (
                getattr(bot, "spread_inventory", None) is not None
                and (
                    bot.spread_inventory.yes_shares > 0
                    or bot.spread_inventory.no_shares > 0
                )
            )
            if bot.active_market or in_position:
                active_count += 1
            if in_position:
                pnl_dollars, _, _ = bot.get_current_pnl()
                if pnl_dollars > 0:
                    in_profit_count += 1
                elif pnl_dollars < 0:
                    in_loss_count += 1
        except Exception:
            continue

    total_trades = total_wins + total_losses
    win_rate     = round((total_wins / total_trades) * 100, 1) if total_trades > 0 else 0.0

    if total_trades != _biggest_win_cache['last_total_trades']:
        _biggest_win_cache['value'] = _compute_biggest_win()
        _biggest_win_cache['last_total_trades'] = total_trades

    workers_in_cooldown = 0
    if asset_cooldown is not None:
        try:
            for wc in WORKER_CONFIGS:
                if asset_cooldown.get_status(wc.asset, wc.window).get("cooldown_active"):
                    workers_in_cooldown += 1
        except Exception:
            pass

    return {
        "total_bots":           len(bots),
        "active_bots":          active_count,
        "total_pnl":            round(total_pnl, 2),
        "in_profit":            in_profit_count,
        "in_loss":              in_loss_count,
        "total_trades":         total_trades,
        "total_wins":           total_wins,
        "total_losses":         total_losses,
        "win_rate":             win_rate,
        "trading_allowed":      _trading_ok,
        "trading_tz":           _TRADING_TZ_NAME,
        "assets_in_cooldown":   workers_in_cooldown,
        "workers_in_cooldown":  workers_in_cooldown,
        "asset_max_loss":       ASSET_MAX_CUMULATIVE_LOSS,
        "asset_cooldown_minutes": ASSET_COOLDOWN_MINUTES,
        "biggest_win":          round(_biggest_win_cache['value'], 2),
    }


# ─────────────────────────────────────────────────────────────────────────────
# /api/history  — portfolio equity-curve endpoint
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/history")
async def api_history(period: str = "1D"):
    """
    Returns the portfolio equity curve for the requested period.

    Query params
    ────────────
    period : '1D' | '1W' | '1M' | '1Y' | 'ALL'   (default '1D')

    Response
    ────────
    { "points": [ {"t": unix_ms, "v": cumulative_pnl_dollars}, ... ] }

    Data sources: Redis history + in-memory trailing buffer.
    Period filter keeps points within the lookback window but x-axis domain is
    data-driven (first→last timestamp), never padded with empty pre-data time.
    """
    now_ms = int(_time.time() * 1000)
    persisted: list = []
    if _portfolio_available and portfolio_history_get:
        try:
            persisted = portfolio_history_get("ALL")
        except Exception as e:
            print(f"⚠️ /api/history Redis read error: {e}")

    merged = list(persisted) + list(_snapshot_buffer)

    if _portfolio_available and prepare_chart_history:
        merged = prepare_chart_history(merged, period, now_ms=now_ms)
    elif _portfolio_available and sanitize_portfolio_history:
        merged = sanitize_portfolio_history(merged, drop_isolated_spikes=True, collapse_flat_runs=True)  # type: ignore

    return JSONResponse({"points": merged})


# ─────────────────────────────────────────────────────────────────────────────
# HTML DASHBOARD
# ─────────────────────────────────────────────────────────────────────────────

HTML_CONTENT = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>__BOT_NAME__ • Live</title>

  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&family=JetBrains+Mono:wght@400;600;700&display=swap" rel="stylesheet">
  <script src="https://cdn.tailwindcss.com"></script>
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css">

  <style>
    *, *::before, *::after { box-sizing: border-box; }
    body { font-family: 'Inter', system-ui, sans-serif; -webkit-font-smoothing: antialiased; }
    .mono, .font-mono, [class*="font-mono"] {
      font-family: 'JetBrains Mono','Consolas','Menlo',monospace;
      font-variant-numeric: tabular-nums;
    }
    .font-mono { font-family: 'JetBrains Mono','Consolas','Menlo',monospace !important; font-variant-numeric: tabular-nums; }
    .card { transition: all 0.3s cubic-bezier(0.4,0,0.2,1); }
    .card:hover { transform: translateY(-3px); }
    .sig-strong-bull  { color: #22c55e; font-weight: 700; }
    .sig-strong-bear  { color: #ef4444; font-weight: 700; }
    .sig-mild-bull    { color: #86efac; }
    .sig-mild-bear    { color: #fca5a5; }
    .sig-neutral      { color: #71717a; }
    .pill-strong-bull { background: rgba(34,197,94,.15);   color: #22c55e; }
    .pill-strong-bear { background: rgba(239,68,68,.15);   color: #ef4444; }
    .pill-mild-bull   { background: rgba(134,239,172,.12); color: #86efac; }
    .pill-mild-bear   { background: rgba(252,165,165,.12); color: #fca5a5; }
    .pill-neutral     { background: rgba(113,113,122,.15); color: #71717a; }

    .pm-card { background:#18181b; border-radius:24px; overflow:hidden; margin-bottom:24px; }
    .pm-card-hdr { display:flex; justify-content:space-between; align-items:center; padding:18px 20px 4px; }
    .pm-label-grp { display:flex; align-items:center; gap:7px; }
    .pm-tri { font-size:11px; font-weight:700; line-height:1; }
    .pm-tri.pos { color:#22c55e; } .pm-tri.neg { color:#ef4444; }
    .pm-lbl-txt { font-size:12px; font-weight:500; color:#71717a; letter-spacing:.04em; text-transform:uppercase; }
    .pm-per-tabs { display:flex; gap:1px; }
    .pm-per-btn {
      background:none; border:none; color:#52525b;
      font-family:'Inter',system-ui,sans-serif;
      font-size:11px; font-weight:600; padding:5px 8px; border-radius:6px;
      cursor:pointer; transition:all .15s; letter-spacing:.03em;
    }
    .pm-per-btn.act { background:#22c55e; color:#0a0a0a; }
    .pm-val-block { padding:6px 20px 0; display:flex; flex-direction:column; align-items:flex-start; gap:4px; }
    .pm-balance-row { display:flex; align-items:center; gap:10px; flex-wrap:wrap; }
    .pm-balance-toggle {
      background:none; border:none; color:#71717a; cursor:pointer;
      padding:8px; border-radius:10px; transition:color .15s,background .15s;
      font-size:17px; line-height:1; flex-shrink:0;
    }
    .pm-balance-toggle:hover { color:#a1a1aa; background:rgba(255,255,255,0.06); }
    .pm-balance-toggle:focus-visible { outline:2px solid #22c55e; outline-offset:2px; }
    .pm-big-val { font-family:'Inter',system-ui,sans-serif; font-size:40px; font-weight:800; line-height:1; letter-spacing:-.04em; }
    .pm-big-val.pos { color:#22c55e; } .pm-big-val.neg { color:#ef4444; } .pm-big-val.neu { color:#e4e4e7; }
    .ps-stat-val { font-family:'Inter',system-ui,sans-serif; font-size:20px; font-weight:700; line-height:1.2; letter-spacing:-.02em; font-variant-numeric:tabular-nums; }
    .pm-change-lbl { font-family:'JetBrains Mono','Consolas',monospace; font-variant-numeric:tabular-nums; font-size:12px; font-weight:600; color:#71717a; letter-spacing:.01em; }
    .pm-change-lbl.pos { color:#22c55e; } .pm-change-lbl.neg { color:#ef4444; }
    .pm-chart-box { height:160px; position:relative; overflow:hidden; margin-top:12px; }
    .pm-chart-box svg { display:block; width:100%; height:100%; pointer-events:none; }
    .pm-no-data { position:absolute; top:50%; left:50%; transform:translate(-50%,-50%); font-size:12px; color:#52525b; white-space:nowrap; pointer-events:none; }
    #pm-chart-overlay { position:absolute; inset:0; z-index:10; cursor:crosshair; touch-action:pan-y; }
    .pm-stats-row { display:flex; border-top:1px solid rgba(255,255,255,0.06); padding:12px 20px; align-items:center; justify-content:space-between; }
    .pm-stat-item { display:flex; flex-direction:column; align-items:center; }
    .pm-stat-num { font-family:'JetBrains Mono','Consolas',monospace; font-variant-numeric:tabular-nums; font-size:15px; font-weight:700; line-height:1.2; }
    .pm-stat-lbl { font-size:9px; color:#52525b; text-transform:uppercase; letter-spacing:.1em; margin-top:3px; }

    /* Portfolio section tabs — Trades / Positions / History */
    .pm-section-tabs { display:flex; gap:0; border-bottom:1px solid rgba(255,255,255,0.06); padding:0 20px; }
    .pm-section-tab {
      background:none; border:none; border-bottom:2px solid transparent;
      color:#71717a; font-family:'Inter',system-ui,sans-serif;
      font-size:13px; font-weight:600; padding:14px 16px 12px; margin-bottom:-1px;
      cursor:pointer; transition:color .15s,border-color .15s;
    }
    .pm-section-tab:hover { color:#a1a1aa; }
    .pm-section-tab.act { color:#fafafa; border-bottom-color:#22c55e; }
    .pm-section-panel { padding:0; min-height:120px; }
    .pm-section-panel.hidden { display:none; }
    .pm-trades-panel { padding:16px 20px 20px; }
    .pm-trades-grid {
      display:grid; grid-template-columns:1fr; gap:16px;
    }
    @media (min-width:768px) {
      .pm-trades-grid { grid-template-columns:repeat(2,1fr); gap:24px; }
    }
    .pm-empty { padding:32px 20px; text-align:center; color:#52525b; font-size:13px; }
    .pm-row {
      display:flex; align-items:center; justify-content:space-between; gap:12px;
      padding:14px 20px; border-bottom:1px solid rgba(255,255,255,0.04);
      transition:background .12s;
    }
    .pm-row:last-child { border-bottom:none; }
    .pm-row:hover { background:rgba(255,255,255,0.02); }
    .pm-row-main { flex:1; min-width:0; }
    .pm-row-title { font-size:13px; font-weight:600; color:#e4e4e7; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
    .pm-row-sub { font-size:11px; color:#71717a; margin-top:3px; display:flex; flex-wrap:wrap; gap:8px; align-items:center; }
    .pm-side-yes { color:#22c55e; font-weight:600; font-size:11px; }
    .pm-side-no  { color:#ef4444; font-weight:600; font-size:11px; }
    .pm-action-buy  { color:#22c55e; font-weight:700; font-size:11px; letter-spacing:.04em; }
    .pm-action-sell { color:#f97316; font-weight:700; font-size:11px; letter-spacing:.04em; }
    .pm-action-redeem { color:#38bdf8; font-weight:700; font-size:11px; letter-spacing:.04em; }
    .pm-row-stats { display:flex; flex-direction:column; align-items:flex-end; gap:2px; flex-shrink:0; }
    .pm-row-val { font-family:'JetBrains Mono','Consolas',monospace; font-variant-numeric:tabular-nums; font-size:13px; font-weight:600; color:#e4e4e7; }
    .pm-row-val.pos { color:#22c55e; } .pm-row-val.neg { color:#ef4444; }
    .pm-row-meta { font-size:10px; color:#52525b; font-family:'JetBrains Mono','Consolas',monospace; }
    .pm-row-meta.pos { color:#22c55e; } .pm-row-meta.neg { color:#ef4444; }
    .pm-cashout-btn {
      background:rgba(34,197,94,.12); color:#22c55e; border:1px solid rgba(34,197,94,.25);
      font-family:'Inter',system-ui,sans-serif; font-size:11px; font-weight:700;
      padding:6px 12px; border-radius:8px; cursor:pointer; flex-shrink:0;
      transition:background .15s,opacity .15s; letter-spacing:.02em;
    }
    .pm-cashout-btn:hover:not(:disabled) { background:rgba(34,197,94,.22); }
    .pm-cashout-btn:disabled { opacity:.45; cursor:not-allowed; }
    @media (max-width:640px) {
      .pm-row { flex-wrap:wrap; }
      .pm-row-stats { align-items:flex-start; width:100%; flex-direction:row; justify-content:space-between; }
      .pm-cashout-btn { width:100%; text-align:center; margin-top:4px; }
    }
  </style>
</head>

<body class="bg-zinc-950 text-zinc-100 min-h-screen p-4 md:p-6">
<div class="max-w-7xl mx-auto">

  <!-- ── Header ──────────────────────────────────────────────── -->
  <div class="flex items-center justify-between mb-5">
    <div>
      <h1 class="text-4xl font-bold"
          style="font-family:'JetBrains Mono','Consolas',monospace;letter-spacing:-.03em;line-height:1;">
        __BOT_NAME__
      </h1>
      <p id="assets-subtitle" class="text-zinc-500 text-sm mt-1 font-mono">__TRADING_ASSETS__</p>
    </div>
    <span id="conn-dot" class="w-2.5 h-2.5 rounded-full bg-zinc-600 inline-block" title="WebSocket status"></span>
  </div>

  <!-- ── Trading schedule banner ─────────────────────────────────── -->
  <div id="schedule-banner"
       class="mb-4 px-4 py-2 rounded-xl text-xs font-semibold flex items-center gap-2
              bg-emerald-950 text-emerald-400 border border-emerald-900"
       style="font-family:'JetBrains Mono','Consolas',monospace;letter-spacing:.02em;
              transition:background .4s,color .4s,border-color .4s;">
    <span id="schedule-dot" class="w-2 h-2 rounded-full bg-emerald-400 inline-block flex-shrink-0"></span>
    <span id="schedule-text">Checking trading schedule…</span>
  </div>

  <!-- ── Profile stats strip (Polymarket-style) ─────────────────── -->
  <div id="profile-stats-strip" class="grid grid-cols-3 w-full mb-5">
    <div class="flex flex-col gap-1">
      <span class="text-[10px] font-semibold uppercase tracking-widest text-zinc-500"
            style="font-family:'Inter',system-ui,sans-serif;">Positions Value</span>
      <span class="ps-stat-val text-zinc-200" id="ps-positions-value">$0.00</span>
    </div>
    <div class="flex flex-col gap-1">
      <span class="text-[10px] font-semibold uppercase tracking-widest text-zinc-500"
            style="font-family:'Inter',system-ui,sans-serif;">Biggest Win</span>
      <span class="ps-stat-val text-zinc-200" id="ps-biggest-win">$0.00</span>
    </div>
    <div class="flex flex-col gap-1">
      <span class="text-[10px] font-semibold uppercase tracking-widest text-zinc-500"
            style="font-family:'Inter',system-ui,sans-serif;">Predictions</span>
      <span class="ps-stat-val text-zinc-100" id="ps-predictions">0</span>
    </div>
  </div>

  <p id="math-quote" class="text-[#22c55e] mb-5 text-sm font-bold"
     style="font-family:'JetBrains Mono','Consolas',monospace;letter-spacing:-.03em;line-height:1;transition:opacity .6s ease;"></p>
  <script>
  (function(){
    var q=[
      '"The only way to learn mathematics is to do mathematics." — Paul Halmos',
      '"Mathematics is the language in which God has written the universe." — Galileo Galilei',
      '"In mathematics you don\'t understand things. You just get used to them." — John von Neumann',
      '"God made the integers; all else is the work of man." — Leopold Kronecker',
      '"Pure mathematics is, in its way, the poetry of logical ideas." — Albert Einstein',
      '"Mathematics is not about numbers, equations, computations, or algorithms: it is about understanding." — William Paul Thurston',
      '"Do not worry about your difficulties in mathematics. I can assure you mine are still greater." — Albert Einstein',
      '"A mathematician is a machine for turning coffee into theorems." — Paul Erdős',
      '"Without mathematics, there\'s nothing you can do. Everything around you is mathematics." — Shakuntala Devi',
      '"The essence of mathematics lies in its freedom." — Georg Cantor',
      '"If people do not believe that mathematics is simple, it is only because they do not realize how complicated life is." — John von Neumann',
      '"An equation for me has no meaning unless it represents a thought of God." — Srinivasa Ramanujan',
      '"It is not knowledge, but the act of learning, not possession but the act of getting there, which grants the greatest enjoyment." — Carl Friedrich Gauss',
      '"Mathematics is the queen of the sciences and number theory is the queen of mathematics." — Carl Friedrich Gauss',
      '"No human investigation can be called real science if it cannot be demonstrated mathematically." — Leonardo da Vinci',
    ];
    var el=document.getElementById('math-quote');
    var idx=Math.floor(Math.random()*q.length);
    function next(){el.style.opacity='0';setTimeout(function(){idx=(idx+1)%q.length;el.textContent=q[idx];el.style.opacity='1';},600);}
    el.textContent=q[idx]; el.style.opacity='1';
    setInterval(next,12000);
  })();
  </script>

  <!-- ── P&L Chart Card ──────────────────────────────────────── -->
  <div class="pm-card">
    <div class="pm-card-hdr">
      <div class="pm-label-grp">
        <span class="pm-tri neg" id="pm-tri">▼</span>
        <span class="pm-lbl-txt">Portfolio P&amp;L</span>
      </div>
      <div class="pm-per-tabs">
        <button class="pm-per-btn act" id="ppb-1D"  onclick="setPeriod('1D')">1D</button>
        <button class="pm-per-btn"     id="ppb-1W"  onclick="setPeriod('1W')">1W</button>
        <button class="pm-per-btn"     id="ppb-1M"  onclick="setPeriod('1M')">1M</button>
        <button class="pm-per-btn"     id="ppb-1Y"  onclick="setPeriod('1Y')">1Y</button>
        <button class="pm-per-btn"     id="ppb-ALL" onclick="setPeriod('ALL')">ALL</button>
      </div>
    </div>
    <div class="pm-val-block">
      <div class="pm-balance-row">
        <div class="pm-big-val neu" id="pm-bigval">$0.00</div>
        <button class="pm-balance-toggle" id="pm-balance-toggle" type="button"
                aria-label="Hide balances" aria-pressed="true" onclick="toggleBalanceVisibility()">
          <i class="fa-solid fa-eye" id="pm-balance-toggle-icon"></i>
        </button>
      </div>
      <span class="pm-change-lbl" id="pm-change-lbl">Loading history…</span>
    </div>
    <div class="pm-chart-box" id="pm-chart-box">
      <div class="pm-no-data" id="pm-no-data">Loading history…</div>
      <div id="pm-chart-overlay"></div>
    </div>
    <div class="pm-stats-row">
      <div class="pm-stat-item">
        <span class="pm-stat-num text-zinc-200" id="pm-st-trades">0</span>
        <span class="pm-stat-lbl">Trades</span>
      </div>
      <div class="pm-stat-item">
        <span class="pm-stat-num text-emerald-400" id="pm-st-wins">0</span>
        <span class="pm-stat-lbl">Wins</span>
      </div>
      <div class="pm-stat-item">
        <span class="pm-stat-num text-red-400" id="pm-st-losses">0</span>
        <span class="pm-stat-lbl">Losses</span>
      </div>
      <div class="pm-stat-item">
        <span class="pm-stat-num text-yellow-400" id="pm-st-wr">--%</span>
        <span class="pm-stat-lbl">Win Rate</span>
      </div>
    </div>
  </div>

  <!-- ── Trades / Positions / History (Polymarket-style) ───────── -->
  <div class="pm-card" id="portfolio-tabs-card">
    <div class="pm-section-tabs">
      <button class="pm-section-tab act" id="tab-btn-trades" onclick="setPortfolioTab('trades')">Trades</button>
      <button class="pm-section-tab" id="tab-btn-positions" onclick="setPortfolioTab('positions')">Positions (0)</button>
      <button class="pm-section-tab" id="tab-btn-history" onclick="setPortfolioTab('history')">History</button>
    </div>
    <div id="panel-trades" class="pm-section-panel pm-trades-panel">
      <div id="bots-container" class="pm-trades-grid"></div>
    </div>
    <div id="panel-positions" class="pm-section-panel hidden">
      <div id="positions-list"></div>
    </div>
    <div id="panel-history" class="pm-section-panel hidden">
      <div id="history-list"></div>
    </div>
  </div>

</div>

<script>
// ═══════════════════════════════════════════════════════════════════
// PORTFOLIO HISTORY CHART ENGINE
//
// • Points only on PnL changes (no heartbeat spam).
// • X-axis domain = [first data timestamp, last data timestamp] — never
//   padded with empty pre-data time (e.g. new session at 8:04pm starts there).
// • Period tabs filter the lookback window server-side; scaling is always
//   data-driven on the client.
// ═══════════════════════════════════════════════════════════════════

const _PERIOD_MS = {
  '1D':  24 * 60 * 60 * 1000,
  '1W':  7  * 24 * 60 * 60 * 1000,
  '1M':  30 * 24 * 60 * 60 * 1000,
  '1Y':  365 * 24 * 60 * 60 * 1000,
};
const _FLAT_EPS = 0.005;
const _BALANCE_LS_KEY = 'pm_balance_visible';

let _balanceVisible = true;
let _profileStats = { positionsValue: 0, biggestWin: 0, predictions: 0 };
let _historyPts    = [];
let _livePts       = [];
let _pnlPeriod     = '1D';
let _historyLoaded = false;
let _loadingHist   = false;
let _lastLivePnl   = null;
let _lastLiveTs    = 0;
let _chartRaf      = 0;

function _isFiniteNum(v) {
  return typeof v === 'number' && isFinite(v);
}

function _loadBalancePref() {
  try {
    const v = localStorage.getItem(_BALANCE_LS_KEY);
    if (v === '0' || v === 'false') _balanceVisible = false;
  } catch (e) { /* private browsing */ }
}

function _saveBalancePref() {
  try {
    localStorage.setItem(_BALANCE_LS_KEY, _balanceVisible ? '1' : '0');
  } catch (e) { /* private browsing */ }
}

function _maskUsd() { return '••••'; }
function _maskCents() { return '•••'; }

function _displayBigVal(val) {
  if (!_balanceVisible) return _maskUsd();
  return (val >= 0 ? '' : '-') + '$' + Math.abs(val).toFixed(2);
}

function _displayPeriodChange(change, pct, arrow, periodLabel) {
  const pctStr = pct !== null
    ? ` (${change >= 0 ? '+' : ''}${pct.toFixed(1)}%)`
    : '';
  if (!_balanceVisible) {
    const maskedPart = pct !== null ? pctStr : _maskUsd();
    return `${maskedPart}  ${arrow}  ${periodLabel}`;
  }
  const sign = change >= 0 ? '+' : '';
  return `${sign}$${Math.abs(change).toFixed(2)}${pctStr}  ${arrow}  ${periodLabel}`;
}

function _updateBalanceToggleUI() {
  const btn = document.getElementById('pm-balance-toggle');
  const icon = document.getElementById('pm-balance-toggle-icon');
  if (!btn || !icon) return;
  btn.setAttribute('aria-pressed', _balanceVisible ? 'true' : 'false');
  btn.setAttribute('aria-label', _balanceVisible ? 'Hide balances' : 'Show balances');
  icon.className = _balanceVisible ? 'fa-solid fa-eye' : 'fa-solid fa-eye-slash';
}

function toggleBalanceVisibility() {
  _balanceVisible = !_balanceVisible;
  _saveBalancePref();
  _updateBalanceToggleUI();
  refreshBalanceSensitiveUI();
}

function refreshBalanceSensitiveUI() {
  updatePnlChart();
  renderPositions(window._lastPositions || []);
  if (window._lastBots) renderBots(window._lastBots);
  if (window._lastTradeHistory) renderTradeHistory(window._lastTradeHistory);
  renderProfileStats(window._lastBots || [], window._lastGlobalStats || {});
}

function _updatePositionsTabCount(count) {
  const btn = document.getElementById('tab-btn-positions');
  if (btn) btn.textContent = 'Positions (' + (count || 0) + ')';
}

function _periodLabel(p, tMin, tMax) {
  const labels = {'1D':'Past 24h','1W':'Past 7 Days','1M':'Past 30 Days',
                  '1Y':'Past Year','ALL':'All Time'};
  const full = _PERIOD_MS[p];
  const span = (tMax && tMin) ? (tMax - tMin) : 0;
  if (!full || p === 'ALL' || span >= full * 0.9) return labels[p] || p;
  if (span < 24 * 60 * 60 * 1000) {
    return 'Since ' + new Date(tMin).toLocaleTimeString([], {hour:'2-digit', minute:'2-digit'});
  }
  return 'Since ' + new Date(tMin).toLocaleDateString([], {month:'short', day:'numeric'});
}

function _mergedPts() {
  const byT = new Map();
  for (const p of _historyPts) {
    if (p && Number.isFinite(p.t) && _isFiniteNum(p.v)) byT.set(p.t, p.v);
  }
  for (const p of _livePts) {
    if (p && Number.isFinite(p.t) && _isFiniteNum(p.v)) byT.set(p.t, p.v);
  }
  return Array.from(byT.entries())
    .map(([t, v]) => ({ t, v }))
    .sort((a, b) => a.t - b.t);
}

function _collapseFlatRuns(pts) {
  if (pts.length < 2) return pts.slice();
  const out = [{ t: pts[0].t, v: pts[0].v }];
  for (let i = 1; i < pts.length; i++) {
    const prev = out[out.length - 1];
    if (Math.abs(pts[i].v - prev.v) <= _FLAT_EPS) {
      prev.t = pts[i].t;
    } else {
      out.push({ t: pts[i].t, v: pts[i].v });
    }
  }
  return out;
}

/** Data-driven x domain: first real point → last real point (no fixed 24h padding). */
function _chartTimeDomain(pts) {
  if (!pts.length) {
    const now = Date.now();
    return { tMin: now, tMax: now, span: 1 };
  }
  const tMin = pts[0].t;
  const tMax = pts[pts.length - 1].t;
  return { tMin, tMax, span: Math.max(tMax - tMin, 1) };
}

function _chartRenderPts(basePts) {
  const collapsed = _collapseFlatRuns(basePts);
  if (!collapsed.length) return [];
  const last = collapsed[collapsed.length - 1];
  const trailV = (_lastLivePnl != null && _isFiniteNum(_lastLivePnl))
    ? _lastLivePnl : last.v;
  const trailT = _lastLiveTs || Date.now();
  if (trailT > last.t && Math.abs(trailV - last.v) <= _FLAT_EPS) {
    return [...collapsed, { t: trailT, v: trailV, trailing: true }];
  }
  return collapsed;
}

function _xForTime(t, tMin, span, PX, drawW) {
  return PX + ((t - tMin) / span) * drawW;
}

function _nearestPtByTime(pts, targetT) {
  if (!pts.length) return null;
  let lo = 0, hi = pts.length - 1;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (pts[mid].t < targetT) lo = mid + 1;
    else hi = mid;
  }
  if (lo > 0 && Math.abs(pts[lo - 1].t - targetT) < Math.abs(pts[lo].t - targetT)) {
    return pts[lo - 1];
  }
  return pts[lo];
}

async function loadHistory(period) {
  if (_loadingHist) return;
  _loadingHist = true;
  try {
    const res = await fetch('/api/history?period=' + period);
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const data   = await res.json();
    _historyPts  = (data.points || []).filter(p => !p.synthetic);
    _historyLoaded = true;
    _livePts = [];
  } catch(e) {
    console.warn('loadHistory error:', e);
    _historyLoaded = true;
  } finally {
    _loadingHist = false;
  }
  updatePnlChart();
}

function setPeriod(p) {
  _pnlPeriod = p;
  document.querySelectorAll('.pm-per-btn').forEach(b => {
    b.classList.toggle('act', b.id === 'ppb-' + p);
  });
  loadHistory(p);
}

function pushLivePnlPoint(totalPnl) {
  if (!_isFiniteNum(totalPnl)) return;
  const now = Date.now();
  _lastLivePnl = totalPnl;
  _lastLiveTs  = now;

  const lastLive = _livePts.length ? _livePts[_livePts.length - 1] : null;
  const histLast = _historyPts.length ? _historyPts[_historyPts.length - 1] : null;
  const ref = lastLive || histLast;

  if (ref && Math.abs(ref.v - totalPnl) <= _FLAT_EPS) {
    schedulePnlChartUpdate();
    return;
  }

  _livePts.push({ t: now, v: totalPnl });
  if (_livePts.length > 500) _livePts.splice(0, _livePts.length - 500);
  schedulePnlChartUpdate(true);
}

function schedulePnlChartUpdate(force) {
  if (_chartRaf && !force) return;
  if (_chartRaf) cancelAnimationFrame(_chartRaf);
  _chartRaf = requestAnimationFrame(() => {
    _chartRaf = 0;
    updatePnlChart();
  });
}

function updatePnlChart() {
  const basePts = _mergedPts();
  const pts = _chartRenderPts(basePts);
  const { tMin, tMax, span } = _chartTimeDomain(pts);

  const totalPnl       = basePts.length > 0 ? basePts[basePts.length - 1].v : (_lastLivePnl || 0);
  const periodStartVal = basePts.length > 0 ? basePts[0].v : totalPnl;
  const periodEndVal   = totalPnl;
  const periodChange   = periodEndVal - periodStartVal;
  const periodChangePct = periodStartVal !== 0
    ? (periodChange / Math.abs(periodStartVal)) * 100
    : null;

  const bigEl = document.getElementById('pm-bigval');
  if (bigEl) {
    bigEl.textContent = _displayBigVal(totalPnl);
    bigEl.className   = 'pm-big-val ' + (!_balanceVisible ? 'neu'
      : (totalPnl > 0 ? 'pos' : totalPnl < 0 ? 'neg' : 'neu'));
  }

  const triEl = document.getElementById('pm-tri');
  if (triEl) {
    triEl.textContent = periodChange >= 0 ? '▲' : '▼';
    triEl.className   = 'pm-tri ' + (periodChange >= 0 ? 'pos' : 'neg');
  }

  const chEl = document.getElementById('pm-change-lbl');
  if (chEl) {
    if (basePts.length > 1) {
      const arrow = periodChange >= 0 ? '▲' : '▼';
      chEl.textContent = _displayPeriodChange(
        periodChange, periodChangePct, arrow, _periodLabel(_pnlPeriod, tMin, tMax));
      chEl.className   = 'pm-change-lbl ' + (periodChange >= 0 ? 'pos' : 'neg');
    } else {
      chEl.textContent = _historyLoaded ? _periodLabel(_pnlPeriod, tMin, tMax) : 'Loading…';
      chEl.className   = 'pm-change-lbl';
    }
  }

  const chartEl = document.getElementById('pm-chart-box');
  if (!chartEl) return;

  if (pts.length < 2) {
    const old = chartEl.querySelector('svg');
    if (old) old.remove();
    const nd = chartEl.querySelector('#pm-no-data');
    if (nd) {
      nd.textContent   = _historyLoaded ? 'No data for this period yet' : 'Loading history…';
      nd.style.display = '';
    }
    window._pmChart = null;
    return;
  }

  const W = 400, H = 160, PX = 20, PY = 10;
  const vals  = pts.map(p => p.v);
  const minV  = Math.min(...vals);
  const maxV  = Math.max(...vals);
  const padV  = (maxV - minV) * 0.15 || 0.05;
  const lo = minV - padV, hi = maxV + padV;
  const vRange = hi - lo;
  const drawW  = W - PX * 2;
  const drawH  = H - PY * 2;

  const mx = t => _xForTime(t, tMin, span, PX, drawW).toFixed(2);
  const my = v  => (PY + drawH - ((v - lo) / vRange * drawH)).toFixed(2);

  const lineColor  = periodChange >= 0 ? '#22c55e' : '#ef4444';
  const lineColor2 = periodChange >= 0 ? '#16a34a' : '#dc2626';

  const dLine = pts.map((p, i) => `${i===0?'M':'L'}${mx(p.t)},${my(p.v)}`).join(' ');
  const zeroY = parseFloat(my(0));
  const czY   = Math.max(PY, Math.min(H - PY, zeroY));
  const lastX = parseFloat(mx(pts[pts.length - 1].t));
  const dArea = dLine
    + ` L${lastX.toFixed(2)},${czY.toFixed(2)}`
    + ` L${parseFloat(mx(pts[0].t)).toFixed(2)},${czY.toFixed(2)} Z`;

  const labelCount = 5;
  const xLabels = [];
  for (let i = 0; i < labelCount; i++) {
    const frac = labelCount === 1 ? 0 : i / (labelCount - 1);
    const ts = tMin + frac * span;
    const x = parseFloat(mx(ts));
    const tsStr = _pnlPeriod === '1D'
      ? new Date(ts).toLocaleTimeString([], {hour:'2-digit', minute:'2-digit'})
      : (_pnlPeriod === '1W'
        ? new Date(ts).toLocaleString([], {weekday:'short', hour:'2-digit', minute:'2-digit'})
        : new Date(ts).toLocaleDateString([], {month:'short', day:'numeric'}));
    const anchor = i === 0 ? 'start' : (i === labelCount - 1 ? 'end' : 'middle');
    xLabels.push(`<text x="${x}" y="${H-3}" text-anchor="${anchor}" font-size="8"
      fill="rgba(255,255,255,0.28)"
      font-family="'JetBrains Mono','Consolas',monospace">${tsStr}</text>`);
  }

  let zeroLine = '';
  if (minV < 0 && maxV > 0) {
    zeroLine = `<line x1="${PX}" y1="${czY.toFixed(2)}" x2="${W-PX}" y2="${czY.toFixed(2)}"
      stroke="rgba(255,255,255,0.12)" stroke-width="1" stroke-dasharray="3,3"/>`;
  }

  const lastY = parseFloat(my(pts[pts.length - 1].v));

  window._pmChart = {
    pts, PX, PY, drawW, drawH, W, H,
    tMin, tMax, span,
    period: _pnlPeriod, lo, vRange, lineColor,
    origVal: totalPnl,
    origPeriodChange: periodChange,
    origChangeTxt: chEl ? chEl.textContent : '',
    origChangeCls: chEl ? chEl.className   : '',
  };

  const ndEl = chartEl.querySelector('#pm-no-data');
  if (ndEl) ndEl.style.display = 'none';
  const oldSvg = chartEl.querySelector('svg');
  if (oldSvg) oldSvg.remove();

  const svgEl = document.createElementNS('http://www.w3.org/2000/svg','svg');
  svgEl.setAttribute('viewBox',`0 0 ${W} ${H}`);
  svgEl.setAttribute('preserveAspectRatio','none');
  svgEl.style.cssText = 'display:block;width:100%;height:100%;pointer-events:none';
  svgEl.innerHTML = `
    <defs>
      <linearGradient id="pnlGrd" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%"   stop-color="${lineColor}" stop-opacity="0.38"/>
        <stop offset="65%"  stop-color="${lineColor}" stop-opacity="0.07"/>
        <stop offset="100%" stop-color="${lineColor}" stop-opacity="0"/>
      </linearGradient>
      <linearGradient id="lineGrd" x1="0" y1="0" x2="1" y2="0">
        <stop offset="0%"   stop-color="${lineColor2}"/>
        <stop offset="100%" stop-color="${lineColor}"/>
      </linearGradient>
      <filter id="glow">
        <feGaussianBlur stdDeviation="1.5" result="coloredBlur"/>
        <feMerge><feMergeNode in="coloredBlur"/><feMergeNode in="SourceGraphic"/></feMerge>
      </filter>
      <clipPath id="chartClip">
        <rect x="${PX}" y="${PY}" width="${drawW}" height="${drawH+PY}"/>
      </clipPath>
    </defs>
    ${zeroLine}
    <g clip-path="url(#chartClip)">
      <path d="${dArea}" fill="url(#pnlGrd)"/>
      <path d="${dLine}" fill="none" stroke="url(#lineGrd)"
            stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"
            filter="url(#glow)"/>
    </g>
    ${xLabels.join('')}
    <circle cx="${lastX}" cy="${lastY}" r="3.5" fill="${lineColor}" filter="url(#glow)"/>
    <line id="xhair-line" x1="${PX}" y1="${PY}" x2="${PX}" y2="${H-14}"
          stroke="rgba(255,255,255,0.6)" stroke-width="1.2"
          stroke-dasharray="3,3" display="none"/>
    <circle id="xhair-dot" cx="${PX}" cy="${PY}" r="4.5"
            fill="${lineColor}" stroke="#111" stroke-width="1.5"
            display="none" filter="url(#glow)"/>`;

  const overlay = chartEl.querySelector('#pm-chart-overlay');
  chartEl.insertBefore(svgEl, overlay);
}

// ── Crosshair ────────────────────────────────────────────────
function _initChartOverlay() {
  const overlay = document.getElementById('pm-chart-overlay');
  if (!overlay) return;

  function _move(clientX) {
    const c = window._pmChart;
    if (!c || !c.pts || c.pts.length < 2) return;
    const box  = document.getElementById('pm-chart-box');
    if (!box) return;
    const rect = box.getBoundingClientRect();
    const frac = Math.max(0, Math.min(1, (clientX - rect.left) / rect.width));
    const targetT = c.tMin + frac * c.span;
    const pt   = _nearestPtByTime(c.pts, targetT);
    const svgX = c.PX + ((targetT - c.tMin) / c.span) * c.drawW;
    const svgY = c.PY + c.drawH - ((pt.v - c.lo) / c.vRange * c.drawH);
    const line = document.getElementById('xhair-line');
    const dot  = document.getElementById('xhair-dot');
    if (line) { line.setAttribute('x1',svgX.toFixed(2)); line.setAttribute('x2',svgX.toFixed(2));
                line.setAttribute('y1',c.PY); line.setAttribute('y2',c.H-14);
                line.removeAttribute('display'); }
    if (dot)  { dot.setAttribute('cx',svgX.toFixed(2)); dot.setAttribute('cy',svgY.toFixed(2));
                dot.removeAttribute('display'); }
    const bigEl = document.getElementById('pm-bigval');
    if (bigEl) {
      bigEl.textContent = _displayBigVal(pt.v);
      bigEl.className   = 'pm-big-val ' + (!_balanceVisible ? 'neu'
        : (pt.v > 0 ? 'pos' : pt.v < 0 ? 'neg' : 'neu'));
    }
    const triEl = document.getElementById('pm-tri');
    if (triEl) { triEl.textContent=pt.v>=0?'▲':'▼'; triEl.className='pm-tri '+(pt.v>=0?'pos':'neg'); }
    const chEl = document.getElementById('pm-change-lbl');
    if (chEl && pt.t) {
      chEl.textContent = c.period==='1D'
        ? new Date(pt.t).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit',second:'2-digit'})
        : new Date(pt.t).toLocaleString([],{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'});
      chEl.className = 'pm-change-lbl';
    }
  }
  function _leave() {
    const c   = window._pmChart;
    const line = document.getElementById('xhair-line');
    const dot  = document.getElementById('xhair-dot');
    if (line) line.setAttribute('display','none');
    if (dot)  dot.setAttribute('display','none');
    if (!c) return;
    const bigEl = document.getElementById('pm-bigval');
    if (bigEl) {
      bigEl.textContent = _displayBigVal(c.origVal);
      bigEl.className   = 'pm-big-val ' + (!_balanceVisible ? 'neu'
        : (c.origVal > 0 ? 'pos' : c.origVal < 0 ? 'neg' : 'neu'));
    }
    const triEl = document.getElementById('pm-tri');
    if (triEl) {
      triEl.textContent = c.origPeriodChange>=0?'▲':'▼';
      triEl.className   = 'pm-tri '+(c.origPeriodChange>=0?'pos':'neg');
    }
    const chEl = document.getElementById('pm-change-lbl');
    if (chEl && c.origChangeTxt) { chEl.textContent=c.origChangeTxt; chEl.className=c.origChangeCls; }
  }
  overlay.addEventListener('mousemove',   e => _move(e.clientX));
  overlay.addEventListener('mouseleave',  _leave);
  overlay.addEventListener('touchstart',  e => _move(e.touches[0].clientX), {passive:true});
  overlay.addEventListener('touchmove',   e => _move(e.touches[0].clientX), {passive:true});
  overlay.addEventListener('touchend',    _leave);
  overlay.addEventListener('touchcancel', _leave);
}

// Refresh history from backend every 2 minutes to pick up any new Redis flushes.
setInterval(() => loadHistory(_pnlPeriod), 2 * 60 * 1000);
setInterval(() => {
  if (_lastLivePnl != null) {
    _lastLiveTs = Date.now();
    schedulePnlChartUpdate();
  }
}, 30 * 1000);

// ═══════════════════════════════════════════════════════════════════
// WEBSOCKET
// ═══════════════════════════════════════════════════════════════════
const connDot = document.getElementById('conn-dot');
let ws;
function connect() {
  const proto = location.protocol==='https:' ? 'wss' : 'ws';
  ws = new WebSocket(`${proto}://${location.host}/ws`);
  ws.onopen  = () => connDot.className = 'w-2.5 h-2.5 rounded-full bg-emerald-400 inline-block';
  ws.onclose = () => {
    connDot.className = 'w-2.5 h-2.5 rounded-full bg-red-500 inline-block';
    setTimeout(connect, 2500);
  };
  ws.onerror = () => ws.close();
  ws.onmessage = e => {
    const d = JSON.parse(e.data);
    window._lastBots = d.bots || [];
    window._lastTradeHistory = d.trade_history || [];
    window._lastGlobalStats = d.global_stats || {};
    renderGlobalStats(d.global_stats);
    renderProfileStats(window._lastBots, window._lastGlobalStats);
    pushLivePnlPoint(d.global_stats.total_pnl ?? 0);
    renderBots(window._lastBots);
    renderPositions(d.positions || []);
    renderTradeHistory(window._lastTradeHistory);
    if (d.config) renderAppConfig(d.config);
  };
}
function renderAppConfig(cfg) {
  const el = document.getElementById('assets-subtitle');
  if (!el || !cfg) return;
  const assets = (cfg.trading_assets || []).join(' · ');
  const interval = cfg.market_interval || '';
  el.textContent = assets + (interval ? ` · ${interval} markets` : '');
}
connect();

async function loadPositionsAndHistory() {
  try {
    const [pRes, hRes] = await Promise.all([
      fetch('/api/positions'),
      fetch('/api/trades/history?limit=10'),
    ]);
    if (pRes.ok) renderPositions((await pRes.json()).positions || []);
    if (hRes.ok) {
      window._lastTradeHistory = (await hRes.json()).trades || [];
      renderTradeHistory(window._lastTradeHistory);
    }
  } catch (e) { console.warn('loadPositionsAndHistory:', e); }
}
loadPositionsAndHistory();

let _activePortfolioTab = 'trades';
const _cashoutPending = new Set();
const _PORTFOLIO_TABS = ['trades', 'positions', 'history'];

function setPortfolioTab(tab) {
  if (!_PORTFOLIO_TABS.includes(tab)) return;
  _activePortfolioTab = tab;
  _PORTFOLIO_TABS.forEach(t => {
    const btn = document.getElementById('tab-btn-' + t);
    const panel = document.getElementById('panel-' + t);
    if (btn) btn.classList.toggle('act', tab === t);
    if (panel) panel.classList.toggle('hidden', tab !== t);
  });
}

function _fmtUsd(v, signed) {
  if (!_balanceVisible) return _maskUsd();
  const n = Number(v) || 0;
  const sign = signed && n > 0 ? '+' : (signed && n < 0 ? '' : '');
  return sign + '$' + Math.abs(n).toFixed(2);
}

function _fmtCents(c) {
  if (!_balanceVisible) return _maskCents();
  return (Number(c) || 0).toFixed(1) + '¢';
}

function _displayTradePnl(dollars, pct) {
  const pctPart = ` (${(Number(pct) || 0).toFixed(1)}%)`;
  if (!_balanceVisible) return _maskUsd() + pctPart;
  const n = Number(dollars) || 0;
  const pnlPos = n >= 0;
  return `${pnlPos ? '+' : ''}$${Math.abs(n).toFixed(2)}${pctPart}`;
}

function _displayCumulativePnl(val) {
  if (!_balanceVisible) return _maskUsd();
  const n = Number(val) || 0;
  const cumPos = n >= 0;
  return `${cumPos ? '+' : ''}$${Math.abs(n).toFixed(2)}`;
}

function _displayYesNoPrice(val) {
  if (!_balanceVisible) return _maskCents();
  return (Number(val) || 0) + '¢';
}

function _shortMarket(name) {
  if (!name) return '—';
  return name.length > 48 ? name.slice(0, 46) + '…' : name;
}

function renderPositions(positions) {
  window._lastPositions = positions || [];
  _updatePositionsTabCount(window._lastPositions.length);
  const el = document.getElementById('positions-list');
  if (!el) return;
  if (window._lastPositions.length === 0) {
    el.innerHTML = '<div class="pm-empty">No open positions</div>';
    return;
  }
  el.innerHTML = window._lastPositions.map(p => {
    const roi = Number(p.roi_pct) || 0;
    const pnl = Number(p.unrealized_pnl) || 0;
    const roiCls = roi >= 0 ? 'pos' : 'neg';
    const pending = _cashoutPending.has(`${p.asset}:${p.window}`);
    const cashoutKey = `${p.asset}:${p.window}`;
    const disabled = pending || !p.cashout_available;
    const btnLabel = pending ? 'Selling…' : 'Sell';
    const sideCls = p.side === 'YES' ? 'pm-side-yes' : p.side === 'NO' ? 'pm-side-no' : 'pm-side-spread';
    const isSpread = p.side === 'SPREAD';
    const entryDetail = isSpread
      ? `<span>Y ${Number(p.yes_shares||0).toFixed(1)}@${_fmtCents(p.yes_avg_price_c)} avg · N ${Number(p.no_shares||0).toFixed(1)}@${_fmtCents(p.no_avg_price_c)} avg${p.pair_avg_price_c ? ' · pair '+_fmtCents(p.pair_avg_price_c)+' avg' : ''}</span>`
      : `<span class="${sideCls}">${p.side}</span><span>${_fmtCents(p.entry_price_cents)} → ${_fmtCents(p.current_price_cents)}</span>`;
    return `
      <div class="pm-row" data-asset="${p.asset}" data-window="${p.window || '5m'}">
        <div class="pm-row-main">
          <div class="pm-row-title">${_shortMarket(p.market)}</div>
          <div class="pm-row-sub">
            <span>${p.asset} ${p.window || '5m'}</span>
            ${entryDetail}
            <span>${Number(p.size).toFixed(2)} sh</span>
          </div>
        </div>
        <div class="pm-row-stats">
          <span class="pm-row-val ${roiCls}">${roi >= 0 ? '+' : ''}${roi.toFixed(1)}%</span>
          <span class="pm-row-meta ${roiCls}">${_fmtUsd(pnl, true)}</span>
        </div>
        ${isSpread ? '' : `<button class="pm-cashout-btn" ${disabled ? 'disabled' : ''}
                onclick="cashoutPosition('${p.asset}','${p.window || '5m'}')">${btnLabel}</button>`}
      </div>`;
  }).join('');
}

function _historyActionStyle(action) {
  const a = (action || 'buy').toLowerCase();
  if (a === 'redeem') return { label: 'redeem', cls: 'pm-action-redeem' };
  if (a === 'sell')   return { label: 'sell',   cls: 'pm-action-sell' };
  return { label: 'buy', cls: 'pm-action-buy' };
}

function renderTradeHistory(trades) {
  window._lastTradeHistory = trades || [];
  const el = document.getElementById('history-list');
  if (!el) return;
  if (!trades || trades.length === 0) {
    el.innerHTML = '<div class="pm-empty">No trade history yet</div>';
    return;
  }
  el.innerHTML = trades.map(t => {
    const { label, cls } = _historyActionStyle(t.action);
    const isSpreadTrade = (t.side || '') === 'SPREAD';
    const sideCls = isSpreadTrade ? 'pm-side-spread' : (t.side === 'YES' ? 'pm-side-yes' : 'pm-side-no');
    const ts = t.timestamp_ms
      ? new Date(t.timestamp_ms).toLocaleString('en-US', {month:'short',day:'numeric',hour:'numeric',minute:'2-digit',second:'2-digit',hour12:true})
      : (t.timestamp || '—');
    const detail = isSpreadTrade
      ? `<span class="${sideCls}">SPREAD</span><span>Y ${Number(t.yes_size||0).toFixed(1)}@${_fmtCents((Number(t.yes_price)||0)*100)} · N ${Number(t.no_size||0).toFixed(1)}@${_fmtCents((Number(t.no_price)||0)*100)}</span>`
      : `<span class="${sideCls}">${t.side || ''}</span><span>${_fmtCents((Number(t.price) || 0) * 100)}</span>`;
    const sizeLabel = isSpreadTrade
      ? `${Number(t.size||0).toFixed(2)} pair sh`
      : `${Number(t.size).toFixed(2)} sh`;
    return `
      <div class="pm-row">
        <div class="pm-row-main">
          <div class="pm-row-title">${_shortMarket(t.market)}</div>
          <div class="pm-row-sub">
            <span>${t.asset || ''}</span>
            <span class="${cls}">${label}</span>
            ${detail}
            <span>${sizeLabel}</span>
          </div>
        </div>
        <div class="pm-row-stats">
          <span class="pm-row-meta">${ts}</span>
        </div>
      </div>`;
  }).join('');
}

async function cashoutPosition(asset, window) {
  const key = `${asset}:${window || '5m'}`;
  if (_cashoutPending.has(key)) return;
  _cashoutPending.add(key);
  renderPositions(window._lastPositions || []);
  try {
    const res = await fetch('/api/positions/' + encodeURIComponent(asset.toLowerCase()) + '/'
      + encodeURIComponent((window || '5m').toLowerCase()) + '/cashout', {
      method: 'POST',
    });
    const data = await res.json();
    if (!res.ok || !data.ok) {
      alert(data.error || 'Cashout failed');
    }
  } catch (e) {
    alert('Cashout request failed');
    console.warn(e);
  } finally {
    _cashoutPending.delete(key);
    loadPositionsAndHistory();
  }
}

// ═══════════════════════════════════════════════════════════════════
// GLOBAL STATS
// ═══════════════════════════════════════════════════════════════════
function renderProfileStats(bots, g) {
  let positionsValue = 0;
  for (const bot of (bots || [])) {
    const ySh = Number(bot.yes_shares) || 0;
    const nSh = Number(bot.no_shares) || 0;
    positionsValue += ySh * (Number(bot.yes_avg_price_c) || 0) / 100;
    positionsValue += nSh * (Number(bot.no_avg_price_c) || 0) / 100;
  }
  const biggestWin = Number(g?.biggest_win) || 0;
  const predictions = Number(g?.total_trades) || 0;
  _profileStats = { positionsValue, biggestWin, predictions };

  const posEl = document.getElementById('ps-positions-value');
  const winEl = document.getElementById('ps-biggest-win');
  const predEl = document.getElementById('ps-predictions');

  if (posEl) {
    if (!_balanceVisible) {
      posEl.textContent = _maskUsd();
      posEl.className = 'ps-stat-val text-zinc-200';
    } else {
      const n = positionsValue;
      posEl.textContent = (n >= 0 ? '' : '-') + '$' + Math.abs(n).toFixed(2);
      posEl.className = 'ps-stat-val ' + (n > 0 ? 'text-emerald-400' : n < 0 ? 'text-red-400' : 'text-zinc-200');
    }
  }

  if (winEl) {
    if (!_balanceVisible) {
      winEl.textContent = _maskUsd();
      winEl.className = 'ps-stat-val text-zinc-200';
    } else if (biggestWin === 0) {
      winEl.textContent = '$0.00';
      winEl.className = 'ps-stat-val text-zinc-200';
    } else {
      winEl.textContent = '+$' + biggestWin.toFixed(2);
      winEl.className = 'ps-stat-val text-emerald-400';
    }
  }

  if (predEl) {
    predEl.textContent = String(predictions);
    predEl.className = 'ps-stat-val text-zinc-100';
  }
}

function renderGlobalStats(g) {
  const el = id => document.getElementById(id);
  if (el('pm-st-trades')) el('pm-st-trades').textContent = g.total_trades ?? 0;
  if (el('pm-st-wins'))   el('pm-st-wins').textContent   = g.total_wins   ?? 0;
  if (el('pm-st-losses')) el('pm-st-losses').textContent = g.total_losses ?? 0;
  if (el('pm-st-wr'))     el('pm-st-wr').textContent     = (g.win_rate??0).toFixed(1)+'%';

  // ── Trading schedule banner ──────────────────────────────────────
  // Reflects the server-side is_trading_allowed() result broadcast
  // through global_stats.trading_allowed on every WebSocket tick.
  // The banner updates automatically the moment the day changes —
  // no page refresh needed.
  const banner   = el('schedule-banner');
  const dot      = el('schedule-dot');
  const schedTxt = el('schedule-text');
  if (banner && dot && schedTxt) {
    const allowed  = g.trading_allowed ?? true;
    const tz       = g.trading_tz || 'UTC';
    const inCd     = (g.workers_in_cooldown ?? g.assets_in_cooldown ?? 0) > 0;
    if (allowed && !inCd) {
      banner.className   = banner.className.replace(
        /bg-\S+|text-\S+|border-\S+/g, '').trim() +
        ' bg-emerald-950 text-emerald-400 border border-emerald-900';
      dot.className      = 'w-2 h-2 rounded-full bg-emerald-400 inline-block flex-shrink-0';
      schedTxt.textContent =
        `✅ Trading ACTIVE — new entries permitted (${tz})`;
    } else if (!allowed) {
      banner.className   = banner.className.replace(
        /bg-\S+|text-\S+|border-\S+/g, '').trim() +
        ' bg-yellow-950 text-yellow-400 border border-yellow-900';
      dot.className      = 'w-2 h-2 rounded-full bg-yellow-400 inline-block flex-shrink-0';
      schedTxt.textContent =
        `🚫 Weekend — new entries BLOCKED (${tz}). TP/SL and portfolio tracking continue.`;
    } else {
      banner.className   = banner.className.replace(
        /bg-\S+|text-\S+|border-\S+/g, '').trim() +
        ' bg-orange-950 text-orange-400 border border-orange-900';
      dot.className      = 'w-2 h-2 rounded-full bg-orange-400 inline-block flex-shrink-0';
      schedTxt.textContent =
        `🛡️ ${g.workers_in_cooldown ?? g.assets_in_cooldown} worker(s) in COOLDOWN — new entries blocked. TP/SL continues.`;
    }
  }
}

// ═══════════════════════════════════════════════════════════════════
// SIGNAL HELPERS
// ═══════════════════════════════════════════════════════════════════
function sigClass(s){
  if(!s)return'sig-neutral';const u=s.toUpperCase();
  if(u.includes('STALE'))return'sig-neutral';
  if(u.includes('BULL'))return'sig-strong-bull';
  if(u.includes('BEAR'))return'sig-strong-bear';
  if(u.includes('STRONGLY')&&u.includes('BULL'))return'sig-strong-bull';
  if(u.includes('STRONGLY')&&u.includes('BEAR'))return'sig-strong-bear';
  if(u.includes('MILDLY')&&u.includes('BULL'))return'sig-mild-bull';
  if(u.includes('MILDLY')&&u.includes('BEAR'))return'sig-mild-bear';
  return'sig-neutral';
}
function pillClass(s){
  if(!s)return'pill-neutral';const u=s.toUpperCase();
  if(u.includes('STALE'))return'pill-neutral';
  if(u.includes('BULL'))return'pill-strong-bull';
  if(u.includes('BEAR'))return'pill-strong-bear';
  if(u.includes('STRONGLY')&&u.includes('BULL'))return'pill-strong-bull';
  if(u.includes('STRONGLY')&&u.includes('BEAR'))return'pill-strong-bear';
  if(u.includes('MILDLY')&&u.includes('BULL'))return'pill-mild-bull';
  if(u.includes('MILDLY')&&u.includes('BEAR'))return'pill-mild-bear';
  return'pill-neutral';
}
function sigIcon(s){
  if(!s)return'—';const u=s.toUpperCase();
  if(u.includes('STALE'))return'⚠️';
  if(u.includes('BULL'))return'🚀';
  if(u.includes('BEAR'))return'🔻';
  if(u.includes('STRONGLY')&&u.includes('BULL'))return'🚀';
  if(u.includes('STRONGLY')&&u.includes('BEAR'))return'🔻';
  if(u.includes('MILDLY')&&u.includes('BULL'))return'📈';
  if(u.includes('MILDLY')&&u.includes('BEAR'))return'📉';
  return'➖';
}
function formatMarketWindow(s,e){
  if(!s||!e)return'Waiting for market…';
  try{
    const o={timeZone:'America/New_York'};
    const sd=new Date(s).toLocaleString('en-US',{...o,month:'short',day:'numeric'});
    const st=new Date(s).toLocaleString('en-US',{...o,hour:'numeric',minute:'2-digit',hour12:true});
    const et=new Date(e).toLocaleString('en-US',{...o,hour:'numeric',minute:'2-digit',hour12:true});
    return`${sd}, ${st} – ${et} ET`;
  }catch(err){return'Waiting for market…';}
}

// ═══════════════════════════════════════════════════════════════════
// BOT CARDS
// ═══════════════════════════════════════════════════════════════════
function renderBots(bots){
  window._lastBots = bots || [];
  const c=document.getElementById('bots-container');
  if(!c) return;
  window._lastBots.forEach((bot,i)=>{
    let card=document.getElementById(`bot-card-${i}`);
    if(!card){
      card=document.createElement('div');
      card.id=`bot-card-${i}`;
      card.className='card bg-zinc-900 rounded-3xl p-5 md:p-6';
      c.appendChild(card);
    }
    card.innerHTML=renderCard(bot);
  });
  // Remove stale cards when asset count decreases (e.g. config change).
  let i=window._lastBots.length;
  while(document.getElementById(`bot-card-${i}`)){
    document.getElementById(`bot-card-${i}`).remove();
    i++;
  }
}
function _fmtCooldownRemaining(sec) {
  const s = Math.max(0, Number(sec) || 0);
  const m = Math.floor(s / 60);
  const r = s % 60;
  return `${m}m ${String(r).padStart(2, '0')}s`;
}

function _fmtPositionRow(bot) {
  const ySh = Number(bot.yes_shares) || 0;
  const nSh = Number(bot.no_shares) || 0;
  if (ySh <= 0 && nSh <= 0) return '-';
  const parts = [];
  let yesCost = 0;
  let noCost = 0;
  if (ySh > 0) {
    yesCost = ySh * (Number(bot.yes_avg_price_c) || 0) / 100;
    parts.push(`YES @ $${yesCost.toFixed(2)}`);
  }
  if (nSh > 0) {
    noCost = nSh * (Number(bot.no_avg_price_c) || 0) / 100;
    parts.push(`NO @ $${noCost.toFixed(2)}`);
  }
  let text = parts.join(' ');
  if (ySh > 0 && nSh > 0) text += ` (pair=$${(yesCost + noCost).toFixed(1)})`;
  return text;
}

function renderCard(bot){
  const hasInv=(bot.yes_shares||0)>0||(bot.no_shares||0)>0;
  const positionRow = _fmtPositionRow(bot);
  const hasPos = positionRow !== '-' || hasInv;
  const pnlPos=(bot.pnl_dollars||0)>=0;
  const cumPos=(bot.cumulative_pnl||0)>=0;
  const inProfit=hasInv&&(bot.pnl_dollars||0)>0;
  const inLoss=hasInv&&(bot.pnl_dollars||0)<0;
  const inCooldown=!!bot.cooldown_active;
  const cdPnl=Number(bot.cooldown_window_pnl)||0;
  const cdPnlPos=cdPnl>=0;
  const border=inProfit?'border-l-4 border-emerald-500':inLoss?'border-l-4 border-red-500':inCooldown?'border-l-4 border-orange-500':hasPos?'border-l-4 border-sky-500':'';
  const edgeCents=(bot.spread_edge_cents!=null)?bot.spread_edge_cents:((bot.spread_edge||0)*100);
  const thrCents=(bot.spread_threshold_cents!=null)?bot.spread_threshold_cents:((bot.spread_threshold||0.03)*100);
  const edgeOk=!!bot.edge_above_threshold;
  const signal=edgeOk?`EDGE ${edgeCents.toFixed(2)}c`:'NO EDGE';
  const yesAvgC=(bot.yes_shares||0)>0?(bot.yes_avg_price_c||0):null;
  const noAvgC=(bot.no_shares||0)>0?(bot.no_avg_price_c||0):null;
  const yesShLabel=(bot.yes_shares||0)>0
    ?`YES ${(bot.yes_shares||0).toFixed(1)} sh avg @ ${yesAvgC.toFixed(1)}c`
    :`YES 0 / ${(bot.max_shares||'?')}`;
  const noShLabel=(bot.no_shares||0)>0
    ?`NO ${(bot.no_shares||0).toFixed(1)} sh avg @ ${noAvgC.toFixed(1)}c`
    :`NO 0 / ${(bot.max_shares||'?')}`;
  const pairAvgC=(bot.pair_avg_price_c||0)>0?bot.pair_avg_price_c:null;
  const wins=bot.wins??0;const losses=bot.losses??0;
  const trades=bot.trade_count??0;const wr=bot.win_rate??0;
  const mw=formatMarketWindow(bot.market_start_iso,bot.market_end_iso);
  const label=`${bot.asset||'BOT'}`;
  const badge=inProfit
    ?`<span class="text-xs bg-emerald-950 text-emerald-400 px-2 py-0.5 rounded-full font-semibold">🟢 IN PROFIT</span>`
    :inLoss
      ?`<span class="text-xs bg-red-950 text-red-400 px-2 py-0.5 rounded-full font-semibold">🔴 IN LOSS</span>`
      :inCooldown
        ?`<span class="text-xs bg-orange-950 text-orange-400 px-2 py-0.5 rounded-full font-semibold">🛡️ COOLDOWN</span>`
      :hasPos
        ?`<span class="text-xs bg-sky-900 text-sky-300 px-2 py-0.5 rounded-full">⚡ IN POSITION</span>`
        :`<span class="text-xs bg-zinc-800 text-zinc-400 px-2 py-0.5 rounded-full">WAITING</span>`;
  const cooldownBlock=inCooldown?`
      <div class="bg-orange-950/40 border border-orange-900/60 rounded-xl px-3 py-2 mb-3 text-xs text-orange-300">
        <div class="font-semibold mb-0.5">New entries blocked — cooldown active</div>
        <div class="font-mono text-orange-400/90">Until ${bot.cooldown_until_utc||'?'} UTC · ${_fmtCooldownRemaining(bot.cooldown_remaining_sec)} left</div>
      </div>`:'';
  return`
    <div class="${border} rounded-2xl pl-3">
      <div class="flex items-center justify-between mb-1">
        <div class="flex items-center gap-2 flex-wrap">
          <span class="text-xl font-black tracking-tight" style="font-family:'Inter',system-ui,sans-serif;letter-spacing:-.02em;">${label}</span>
          ${badge}
        </div>
        <span class="text-zinc-500 text-xs font-mono">${bot.timer||'--:--'}</span>
      </div>
      <div class="text-zinc-500 text-xs mb-3 font-mono">${mw}</div>
      ${cooldownBlock}
      <div class="flex gap-3 mb-3">
        <div class="flex-1 bg-zinc-800 rounded-xl p-2 text-center">
          <div class="text-zinc-400 text-xs mb-0.5">YES</div>
          <div class="text-lg font-bold text-emerald-400 font-mono" style="font-variant-numeric:tabular-nums;">${_displayYesNoPrice(bot.yes||0)}</div>
        </div>
        <div class="flex-1 bg-zinc-800 rounded-xl p-2 text-center">
          <div class="text-zinc-400 text-xs mb-0.5">NO</div>
          <div class="text-lg font-bold text-red-400 font-mono" style="font-variant-numeric:tabular-nums;">${_displayYesNoPrice(bot.no||0)}</div>
        </div>
      </div>
      <div class="bg-zinc-800 rounded-xl px-3 py-2 mb-3 flex items-center justify-between gap-2 flex-wrap">
        <span class="inline-flex items-center gap-1.5 text-sm font-semibold px-2.5 py-1 rounded-full ${pillClass(signal)}">
          ${sigIcon(signal)} ${signal}
        </span>
        <div class="text-xs text-zinc-400 font-mono">
          live edge <span class="${edgeOk?'text-emerald-400':'text-zinc-300'}">${edgeCents.toFixed(2)}c</span>
          &nbsp;· need <span class="text-cyan-400">&gt;${thrCents.toFixed(1)}c</span>
        </div>
      </div>
      <div class="bg-zinc-800/60 rounded-xl px-3 py-2 mb-3 text-xs font-mono text-zinc-300 grid grid-cols-2 gap-x-3 gap-y-1">
        <span>bids: ${bot.combined_bid_c||0}c</span>
        <span>caps: ${bot.spread_captures||0}</span>
        <span>${yesShLabel}</span>
        <span>${noShLabel}</span>
        ${pairAvgC!=null?`<span class="col-span-2">pair avg: ${pairAvgC.toFixed(1)}c</span>`:''}
      </div>
      <div class="space-y-1.5 text-sm mb-3">
        <div class="flex items-center justify-between">
          <span class="text-zinc-400">Position</span>
          <span class="font-mono text-zinc-200">${positionRow}</span>
        </div>
        <div class="flex items-center justify-between">
          <span class="text-zinc-400">Trade PnL</span>
          <span class="font-mono ${pnlPos?'text-emerald-400':'text-red-400'}" style="font-variant-numeric:tabular-nums;">
            ${_displayTradePnl(bot.pnl_dollars, bot.pnl_pct)}
          </span>
        </div>
        <div class="flex items-center justify-between">
          <span class="text-zinc-400">Cumulative PnL</span>
          <span class="font-mono font-semibold ${cumPos?'text-emerald-300':'text-red-300'}" style="font-variant-numeric:tabular-nums;">
            ${_displayCumulativePnl(bot.cumulative_pnl)}
          </span>
        </div>
        <div class="flex items-center justify-between">
          <span class="text-zinc-400">Cooldown window PnL</span>
          <span class="font-mono text-xs ${cdPnlPos?'text-emerald-400':'text-orange-400'}" style="font-variant-numeric:tabular-nums;" title="Resets after cooldown · limit -$${bot.cooldown_max_loss??''}">
            ${_displayCumulativePnl(cdPnl)}
          </span>
        </div>
        <div class="flex items-center justify-between">
          <span class="text-zinc-400">Outcome</span>
          <span class="font-mono ${bot.outcome==='YES'?'text-emerald-400':bot.outcome==='NO'?'text-red-400':'text-zinc-500'}">
            ${bot.outcome||'PENDING'}
          </span>
        </div>
      </div>
      <div class="bg-zinc-800/60 rounded-xl px-3 py-2 mb-3 flex items-center justify-between text-xs">
        <div class="flex flex-col items-center"><span class="font-bold text-zinc-200 font-mono">${trades}</span><span class="text-zinc-500 uppercase" style="font-size:9px;letter-spacing:.08em">Trades</span></div>
        <div class="flex flex-col items-center"><span class="font-bold text-emerald-400 font-mono">${wins}</span><span class="text-zinc-500 uppercase" style="font-size:9px;letter-spacing:.08em">Wins</span></div>
        <div class="flex flex-col items-center"><span class="font-bold text-red-400 font-mono">${losses}</span><span class="text-zinc-500 uppercase" style="font-size:9px;letter-spacing:.08em">Losses</span></div>
        <div class="flex flex-col items-center"><span class="font-bold text-yellow-400 font-mono">${wr.toFixed(1)}%</span><span class="text-zinc-500 uppercase" style="font-size:9px;letter-spacing:.08em">Win Rate</span></div>
      </div>
      <div class="pt-2 border-t border-zinc-800 flex items-center justify-between text-xs text-zinc-500">
        <span>Listener in</span>
        <span class="font-mono">${bot.listener||'--:--'}</span>
      </div>
    </div>`;
}

// Boot: init crosshair, then load history immediately from the backend.
// The backend will return backfilled historical data so the chart shows
// past performance right away — not an empty chart from today.
_loadBalancePref();
_updateBalanceToggleUI();
_initChartOverlay();
loadHistory(_pnlPeriod);
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def index():
    content = HTML_CONTENT.replace("__BOT_NAME__", BOT_DISPLAY_NAME)
    subtitle = f"{TRADING_ASSETS_SUBTITLE} · {MARKET_INTERVAL_SLUG} markets"
    return content.replace("__TRADING_ASSETS__", subtitle)


@app.get("/api/config")
async def api_config():
    return JSONResponse(_app_config_payload())


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.append(websocket)
    try:
        while True:
            await asyncio.sleep(30)
    except WebSocketDisconnect:
        if websocket in active_connections:
            active_connections.remove(websocket)


@app.get("/health")
async def health():
    """Lightweight probe for Render / load balancers."""
    return {"ok": True, "bots": len(bots)}


@app.get("/api/status")
async def api_status():
    return {
        "bots":          [bot.get_dashboard_data() for bot in bots] if bots else [],
        "global_stats":  get_global_stats(),
        "positions":     collect_open_positions(bots) if bots else [],
        "trade_history": get_trade_history(10),
        "config":        _app_config_payload(),
    }


@app.get("/api/positions")
async def api_positions():
    """Live open positions — one row per FILLED worker position."""
    return JSONResponse({"positions": collect_open_positions(bots) if bots else []})


@app.get("/api/trades/history")
async def api_trades_history(limit: int = 10):
    """Most recent executed BUY/SELL records, newest first."""
    lim = max(1, min(limit, 50))
    return JSONResponse({"trades": get_trade_history(lim)})


@app.post("/api/positions/{asset}/{window}/cashout")
async def api_cashout(asset: str, window: str):
    raise HTTPException(
        status_code=410,
        detail="Manual cashout removed — spread capture settles at market expiry",
    )


@app.post("/api/positions/{asset}/cashout")
async def api_cashout_legacy(asset: str):
    raise HTTPException(
        status_code=410,
        detail="Manual cashout removed — spread capture settles at market expiry",
    )


if __name__ == "__main__":
    _port = int(os.getenv("PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=_port, log_level="info")