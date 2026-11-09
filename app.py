# -*- coding: utf-8 -*-
"""
MT5 Hub - zero config.

    pip install MetaTrader5 requests psutil "requests[socks]"
    python app.py

Nothing to edit: running MT5 terminals are discovered automatically and watched
one after another (round-robin).  Telegram token, proxy, chats, alerts, daily
report... are all set in the web panel that opens in your browser.
WATCH-ONLY: no trading controls (only one terminal can be attached at a time).
"""

import collections
import copy
import hashlib
import hmac
import html
import json
import logging
import os
import re
import secrets
import sqlite3
import subprocess
import sys
import threading
import time
import webbrowser
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs, quote

import requests

import analytics

BASE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(BASE, "data")
os.makedirs(DATA, exist_ok=True)
ORIG_ARGV = list(sys.argv)


def cli(name, default=None):
    if name in ORIG_ARGV:
        i = ORIG_ARGV.index(name)
        if i + 1 < len(ORIG_ARGV):
            return ORIG_ARGV[i + 1]
    return default


# The engine (mt5_monitor.py) wants a config file at import time: give it an empty one.
BOOT = os.path.join(DATA, "bootstrap.json")
with open(BOOT, "w", encoding="utf-8") as f:
    json.dump({"telegram": {"bot_token": "", "chat_id": ""}}, f)
os.environ["MT5_LOG_NAME"] = os.path.join(DATA, "monitor")
sys.argv = [sys.argv[0], "--config", BOOT]

import MetaTrader5 as mt5          # noqa: E402
import mt5_monitor as core         # noqa: E402

logger = core.logger
CUR = [None]


# =========================================================
# SETTINGS (everything lives in data/settings.json, edited from the panel)
# =========================================================

DEFAULT_FORMATS = {
    "startup": "🟢 <b>MT5 HUB STARTED</b> — {count} account(s)\n\n{accounts}\n\nTotal equity: {total_equity}",
    "startup_account": "<b>[{label}]</b> #{login} · {server}\nBalance {balance} · Equity {equity} {currency} · Open {open}\n{positions}",
    "heartbeat": "💓 <b>Monitor alive</b> · {time}\n\n{accounts}\n\nTotal equity {total_equity} · Floating {total_floating} · Today {total_today}",
    "heartbeat_account": "<b>[{label}]</b> {status} Equity {equity} · Floating {floating} · Today {today} · Open {open}",
    "daily_full": "📊 <b>DAILY REPORT — {date}</b>\n\n{accounts}\n\n<b>TOTAL</b>\nNet: <b>{total_net}</b> · Trades: {total_trades} (✅ {total_wins} / ❌ {total_losses}) · Win rate {total_winrate}%\nBalance {total_balance} · Equity {total_equity} · Floating {total_floating}\n\n<b>By symbol</b>\n{by_symbol}\n\n<b>By EA</b>\n{by_ea}",
    "daily_full_account": "<b>[{label}]</b> {status}\nNet {net} · {trades} trades (✅ {wins} / ❌ {losses}) · WR {winrate}% · PF {pf}\nBalance {balance} · Equity {equity} · Floating {floating} · Open {open}",
    "daily_public": "📊 <b>DAILY SIGNALS RECAP — {date}</b>\n\n{icon} Net result: <b>{points} points</b>\nSignals closed: {trades} (✅ {wins} / ❌ {losses})\nWin rate: {winrate}%\nBest: {best_points} ({best_symbol}) · Worst: {worst_points} ({worst_symbol})\n\n<b>By symbol</b>\n{by_symbol}\n\n<i>Past results do not guarantee future performance.</i>",
}

DEFAULTS = {
    "telegram": {
        "bots": [],
        "proxy": {"enabled": False, "type": "socks5", "host": "127.0.0.1", "port": 10808,
                  "username": "", "password": ""},
    },
    "destinations": [],
    "rr": {"dwell_seconds": 6, "discover_seconds": 20},
    "terminals": {"manual": [], "ignored": []},
    "accounts": {},
    "monitor": {
        "notify_startup": True, "notify_connection": True, "notify_opened": True,
        "notify_closed": True, "notify_position_updates": True, "notify_pending_orders": True,
        "notify_balance_changes": True, "notify_existing_positions_on_start": True,
        "sl_tp_notify_min_interval_sec": 60, "heartbeat_hours": 0,
        "max_catchup_hours": 12, "signal_max_delay_sec": 120,
    },
    "alerts": {
        "broker_connection": True, "autotrading": True, "margin_level_warn": 150,
        "margin_level_critical": 100, "daily_loss_percent": 3, "daily_loss_amount": 0,
        "daily_profit_target": 0, "floating_loss_percent": 5, "max_open_positions": 0,
        "warn_no_sl": True, "no_sl_grace_seconds": 90, "position_loss_amount": 0,
    },
    "daily_report": {"enabled": True, "time": "23:55", "day_boundary": "local",
                     "send_if_no_trades": False},
    "web": {"lan": False, "salt": "", "password_hash": ""},
    "report_format": dict(DEFAULT_FORMATS),
    "analytics": {"enabled": True, "days": 180, "min_trades": 8},
    "runtime": {"last_report_date": "", "last_heartbeat": 0.0},
}
SECRETS = {"telegram.proxy.password"}
PATCHABLE = {"telegram", "destinations", "rr", "monitor", "alerts", "daily_report", "report_format", "analytics"}
SETTINGS_FILE = os.path.join(DATA, "settings.json")
SLOCK = threading.RLock()


def merge_known(dst, src):
    for k, v in src.items():
        if k not in dst:
            continue
        if isinstance(dst[k], dict) and isinstance(v, dict) and k != "accounts":
            merge_known(dst[k], v)
        else:
            dst[k] = v


def load_settings():
    s = copy.deepcopy(DEFAULTS)
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            old_tg = loaded.get("telegram", {})
            if old_tg.get("bot_token") and not old_tg.get("bots"):     # older single-bot settings
                old_tg["bots"] = [{"id": "main", "name": "main", "token": old_tg["bot_token"], "proxy": True}]
            merge_known(s, loaded)
            for d in s["destinations"]:
                d.setdefault("bot", "")
        except Exception:
            logger.exception("settings.json is corrupt - starting from defaults")
            os.replace(SETTINGS_FILE, SETTINGS_FILE + ".corrupt")
    return s


S = load_settings()


def save_settings():
    with SLOCK:
        tmp = SETTINGS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(S, f, ensure_ascii=False, indent=2)
        os.replace(tmp, SETTINGS_FILE)


def coerce(default, v):
    if isinstance(default, bool):
        return bool(v)
    if isinstance(default, int):
        return int(float(v))
    if isinstance(default, float):
        return float(v)
    if isinstance(default, str):
        return str(v)
    return v


def patch_into(dst, patch, path=""):
    for k, v in patch.items():
        if k not in dst:
            continue
        cur = dst[k]
        full = path + k
        if isinstance(cur, dict) and isinstance(v, dict):
            patch_into(cur, v, full + ".")
        elif full in SECRETS and v == "":
            continue
        elif full in SECRETS and v == "__clear__":
            dst[k] = ""
        elif isinstance(cur, list):
            dst[k] = v if isinstance(v, list) else cur
        else:
            dst[k] = coerce(cur, v)


def clean_destinations(items):
    out, names = [], set()
    for d in items:
        name = str(d.get("name", "")).strip()[:30] or f"dest{len(out) + 1}"
        while name in names:
            name += "_"
        names.add(name)
        mode = d.get("mode") if d.get("mode") in ("full", "signal") else "full"
        f = d.get("filters") or {}
        bot = str(d.get("bot", ""))
        out.append({
            "name": name,
            "bot": bot if bot in [b["id"] for b in S["telegram"]["bots"]] else "",
            "chat_id": str(d.get("chat_id", "")).strip(),
            "mode": mode,
            "enabled": bool(d.get("enabled", True)),
            "events": [str(e) for e in d.get("events", ["*"])] or ["*"],
            "report": d.get("report") if d.get("report") in ("full", "public", "none")
            else ("full" if mode == "full" else "public"),
            "filters": {
                "magics": [int(m) for m in f.get("magics", []) if str(m).strip().lstrip("-").isdigit()],
                "symbols": [str(x).strip() for x in f.get("symbols", []) if str(x).strip()],
                "only_ea": bool(f.get("only_ea", False)),
            },
        })
    return out


def settings_view():
    with SLOCK:
        v = copy.deepcopy(S)
    secrets_set = {"telegram.proxy.password": bool(v["telegram"]["proxy"]["password"])}
    v["telegram"]["bots"] = [
        {"id": b["id"], "name": b["name"], "proxy": b.get("proxy", True),
         "token_set": True, "hint": "…" + b["token"][-4:]}
        for b in v["telegram"]["bots"]
    ]
    v["telegram"]["proxy"]["password"] = ""
    v["web"] = {"lan": v["web"]["lan"], "password_set": bool(S["web"]["password_hash"])}
    return {"settings": v, "secrets_set": secrets_set, "defaults": {"report_format": DEFAULT_FORMATS}}


# =========================================================
# DATABASE (event feed + equity history)
# =========================================================

DB = sqlite3.connect(os.path.join(DATA, "app.db"), check_same_thread=False)
DB_LOCK = threading.Lock()
with DB_LOCK:
    DB.executescript(
        """
        CREATE TABLE IF NOT EXISTS snap(ts INTEGER, acc TEXT, balance REAL, equity REAL);
        CREATE INDEX IF NOT EXISTS snap_i ON snap(acc, ts);
        CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, acc TEXT, kind TEXT, text TEXT);
        CREATE TABLE IF NOT EXISTS trades(acc TEXT, pid INTEGER, symbol TEXT, side TEXT, volume REAL, open_ts INTEGER,
            close_ts INTEGER, open_price REAL, close_price REAL, gross REAL, costs REAL, points REAL, magic INTEGER,
            comment TEXT, PRIMARY KEY(acc, pid));
        CREATE INDEX IF NOT EXISTS trades_t ON trades(acc, close_ts);
        """
    )
    DB.commit()


def db(sql, args=(), fetch=False):
    with DB_LOCK:
        cur = DB.execute(sql, args)
        rows = cur.fetchall() if fetch else None
        DB.commit()
        return rows


def acc_label(key):
    return (S["accounts"].get(key, {}).get("label")) or key or "?"


class Shim(object):
    """Replaces panel.py inside the engine: stores every event in the feed."""

    @staticmethod
    def log_event(kind, text):
        ctx = CUR[0]
        if ctx is None:
            return
        plain = html.unescape(re.sub(r"<[^>]+>", "", text)).strip()
        db("INSERT INTO events(ts,acc,kind,text) VALUES(?,?,?,?)",
           (int(time.time()), acc_label(ctx["key"]), kind, plain[:700]))

    @staticmethod
    def tick():
        pass


core.PANEL = Shim


class PrefixFilter(logging.Filter):
    def filter(self, record):
        ctx = CUR[0]
        if ctx is not None and ctx.get("key") and not getattr(record, "_pfx", False):
            record.msg = f"[{acc_label(ctx['key'])}] {record.msg}"
            record._pfx = True
        return True


for _h in logging.getLogger().handlers:
    _h.addFilter(PrefixFilter())


# =========================================================
# TELEGRAM HELPERS (used by the panel itself: test / detect chats)
# =========================================================

def build_proxies(p):
    if not p.get("enabled"):
        return None
    scheme = {"socks5": "socks5h", "socks5h": "socks5h", "http": "http", "https": "https"}.get(
        str(p.get("type", "socks5")).lower(), "socks5h")
    cred = f"{quote(p['username'], safe='')}:{quote(p['password'], safe='')}@" \
        if p.get("username") and p.get("password") else ""
    url = f"{scheme}://{cred}{p.get('host', '127.0.0.1')}:{p.get('port', 10808)}"
    return {"http": url, "https": url}


def find_bot(bot_id):
    bots = S["telegram"]["bots"]
    for b in bots:
        if b["id"] == bot_id:
            return b
    return bots[0] if (bots and not bot_id) else None


def tg_call(method, payload=None, timeout=15, bot_id=None):
    with SLOCK:
        bot = find_bot(bot_id)
        token = bot["token"] if bot else ""
        proxies = build_proxies(S["telegram"]["proxy"]) if (bot and bot.get("proxy", True)) else None
    if not token:
        return {"ok": False, "description": "Unknown bot - save your settings first" if bot_id else "No bot token set"}
    try:
        r = requests.post(f"https://api.telegram.org/bot{token}/{method}",
                          json=payload or {}, proxies=proxies, timeout=timeout)
        return r.json()
    except Exception as e:
        return {"ok": False, "description": str(e)[:300]}


def tg_detect(bot_id=None):
    res = tg_call("getUpdates", {"timeout": 0, "limit": 100,
                                 "allowed_updates": ["message", "channel_post", "my_chat_member"]},
                  bot_id=bot_id)
    if not res.get("ok"):
        return res
    chats = {}
    for u in res.get("result", []):
        for part in ("message", "channel_post", "my_chat_member"):
            c = (u.get(part) or {}).get("chat")
            if c:
                chats[c["id"]] = {
                    "id": c["id"], "type": c.get("type"),
                    "name": c.get("title") or " ".join(filter(None, [c.get("first_name"), c.get("last_name")])) or c.get("username", ""),
                }
    return {"ok": True, "chats": list(chats.values())}


# =========================================================
# TERMINAL DISCOVERY
# =========================================================

def running_terminals():
    found = {}
    try:
        import psutil
        for p in psutil.process_iter(["name", "exe"]):
            n = (p.info.get("name") or "").lower()
            if n in ("terminal64.exe", "terminal.exe") and p.info.get("exe"):
                found[os.path.normpath(p.info["exe"]).lower()] = os.path.normpath(p.info["exe"])
        return found, True
    except ImportError:
        pass
    try:
        out = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             "Get-Process terminal64,terminal -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Path"],
            capture_output=True, text=True, timeout=15,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        ).stdout
        for line in out.splitlines():
            if line.strip():
                found[os.path.normpath(line.strip()).lower()] = os.path.normpath(line.strip())
    except Exception:
        pass
    return found, False


CTX = collections.OrderedDict()   # lower(path) -> terminal context
CT_LOCK = threading.RLock()
INFO = {"psutil": True, "last_discover": 0.0, "round_sec": 0.0}


def new_ctx(path):
    return {"path": path, "running": True, "manual": False, "ok": False, "err": "not visited yet",
            "key": None, "state": None, "snap": None, "first": True, "fail": 0,
            "last_ok": 0.0, "last_equity_ts": 0.0, "report_req": False,
            "rt": {"next_send_ok": 0.0, "offset_check": 0.0}}


def discover():
    found, have_psutil = running_terminals()
    INFO["psutil"] = have_psutil
    with SLOCK:
        manual = {os.path.normpath(p).lower(): os.path.normpath(p) for p in S["terminals"]["manual"]}
        ignored = {os.path.normpath(p).lower() for p in S["terminals"]["ignored"]}
    targets = {}
    targets.update(found)
    targets.update(manual)
    with CT_LOCK:
        for low, path in targets.items():
            if low in ignored:
                continue
            ctx = CTX.get(low)
            if ctx is None:
                ctx = CTX[low] = new_ctx(path)
                logger.info("Terminal discovered: %s", path)
            ctx["running"] = True
            ctx["manual"] = low in manual
        for low, ctx in CTX.items():
            if low not in targets or low in ignored:
                if ctx["running"]:
                    logger.info("Terminal gone: %s", ctx["path"])
                ctx["running"] = False
                ctx["ok"] = False
                ctx["err"] = "ignored" if low in ignored else "terminal is not running"
    INFO["last_discover"] = time.time()


# =========================================================
# CONTEXT SWITCHING (the engine uses module-level globals)
# =========================================================

def state_file(key):
    return os.path.join(DATA, f"state_{key}.json")


def load_state_for(key):
    old = core.STATE_FILE
    core.STATE_FILE = state_file(key)
    try:
        return core.load_state()
    finally:
        core.STATE_FILE = old


def build_globals(ctx):
    with SLOCK:
        tg = copy.deepcopy(S["telegram"])
        acc = copy.deepcopy(S["accounts"].get(ctx["key"], {}))
        default_token = tg["bots"][0]["token"] if tg["bots"] else ""
        conf = {
            "telegram": {"bot_token": default_token, "chat_id": "", "proxy": tg["proxy"]},
            "bots": {b["id"]: {"token": b["token"], "proxy": b.get("proxy", True)} for b in tg["bots"]},
            "account_label": acc.get("label") or ctx["key"],
            "mt5": {"terminal_path": ctx["path"], "poll_interval": 2, "history_lookback_days": 7},
            "monitor": dict(copy.deepcopy(S["monitor"]), notify_startup=False, heartbeat_hours=0),
            "alerts": copy.deepcopy(S["alerts"]),
            "daily_report": dict(copy.deepcopy(S["daily_report"]), enabled=False),
            "destinations": [] if acc.get("muted") else copy.deepcopy(S["destinations"]),
            "ea_names": dict(acc.get("ea_names", {})),
        }
    report = conf["daily_report"]
    g = {
        "CONFIG": conf, "TOKEN": default_token, "CHAT_ID": "",
        "ACCOUNT_LABEL": conf["account_label"], "POLL_INTERVAL": 2, "LOOKBACK_DAYS": 7,
        "TERMINAL_PATH": ctx["path"], "ALERTS": conf["alerts"], "REPORT": report,
        "COMMANDS": {"enabled": False},
        "DAY_MODE": str(report.get("day_boundary", "local")).lower(),
        "TELEGRAM_CONFIG": conf["telegram"], "PROXY_CONFIG": tg["proxy"],
        "API": f"https://api.telegram.org/bot{default_token}",
        "INSTANCE": ctx["key"], "STATE_FILE": state_file(ctx["key"]), "STATE": ctx["state"],
    }
    old = core.PROXY_CONFIG
    core.PROXY_CONFIG = tg["proxy"]
    try:
        g["PROXIES"] = core.build_proxy()
    except Exception:
        g["PROXIES"] = None
    core.PROXY_CONFIG = old
    return g


def apply(ctx):
    for k, v in build_globals(ctx).items():
        setattr(core, k, v)
    core._digits_cache.clear()
    core._next_send_ok = ctx["rt"]["next_send_ok"]
    core._last_offset_check = ctx["rt"]["offset_check"]


def release(ctx):
    ctx["rt"]["next_send_ok"] = core._next_send_ok
    ctx["rt"]["offset_check"] = core._last_offset_check


def bind_identity(ctx, a):
    key = re.sub(r"[^A-Za-z0-9_.-]", "_", f"{a.login}_{a.server}")
    if ctx["key"] == key:
        return

    if ctx["state"] is not None:       # someone logged into another account in this terminal
        apply(ctx)
        core.save_state()
        release(ctx)
        logger.info("Terminal switched account: %s -> %s", ctx["key"], key)

    ctx["key"] = key
    ctx["state"] = load_state_for(key)
    ctx["first"] = True

    with SLOCK:
        acc = S["accounts"].setdefault(key, {
            "label": (a.name or "").strip()[:24] or f"#{a.login}",
            "muted": False, "ea_names": {}, "magics": []})
        acc.update(login=a.login, server=a.server, name=a.name, path=ctx["path"],
                   company=getattr(a, "company", ""))
        save_settings()


# =========================================================
# ONE VISIT
# =========================================================

def attach(ctx):
    mt5.shutdown()
    ok = False
    for _ in range(3):
        if mt5.initialize(path=ctx["path"], timeout=30000) and mt5.account_info() is not None:
            ok = True
            break
        mt5.shutdown()
        time.sleep(1.5)

    if not ok:
        ctx["err"] = "terminal is running but not reachable (is an account logged in?)"
        return False

    for _ in range(10):
        t = mt5.terminal_info()
        if t is not None and t.connected:
            break
        time.sleep(0.5)
    return True


def snapshot(ctx):
    a, t = mt5.account_info(), mt5.terminal_info()
    if not a or not t:
        return None

    stats = core.compute_day_stats() or {}
    if stats:
        ctx["day_stats"] = {"t": time.time(), "stats": stats,
                            "trades": [dict(t, ea=core.ea_name(t["magic"])) for t in stats.get("trades", [])]}
    A = core.RuntimeAlerts()
    day = core.STATE.get("day") or {}
    base = day.get("balance") or a.balance
    pnl = a.equity - base
    live = mt5.positions_get() or ()

    positions = []
    for x in live:
        info = mt5.symbol_info(x.symbol)
        point = info.point if info and info.point else 0
        sign = 1 if x.type == mt5.POSITION_TYPE_BUY else -1
        positions.append({
            "ticket": x.ticket, "symbol": x.symbol, "type": core.pos_direction(x.type),
            "volume": x.volume, "open": x.price_open, "price": x.price_current,
            "sl": x.sl, "tp": x.tp, "profit": x.profit + x.swap, "magic": x.magic,
            "ea": core.ea_name(x.magic), "digits": info.digits if info else 5,
            "age": max(0, time.time() - core.to_utc_ts(x.time)),
        })

    by_magic = stats.get("by_magic") or {}
    ctx["magics"] = {p["magic"] for p in positions} | {int(m) for m in by_magic}

    return {
        "account": {
            "login": a.login, "server": a.server, "currency": a.currency, "name": a.name,
            "balance": a.balance, "equity": a.equity, "margin": a.margin,
            "free": a.margin_free, "level": a.margin_level if a.margin > 0 else 0,
            "floating": a.profit, "leverage": a.leverage,
        },
        "terminal": {"connected": t.connected, "trade_allowed": t.trade_allowed},
        "day": {"start": base, "pnl": pnl, "pct": pnl / base * 100 if base else 0},
        "stats": {k: stats.get(k) for k in ("count", "wins", "losses", "net")} if stats else {},
        "limits": {k: A.get(k, 0) for k in ("daily_loss_percent", "daily_profit_target", "max_open_positions")},
        "positions": positions,
        "orders": len(mt5.orders_get() or ()),
    }


def sync_trades(ctx):
    """Pull closed positions from the terminal's history into the analytics table."""
    key, now = ctx["key"], time.time()
    with SLOCK:
        acc = S["accounts"].get(key)
        if acc is None:
            return
        last = acc.get("sync_ts", 0)
        days = max(7, int(S["analytics"]["days"]))
    start = now - days * 86400 if not last else last - 2 * 86400
    deals = core.deals_in_range(start, now + 60)
    if deals is None:
        return
    trades = analytics.build_trades(mt5, core, deals)
    with DB_LOCK:
        DB.executemany(
            "INSERT OR REPLACE INTO trades VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [(key, t["pid"], t["symbol"], t["side"], t["volume"], t["open_ts"], t["close_ts"],
              t["open_price"], t["close_price"], t["gross"], t["costs"], t["points"], t["magic"],
              t["comment"]) for t in trades])
        DB.commit()
    with SLOCK:
        acc["sync_ts"] = now
        save_settings()
    if not last:
        logger.info("Analytics: imported %d closed trades (%d days of history)", len(trades), days)


def run_pass():
    core.update_server_offset()
    core.process_new_deals()
    core.monitor_positions()
    core.monitor_orders()
    core.monitor_risk()
    core.check_daily_report()
    core.heartbeat()
    core.flush_outbox()


def visit(ctx):
    CUR[0] = ctx
    applied = False
    try:
        if not attach(ctx):
            ctx["ok"] = False
            ctx["fail"] += 1
            if ctx["state"] is not None and ctx["fail"] == 2:
                apply(ctx)
                applied = True
                core.notify(f"🔴 <b>TERMINAL UNREACHABLE</b>\n{core.esc(ctx['err'])}")
            return

        a = mt5.account_info()
        bind_identity(ctx, a)
        apply(ctx)
        applied = True

        if ctx["fail"] >= 2:
            core.notify("🟢 <b>Terminal reachable again</b>")
        ctx.update(ok=True, fail=0, err="", last_ok=time.time())

        core.update_server_offset(force=ctx["first"])

        if not core.STATE["initialized"]:
            time.sleep(3)  # let the terminal finish loading its history
            core.initialize_baseline()

        if ctx["first"]:
            ctx["first"] = False
            if core.on("notify_startup"):
                core.startup_message()

        dwell = float(S["rr"]["dwell_seconds"])
        started = time.time()
        while True:
            run_pass()
            if time.time() - started >= dwell:
                break
            time.sleep(core.POLL_INTERVAL)

        snap = snapshot(ctx)
        if snap:
            ctx["snap"] = snap
            if time.time() - ctx["last_equity_ts"] >= 30:
                ctx["last_equity_ts"] = time.time()
                acc = snap["account"]
                db("INSERT INTO snap VALUES(?,?,?,?)",
                   (int(time.time()), ctx["key"], acc["balance"], acc["equity"]))
            with SLOCK:
                entry = S["accounts"].get(ctx["key"])
                known = set(entry.get("magics", [])) if entry else set()
                if entry is not None and not ctx["magics"] <= known:
                    entry["magics"] = sorted(known | ctx["magics"])
                    save_settings()

        if S["analytics"]["enabled"] and time.time() - ctx.get("last_sync", 0) >= 90:
            ctx["last_sync"] = time.time()
            try:
                sync_trades(ctx)
            except Exception:
                logger.exception("Analytics sync failed")

        core.save_state()

    except Exception as e:
        ctx["ok"] = False
        ctx["err"] = str(e)[:160]
        logger.exception("Visit crashed")
        try:
            if applied and core.cooldown_ok("visit_error", 900):
                core.notify(f"⚠️ <b>MONITOR ERROR</b>\n<code>{core.esc(repr(e))[:400]}</code>")
            if applied:
                core.save_state()
        except Exception:
            pass
    finally:
        if applied:
            release(ctx)
        CUR[0] = None


# =========================================================
# COMBINED MESSAGES: one message for all accounts (startup / heartbeat / daily)
# =========================================================

RPT = {"pending": None, "started": False, "rounds": 0}


def fnum(v, d=2):
    return f"{float(v or 0):,.{d}f}"


def fsg(v, d=2):
    return f"{float(v or 0):+,.{d}f}"


def render(tpl, d):
    return re.sub(r"\{(\w+)\}", lambda m: str(d[m.group(1)]) if m.group(1) in d else m.group(0), tpl)


def report_ctxs():
    with CT_LOCK:
        ctxs = [c for c in CTX.values() if c["key"] and c["snap"]]
    return [c for c in ctxs if not S["accounts"].get(c["key"], {}).get("muted")]


def acct_dict(ctx):
    snap, a, day = ctx["snap"], ctx["snap"]["account"], ctx["snap"]["day"]
    st = (ctx.get("day_stats") or {}).get("stats") or {}
    cnt, w, l = st.get("count", 0), st.get("wins", 0), st.get("losses", 0)
    gp, gl = st.get("gross_profit", 0.0), st.get("gross_loss", 0.0)
    online = ctx["ok"] and ctx["running"]
    pos = []
    if S["monitor"]["notify_existing_positions_on_start"]:
        for p in snap["positions"]:
            pos.append(f"• {p['symbol']} {p['type']} {p['volume']} @ {fnum(p['open'], p['digits'])}  "
                       f"P/L {fsg(p['profit'])}  [{html.escape(p['ea'])}]")
    seen = datetime.fromtimestamp(ctx["last_ok"] or time.time()).strftime("%H:%M")
    return {
        "label": html.escape(S["accounts"].get(ctx["key"], {}).get("label", ctx["key"])),
        "login": a["login"], "server": html.escape(str(a["server"])), "currency": a["currency"],
        "balance": fnum(a["balance"]), "equity": fnum(a["equity"]), "floating": fsg(a["floating"]),
        "today": fsg(day["pnl"]), "today_pct": f"{day['pct']:+.2f}",
        "margin_level": f"{a['level']:.0f}" if a["level"] else "-",
        "open": len(snap["positions"]), "orders": snap["orders"],
        "status": "" if online else f"⚠ offline (data from {seen})",
        "net": fsg(st.get("net", 0.0)), "trades": cnt, "wins": w, "losses": l,
        "winrate": f"{w / (w + l) * 100:.0f}" if (w + l) else "-",
        "pf": f"{gp / abs(gl):.2f}" if gl < 0 else ("∞" if gp > 0 else "-"),
        "points": f"{st.get('points', 0.0):+.0f}",
        "commission": fsg(st.get("commission", 0.0)), "swap": fsg(st.get("swap", 0.0)),
        "positions": "\n".join(pos),
        "_raw": {"balance": a["balance"], "equity": a["equity"], "floating": a["floating"],
                 "today": day["pnl"], "net": st.get("net", 0.0) or 0.0, "trades": cnt, "wins": w,
                 "losses": l, "points": st.get("points", 0.0) or 0.0, "cur": a["currency"]},
    }


def aggregate(ctxs, trade_filter=None):
    sym, ea = {}, {}
    for c in ctxs:
        for t in (c.get("day_stats") or {}).get("trades", []):
            if trade_filter and not trade_filter(t):
                continue
            for tbl, key in ((sym, t["symbol"]), (ea, t["ea"])):
                r = tbl.setdefault(key, [0, 0.0, 0, 0.0])
                r[0] += 1
                r[1] += t["net"]
                r[2] += 1 if t["net"] > 0 else 0
                r[3] += t["points"]
    return sym, ea


def lines_of(tbl, unit, points=False):
    if not tbl:
        return "—"
    rows = sorted(tbl.items(), key=lambda kv: kv[1][3 if points else 1], reverse=True)
    if points:
        return "\n".join(f"{html.escape(str(k))}: {v[0]} {unit}, {v[3]:+.0f} points" for k, v in rows)
    return "\n".join(f"{html.escape(str(k))}: {v[0]} {unit}, {fsg(v[1])} (WR {v[2] / v[0] * 100:.0f}%)" for k, v in rows)


def totals(accs):
    raw = [a["_raw"] for a in accs]
    s = lambda k: sum(r[k] for r in raw)
    w, l = s("wins"), s("losses")
    return {
        "count": len(accs), "online": sum(1 for a in accs if not a["status"]),
        "total_balance": fnum(s("balance")), "total_equity": fnum(s("equity")),
        "total_floating": fsg(s("floating")), "total_today": fsg(s("today")),
        "total_net": fsg(s("net")), "total_trades": s("trades"), "total_wins": w, "total_losses": l,
        "total_winrate": f"{w / (w + l) * 100:.0f}" if (w + l) else "-",
        "total_points": f"{s('points'):+.0f}",
        "currency": raw[0]["cur"] if raw else "",
        "date": datetime.now().strftime("%Y-%m-%d"), "time": datetime.now().strftime("%H:%M"),
    }


def build_message(kind, ctxs, F, dest=None):
    if kind == "daily_public":
        flt = (lambda t: core.trade_ok(dest, t)) if dest else None
        trades = [t for c in ctxs for t in (c.get("day_stats") or {}).get("trades", []) if not flt or flt(t)]
        wins = [t for t in trades if t["points"] > 0]
        losses = [t for t in trades if t["points"] < 0]
        pts = sum(t["points"] for t in trades)
        best = max(trades, key=lambda t: t["points"]) if trades else None
        worst = min(trades, key=lambda t: t["points"]) if trades else None
        sym, _ = aggregate(ctxs, flt)
        return render(F["daily_public"], {
            "date": datetime.now().strftime("%Y-%m-%d"), "icon": "🟢" if pts >= 0 else "🔴",
            "points": f"{pts:+.0f}", "trades": len(trades), "wins": len(wins), "losses": len(losses),
            "winrate": f"{len(wins) / (len(wins) + len(losses)) * 100:.0f}" if (wins or losses) else "-",
            "best_points": f"{best['points']:+.0f}" if best else "-", "best_symbol": html.escape(best["symbol"]) if best else "-",
            "worst_points": f"{worst['points']:+.0f}" if worst else "-", "worst_symbol": html.escape(worst["symbol"]) if worst else "-",
            "by_symbol": lines_of(sym, "signals", points=True),
        })

    accs = [acct_dict(c) for c in ctxs]
    block = {"startup": "startup_account", "heartbeat": "heartbeat_account",
             "daily_full": "daily_full_account"}[kind]
    sep = "\n" if kind == "heartbeat" else "\n\n"
    d = totals(accs)
    sym, ea = aggregate(ctxs)
    d.update({"accounts": sep.join(render(F[block], a) for a in accs),
              "by_symbol": lines_of(sym, "trades"), "by_ea": lines_of(ea, "trades")})
    return render(F[kind], d)


def send_global(items):
    """Send (chat, bot, text, silent) tuples through any bound account's outbox."""
    with CT_LOCK:
        carriers = [c for c in CTX.values() if c["state"] is not None]
    if not carriers or not items:
        return False
    c = carriers[0]
    CUR[0] = c
    apply(c)
    try:
        for chat, bot, text, silent in items:
            core.enqueue(chat, text, silent, bot)
        core.flush_outbox()
        core.save_state()
    finally:
        release(c)
        CUR[0] = None
    return True


def sys_dests():
    return [d for d in S["destinations"] if d.get("enabled", True) and d.get("chat_id")
            and d.get("mode", "full") == "full" and ("*" in d["events"] or "system" in d["events"])]


def log_all(kind, text):
    db("INSERT INTO events(ts,acc,kind,text) VALUES(?,?,?,?)", (int(time.time()), "ALL", kind, text))


def send_daily(force):
    ctxs = report_ctxs()
    F = S["report_format"]
    items = []
    for d in S["destinations"]:
        if not d.get("enabled", True) or not d.get("chat_id"):
            continue
        mode = d.get("report") or ("full" if d.get("mode") == "full" else "public")
        if mode == "none":
            continue
        kind = "daily_full" if mode == "full" else "daily_public"
        items.append((d["chat_id"], d.get("bot") or None, build_message(kind, ctxs, F, d), False))
    if send_global(items):
        log_all("report", "Combined daily report sent")


def finish_round():
    """Runs after every full round: only now, with all accounts freshly checked, messages are written."""
    RPT["rounds"] += 1
    ctxs = report_ctxs()
    with CT_LOCK:
        running = [c for c in CTX.values() if c["running"]]

    if not RPT["started"] and ctxs and (len(ctxs) >= len(running) or RPT["rounds"] >= 2):
        RPT["started"] = True
        S["runtime"]["last_heartbeat"] = time.time()
        if S["monitor"]["notify_startup"]:
            F = S["report_format"]
            text = build_message("startup", ctxs, F)
            if send_global([(d["chat_id"], d.get("bot") or None, text, False) for d in sys_dests()]):
                log_all("system", "Hub started")
        save_settings()

    cfg = S["daily_report"]
    now = datetime.now()
    today = now.strftime("%Y-%m-%d")
    if (cfg["enabled"] and not RPT["pending"] and S["runtime"]["last_report_date"] != today
            and now.strftime("%H:%M") >= cfg["time"]):
        RPT["pending"] = {"since": time.time(), "rounds": 0, "force": False, "date": today}

    p = RPT["pending"]
    if p and ctxs:
        fresh = all((c.get("day_stats") or {}).get("t", 0) >= p["since"] for c in ctxs if c["running"])
        p["rounds"] += 1
        if fresh or p["rounds"] >= 3:
            RPT["pending"] = None
            has_trades = any(((c.get("day_stats") or {}).get("stats") or {}).get("count", 0) for c in ctxs)
            if p["force"] or has_trades or cfg["send_if_no_trades"]:
                send_daily(p["force"])
            if p["date"]:
                S["runtime"]["last_report_date"] = p["date"]
                save_settings()

    hrs = float(S["monitor"]["heartbeat_hours"] or 0)
    if hrs > 0 and RPT["started"] and ctxs and time.time() - S["runtime"]["last_heartbeat"] >= hrs * 3600:
        S["runtime"]["last_heartbeat"] = time.time()
        text = build_message("heartbeat", ctxs, S["report_format"])
        send_global([(d["chat_id"], d.get("bot") or None, text, True) for d in sys_dests()])
        save_settings()


# =========================================================
# PANEL PAYLOAD
# =========================================================

def build_state():
    accounts, positions = [], []
    tot = {"balance": 0.0, "equity": 0.0, "floating": 0.0, "pnl": 0.0, "start": 0.0,
           "positions": 0, "online": 0, "currencies": set()}
    bound = set()

    with CT_LOCK:
        ctxs = list(CTX.values())

    for ctx in ctxs:
        key = ctx["key"]
        if key:
            bound.add(key)
        acc = S["accounts"].get(key, {}) if key else {}
        out = {
            "key": key or ctx["path"], "label": acc.get("label") or "(identifying…)",
            "muted": bool(acc.get("muted")), "path": ctx["path"], "running": ctx["running"],
            "ok": ctx["ok"] and ctx["running"], "err": ctx["err"],
            "age": time.time() - ctx["last_ok"] if ctx["last_ok"] else None,
            "login": acc.get("login"), "server": acc.get("server"), "company": acc.get("company"),
        }
        st = ctx["snap"]
        if st:
            out.update(account=st["account"], terminal=st["terminal"], day=st["day"],
                       stats=st["stats"], limits=st["limits"], orders=st["orders"])
            if ctx["running"]:
                tot["balance"] += st["account"]["balance"]
                tot["equity"] += st["account"]["equity"]
                tot["floating"] += st["account"]["floating"]
                tot["pnl"] += st["day"]["pnl"]
                tot["start"] += st["day"]["start"]
                tot["positions"] += len(st["positions"])
                tot["currencies"].add(st["account"]["currency"])
                positions += [dict(p, acc=out["label"]) for p in st["positions"]]
        if out["ok"]:
            tot["online"] += 1
        rows = db("SELECT equity FROM snap WHERE acc=? AND ts>=? ORDER BY ts",
                  (key, time.time() - 86400), True) if key else []
        out["spark"] = [r[0] for r in rows[::max(1, len(rows) // 40)]]
        accounts.append(out)

    for key, acc in S["accounts"].items():       # known accounts whose terminal is closed
        if key not in bound:
            accounts.append({"key": key, "label": acc.get("label", key), "muted": bool(acc.get("muted")),
                             "path": acc.get("path", ""), "running": False, "ok": False,
                             "err": "terminal is not running", "age": None, "login": acc.get("login"),
                             "server": acc.get("server"), "company": acc.get("company"), "spark": []})

    events = db("SELECT ts,acc,kind,text FROM events ORDER BY id DESC LIMIT 120", fetch=True)
    tot["currencies"] = sorted(tot["currencies"])
    tot["pct"] = tot["pnl"] / tot["start"] * 100 if tot["start"] else 0.0
    tot["total"] = sum(1 for a in accounts if a["running"])

    with SLOCK:
        setup = {
            "token": any(b["token"] for b in S["telegram"]["bots"]),
            "destinations": len([d for d in S["destinations"] if d.get("chat_id")]),
            "terminals": len(ctxs),
            "psutil": INFO["psutil"],
        }

    return {
        "time": time.time(), "totals": tot, "accounts": accounts, "positions": positions,
        "events": [{"ts": r[0], "acc": r[1], "kind": r[2], "text": r[3]} for r in events],
        "setup": setup, "round_sec": INFO["round_sec"],
    }


def api_analytics(q):
    g = lambda k, d="": q.get(k, [d])[0]
    acc = g("acc", "ALL")
    days = int(float(g("days", "90") or 0))
    keys = list(S["accounts"].keys()) if acc == "ALL" else [acc]

    tz = g("tz", "0")
    if tz == "server":
        off = 0.0
        with CT_LOCK:
            for c in CTX.values():
                if c["key"] in keys and c["state"] is not None:
                    off = c["state"].get("server_offset", 0) / 3600
                    break
        tz = off
    tz = float(tz)

    since = 0 if days <= 0 else time.time() - days * 86400
    rows = []
    if keys:
        ph = ",".join("?" * len(keys))
        for r in db(f"SELECT acc,pid,symbol,side,volume,open_ts,close_ts,open_price,close_price,gross,costs,points,magic "
                    f"FROM trades WHERE close_ts>=? AND acc IN ({ph})", [since] + keys, True):
            rows.append(dict(zip(("acc", "pid", "symbol", "side", "volume", "open_ts", "close_ts", "open_price",
                                  "close_price", "gross", "costs", "points", "magic"), r)))

    def ea_of(key, magic):
        if not magic:
            return "Manual"
        return S["accounts"].get(key, {}).get("ea_names", {}).get(str(magic)) or f"Magic {magic}"

    res = analytics.compute(rows, {"tz": tz, "basis": g("basis", "open"), "min_n": g("min", "8") or 8,
                                   "symbol": g("symbol"), "ea": g("ea"), "side": g("side")},
                            ea_of, acc_label, time.time())
    counts = dict(db("SELECT acc, COUNT(*) FROM trades GROUP BY acc", fetch=True))
    res["meta"]["accounts"] = [{"key": k, "label": a.get("label", k), "n": counts.get(k, 0)}
                               for k, a in S["accounts"].items()]
    res["meta"]["syncing"] = [a.get("label", k) for k, a in S["accounts"].items() if not a.get("sync_ts")]
    return res


def api_equity(hours):
    rows = db("SELECT ts,acc,balance,equity FROM snap WHERE ts>=? ORDER BY ts",
              (time.time() - hours * 3600,), True)
    names = {r[1] for r in rows}
    last, out = {}, []
    for ts, acc, bal, eq in rows:
        last[acc] = (bal, eq)
        if len(last) == len(names):
            out.append([ts, sum(v[0] for v in last.values()), sum(v[1] for v in last.values())])
    return out[::max(1, len(out) // 300)]


# =========================================================
# HTTP
# =========================================================

OK_TOKENS = set()


def pw_hash(password, salt):
    return hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), 60000).hex()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def _send(self, code, body, ctype="application/json"):
        data = body if isinstance(body, bytes) else json.dumps(body).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype + "; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.end_headers()
        self.wfile.write(data)

    def _authed(self):
        h = S["web"]["password_hash"]
        if not h:
            return True
        got = self.headers.get("X-Auth", "")
        tok = hashlib.sha256(got.encode()).hexdigest()
        if tok in OK_TOKENS:
            return True
        if got and hmac.compare_digest(pw_hash(got, S["web"]["salt"]), h):
            OK_TOKENS.add(tok)
            return True
        time.sleep(0.5)
        return False

    def do_GET(self):
        u = urlparse(self.path)
        q = parse_qs(u.query)

        if u.path in ("/", "/index.html"):
            try:
                with open(os.path.join(BASE, "app.html"), "rb") as f:
                    return self._send(200, f.read(), "text/html")
            except OSError:
                return self._send(404, {"error": "app.html missing"})

        if u.path == "/api/ping":
            return self._send(200, {"auth_required": bool(S["web"]["password_hash"]), "ok": self._authed()})

        if not self._authed():
            return self._send(401, {"error": "unauthorized"})

        try:
            if u.path == "/api/state":
                return self._send(200, build_state())
            if u.path == "/api/settings":
                return self._send(200, settings_view())
            if u.path == "/api/analytics":
                return self._send(200, api_analytics(q))
            if u.path == "/api/equity":
                return self._send(200, api_equity(float(q.get("hours", ["24"])[0])))
        except Exception as e:
            logger.exception("GET failed")
            return self._send(500, {"error": str(e)})

        self._send(404, {"error": "not found"})

    def do_POST(self):
        path = urlparse(self.path).path
        if self.headers.get("X-Requested-With") != "panel":
            return self._send(403, {"error": "forbidden"})
        if not self._authed():
            return self._send(401, {"error": "unauthorized"})

        try:
            length = min(int(self.headers.get("Content-Length", 0)), 200_000)
            body = json.loads(self.rfile.read(length) or b"{}")
            fn = ROUTES.get(path)
            if not fn:
                return self._send(404, {"error": "not found"})
            self._send(200, fn(body))
        except Exception as e:
            logger.exception("POST failed")
            self._send(500, {"ok": False, "msg": str(e)})


# ---- POST routes -------------------------------------------------

def r_settings(body):
    patch = body.get("patch", {})
    with SLOCK:
        for k in list(patch):
            if k not in PATCHABLE and k != "web":
                patch.pop(k)
        web = patch.pop("web", None)
        bots_in = (patch.get("telegram") or {}).pop("bots", None)
        if bots_in is not None:
            old = {b["id"]: b for b in S["telegram"]["bots"]}
            bots = []
            for it in bots_in:
                bid = str(it.get("id", ""))
                if not re.fullmatch(r"[a-z0-9]{4,12}", bid):
                    bid = secrets.token_hex(3)
                tok = str(it.get("token", "")).strip()
                if not tok and bid in old:
                    tok = old[bid]["token"]
                if not tok:
                    continue
                bots.append({"id": bid, "name": (str(it.get("name") or "").strip() or f"bot{len(bots) + 1}")[:30],
                             "token": tok, "proxy": bool(it.get("proxy", True))})
            S["telegram"]["bots"] = bots
        patch_into(S, patch)
        S["destinations"] = clean_destinations(S["destinations"])
        S["rr"]["dwell_seconds"] = max(0, S["rr"]["dwell_seconds"])
        S["rr"]["discover_seconds"] = max(5, S["rr"]["discover_seconds"])
        msg = "Settings saved - applied on the next visit of each account"
        if web is not None and "lan" in web:
            if web["lan"] and not S["web"]["password_hash"]:
                save_settings()
                return {"ok": False, "msg": "Set a panel password first, then enable remote access"}
            S["web"]["lan"] = bool(web["lan"])
            msg += " (remote access change needs a restart)"
        save_settings()
    return {"ok": True, "msg": msg}


def r_account(body):
    with SLOCK:
        acc = S["accounts"].get(body.get("key"))
        if acc is None:
            return {"ok": False, "msg": "unknown account"}
        if "label" in body:
            acc["label"] = str(body["label"]).strip()[:24] or acc["label"]
        if "muted" in body:
            acc["muted"] = bool(body["muted"])
        if "ea_names" in body:
            acc["ea_names"] = {str(int(k)): str(v)[:40] for k, v in body["ea_names"].items() if str(v).strip()}
        save_settings()
    return {"ok": True, "msg": "Account saved"}


def r_terminal(body):
    path = os.path.normpath(str(body.get("path", "")).strip().strip('"'))
    action = body.get("action")
    with SLOCK:
        t = S["terminals"]
        low = path.lower()
        if action == "add":
            if not path or path == ".":
                return {"ok": False, "msg": "enter the full path of terminal64.exe"}
            if low not in [p.lower() for p in t["manual"]]:
                t["manual"].append(path)
            t["ignored"] = [p for p in t["ignored"] if p.lower() != low]
        elif action == "remove":
            t["manual"] = [p for p in t["manual"] if p.lower() != low]
        elif action == "ignore":
            if low not in [p.lower() for p in t["ignored"]]:
                t["ignored"].append(path)
        elif action == "unignore":
            t["ignored"] = [p for p in t["ignored"] if p.lower() != low]
        save_settings()
    discover()
    return {"ok": True, "msg": "Terminal list updated"}


def r_tg(body):
    action = body.get("action")
    bot = body.get("bot") or None
    if action == "getme":
        r = tg_call("getMe", bot_id=bot)
        if r.get("ok"):
            return {"ok": True, "msg": f"Connected as @{r['result'].get('username')}"}
        return {"ok": False, "msg": r.get("description", "failed")}
    if action == "detect":
        r = tg_detect(bot)
        return {"ok": r.get("ok", False), "msg": r.get("description", ""), "chats": r.get("chats", [])}
    if action == "test":
        chat = str(body.get("chat_id", "")).strip()
        if not chat:
            return {"ok": False, "msg": "chat id is empty"}
        r = tg_call("sendMessage", {"chat_id": chat, "text": "✅ <b>Test message</b> from MT5 Hub",
                                    "parse_mode": "HTML"}, bot_id=bot)
        return {"ok": bool(r.get("ok")), "msg": "Test message sent" if r.get("ok") else r.get("description", "failed")}
    return {"ok": False, "msg": "unknown action"}


def r_password(body):
    new = str(body.get("new", ""))
    with SLOCK:
        if not new:
            if S["web"]["lan"]:
                return {"ok": False, "msg": "Disable remote access before removing the password"}
            S["web"]["password_hash"] = ""
            S["web"]["salt"] = ""
        else:
            salt = secrets.token_hex(16)
            S["web"]["salt"] = salt
            S["web"]["password_hash"] = pw_hash(new, salt)
        OK_TOKENS.clear()
        save_settings()
    return {"ok": True, "msg": "Password updated - sign in again" if new else "Password removed"}


def r_report_now(body):
    RPT["pending"] = {"since": time.time(), "rounds": 0, "force": True, "date": None}
    return {"ok": True, "msg": "One combined report will be sent after all accounts are checked"}


def r_preview(body):
    F = dict(S["report_format"])
    F.update({k: str(v) for k, v in (body.get("formats") or {}).items() if k in F})
    ctxs = report_ctxs()
    if not ctxs:
        return {"ok": False, "msg": "No account data yet - wait for the first round to finish"}
    kind = body.get("kind")
    if kind not in ("startup", "heartbeat", "daily_full", "daily_public"):
        return {"ok": False, "msg": "unknown format"}
    return {"ok": True, "text": build_message(kind, ctxs, F)}


def r_resync(body):
    with SLOCK:
        for acc in S["accounts"].values():
            acc["sync_ts"] = 0
        save_settings()
    if body.get("wipe"):
        db("DELETE FROM trades")
    with CT_LOCK:
        for c in CTX.values():
            c["last_sync"] = 0
    return {"ok": True, "msg": "History will be re-imported as each account is visited"}


def r_restart(body):
    def go():
        time.sleep(0.7)
        subprocess.Popen([sys.executable] + ORIG_ARGV)
        os._exit(0)
    threading.Thread(target=go, daemon=True).start()
    return {"ok": True, "msg": "Restarting… reload the page in a few seconds"}


ROUTES = {
    "/api/settings": r_settings, "/api/account": r_account, "/api/terminal": r_terminal,
    "/api/tg": r_tg, "/api/password": r_password, "/api/report_now": r_report_now,
    "/api/restart": r_restart, "/api/analytics_resync": r_resync, "/api/report_preview": r_preview,
}


def start_web():
    host = "0.0.0.0" if (S["web"]["lan"] and S["web"]["password_hash"]) else "127.0.0.1"
    host = cli("--host", host)
    port0 = int(cli("--port", 9000))
    for port in range(port0, port0 + 20):
        try:
            server = ThreadingHTTPServer((host, port), Handler)
            break
        except OSError:
            continue
    else:
        raise SystemExit("No free port found for the panel")
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{port}"
    print(f"\n  MT5 Hub panel:  {url}\n")
    logger.info("Panel: %s (listening on %s)", url, host)
    if "--no-browser" not in ORIG_ARGV:
        try:
            webbrowser.open(url)
        except Exception:
            pass


# =========================================================
# MAIN LOOP
# =========================================================

def main():
    start_web()
    last_discover = 0.0

    try:
        while True:
            if time.time() - last_discover >= S["rr"]["discover_seconds"]:
                discover()
                last_discover = time.time()

            with CT_LOCK:
                targets = [c for c in CTX.values() if c["running"]]
            if not targets:
                time.sleep(2)
                continue

            t0 = time.time()
            for ctx in targets:
                if ctx["running"]:
                    visit(ctx)
                    time.sleep(0.2)
            INFO["round_sec"] = time.time() - t0
            try:
                finish_round()
            except Exception:
                logger.exception("Combined report failed")

    except KeyboardInterrupt:
        logger.info("Stopped by user")
        with CT_LOCK:
            ctxs = [c for c in CTX.values() if c["state"] is not None]
        for ctx in ctxs:
            CUR[0] = ctx
            apply(ctx)
            try:
                core.notify("⚠️ <b>MT5 HUB STOPPED BY USER</b>")
                core.flush_outbox()
                core.save_state()
            except Exception:
                pass
            finally:
                release(ctx)
                CUR[0] = None
    finally:
        mt5.shutdown()


if __name__ == "__main__":
    main()
