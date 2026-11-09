# -*- coding: utf-8 -*-
"""
MT5 -> Telegram Monitor (v2)

Requirements:
    pip install MetaTrader5 requests
    pip install "requests[socks]"      # only if you use a SOCKS proxy

Files next to this script:
    config.json   (see config.example.json)
    state.json    (created automatically)
    monitor.log   (created automatically)
"""

import calendar
import html
import sys
import threading
import json
import logging
import os
import re
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler

import MetaTrader5 as mt5
import requests

try:
    import panel as PANEL
except ImportError:
    PANEL = None

MT5_LOCK = threading.RLock()


# =========================================================
# CONFIGURATION
# =========================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Several accounts: run one process per account with  --config acc1.json
_cfg_arg = None
for _i, _a in enumerate(sys.argv):
    if _a == "--config" and _i + 1 < len(sys.argv):
        _cfg_arg = sys.argv[_i + 1]

CONFIG_FILE = os.path.abspath(_cfg_arg) if _cfg_arg else os.path.join(BASE_DIR, "config.json")
INSTANCE = os.path.splitext(os.path.basename(CONFIG_FILE))[0]
_sfx = "" if INSTANCE == "config" else "_" + INSTANCE
STATE_FILE = os.path.join(BASE_DIR, f"state{_sfx}.json")

with open(CONFIG_FILE, "r", encoding="utf-8") as f:
    CONFIG = json.load(f)


def cfg(section, key, default=None):
    block = CONFIG.get(section, {})
    if not isinstance(block, dict):
        return default
    return block.get(key, default)


TOKEN = CONFIG["telegram"]["bot_token"]
CHAT_ID = str(CONFIG["telegram"]["chat_id"])

ACCOUNT_LABEL = str(CONFIG.get("account_label", "") or "")


def tag(text):
    """Prefix private messages with the account label (not used for channels)."""
    return f"<b>[{esc(ACCOUNT_LABEL)}]</b> {text}" if ACCOUNT_LABEL else text

POLL_INTERVAL = cfg("mt5", "poll_interval", 2)
LOOKBACK_DAYS = cfg("mt5", "history_lookback_days", 7)
TERMINAL_PATH = cfg("mt5", "terminal_path", "")

ALERTS = CONFIG.get("alerts", {})


class RuntimeAlerts(object):
    """Alert thresholds: panel overrides first, then config.json."""

    def get(self, key, default=None):
        return STATE.get("settings", {}).get(key, ALERTS.get(key, default))

REPORT = CONFIG.get("daily_report", {})
COMMANDS = CONFIG.get("commands", {})
DAY_MODE = str(REPORT.get("day_boundary", "local")).lower()  # local | server

OUTBOX_MAX = 500


def on(key, default=True):
    return bool(cfg("monitor", key, default))


# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        RotatingFileHandler(
            os.path.join(BASE_DIR, os.environ.get("MT5_LOG_NAME", f"monitor{_sfx}") + ".log"),
            maxBytes=5 * 1024 * 1024,
            backupCount=3,
            encoding="utf-8",
        ),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("MT5-Monitor")


# =========================================================
# PERSISTENT STATE
# =========================================================

def default_state():
    return {
        "initialized": False,
        "last_deal_msc": 0,
        "processed_deals": [],
        "positions": {},
        "orders": {},
        "account": {},
        "connection": None,
        "outbox": [],
        "server_offset": 0,
        "last_report_date": "",
        "tg_offset": 0,
        "cmd_primed": False,
        "flags": {},
        "alert_ts": {},
        "warned": {"nosl": [], "loss": []},
        "day": {},
        "last_heartbeat": 0,
        "settings": {},
        "dest_overrides": {},
        "control": {
            "paused_all": False, "paused_magics": [], "paused_symbols": [],
            "since": {}, "lock_day": "", "lock_reason": "",
        },
    }


def load_state():
    state = default_state()

    if not os.path.exists(STATE_FILE):
        return state

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            loaded = json.load(f)
        state.update(loaded)
        for k, v in default_state().items():
            state.setdefault(k, v)
        return state

    except Exception:
        logger.exception("State file is corrupt - starting from a clean baseline")
        try:
            os.replace(STATE_FILE, STATE_FILE + ".corrupt")
        except OSError:
            pass
        return default_state()


STATE = load_state()


def save_state():
    temp = STATE_FILE + ".tmp"

    try:
        with open(temp, "w", encoding="utf-8") as f:
            json.dump(STATE, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())

        for attempt in range(10):
            try:
                os.replace(temp, STATE_FILE)
                return True
            except PermissionError:
                if attempt < 9:
                    time.sleep(0.3)
                else:
                    raise

    except Exception:
        logger.exception("Failed to save monitor state")
        try:
            if os.path.exists(temp):
                os.remove(temp)
        except OSError:
            pass

    return False


# =========================================================
# TELEGRAM (persistent outbox + retry + proxy)
# =========================================================

TELEGRAM_CONFIG = CONFIG["telegram"]
PROXY_CONFIG = TELEGRAM_CONFIG.get("proxy", {}) or {}
API = f"https://api.telegram.org/bot{TOKEN}"


def build_proxy():
    if not PROXY_CONFIG.get("enabled", False):
        return None

    proxy_type = PROXY_CONFIG.get("type", "socks5").lower()
    host = PROXY_CONFIG.get("host", "127.0.0.1")
    port = PROXY_CONFIG.get("port", 10808)
    username = PROXY_CONFIG.get("username", "")
    password = PROXY_CONFIG.get("password", "")

    credentials = f"{username}:{password}@" if username and password else ""

    schemes = {
        "socks5": "socks5h",
        "socks5h": "socks5h",
        "http": "http",
        "https": "https",
    }
    if proxy_type not in schemes:
        raise ValueError(f"Unsupported proxy type: {proxy_type}")

    url = f"{schemes[proxy_type]}://{credentials}{host}:{port}"
    return {"http": url, "https": url}


PROXIES = build_proxy()

_next_send_ok = 0.0


def esc(value):
    return html.escape(str(value), quote=False)


def split_message(text, limit=4000):
    chunks = []
    while len(text) > limit:
        cut = text.rfind("\n", 0, limit)
        if cut <= 0:
            cut = limit
        chunks.append(text[:cut])
        text = text[cut:].lstrip("\n")
    chunks.append(text)
    return chunks


def get_destinations():
    dests = CONFIG.get("destinations") or [
        {"name": "private", "chat_id": CHAT_ID, "mode": "full",
         "events": ["*"], "report": "full"}
    ]
    out = []
    for d in dests:
        d = dict(d)
        d.update(STATE.get("dest_overrides", {}).get(d.get("name"), {}))
        d["chat_id"] = str(d["chat_id"])
        out.append(d)
    return out


def dest_accepts(d, kind, magic, symbol):
    if not d.get("enabled", True):
        return False
    events = d.get("events", ["*"])
    if "*" not in events and kind not in events:
        return False
    f = d.get("filters") or {}
    if magic is not None:
        if f.get("magics") and int(magic) not in [int(m) for m in f["magics"]]:
            return False
        if f.get("only_ea") and int(magic) == 0:
            return False
    if symbol and f.get("symbols") and symbol not in f["symbols"]:
        return False
    return True


def admin_chat_ids():
    return {d["chat_id"] for d in get_destinations() if d.get("mode", "full") == "full"}


def bot_for(name):
    """(token, use_proxy) for a destination's bot; falls back to the default bot."""
    b = (CONFIG.get("bots") or {}).get(name) if name else None
    if b:
        return b.get("token", ""), bool(b.get("proxy", True))
    return TOKEN, True


def have_any_bot():
    return bool(TOKEN) or any(b.get("token") for b in (CONFIG.get("bots") or {}).values())


def enqueue(chat_id, text, silent=False, bot=None, track=None, reply=None):
    if not have_any_bot() or not str(chat_id).strip():
        return
    for n, chunk in enumerate(split_message(text.strip())):
        item = {"chat_id": str(chat_id), "bot": bot, "text": chunk, "silent": silent, "ts": time.time()}
        if n == 0:
            if track is not None:
                item["track"] = str(track)     # remember this message so results can reply to it
            if reply is not None:
                item["reply"] = str(reply)     # send as a reply to the remembered message
        STATE["outbox"].append(item)
    if len(STATE["outbox"]) > OUTBOX_MAX:
        del STATE["outbox"][:-OUTBOX_MAX]


def notify(text, silent=False, kind="system", public=None, magic=None, symbol=None, track=None, reply=None):
    """Route a message to every destination whose events/filters accept it.
    'full' destinations get `text`; 'signal' destinations only get `public`."""
    if PANEL:
        try:
            PANEL.log_event(kind, text)
        except Exception:
            pass
    for d in get_destinations():
        if not dest_accepts(d, kind, magic, symbol):
            continue
        if d.get("mode", "full") == "signal":
            if public is None:
                continue
            enqueue(d["chat_id"], public, silent, d.get("bot"), track, reply)
        else:
            enqueue(d["chat_id"], tag(text), silent, d.get("bot"), track, reply)
    flush_outbox()


def notify_order(text):
    notify(text, kind="order")


def notify_balance(text):
    notify(text, kind="balance")


def notify_risk(text):
    notify(text, kind="risk")


def _strip_html(text):
    return html.unescape(re.sub(r"<[^>]+>", "", text))


def flush_outbox():
    global _next_send_ok

    if not have_any_bot():
        return 0

    if time.time() < _next_send_ok:
        return 0

    sent = 0

    while STATE["outbox"]:
        item = STATE["outbox"][0]
        text = item["text"]

        if time.time() - item.get("ts", time.time()) > 120 and not item.get("late"):
            text = "⏳ <i>delayed delivery</i>\n" + text

        payload = {
            "chat_id": item.get("chat_id", CHAT_ID),
            "text": text,
            "disable_web_page_preview": True,
            "disable_notification": bool(item.get("silent")),
        }
        if not item.get("plain"):
            payload["parse_mode"] = "HTML"

        if item.get("reply"):
            rid = STATE.get("msgmap", {}).get(f"{item.get('chat_id')}|{item['reply']}")
            if rid:
                payload["reply_to_message_id"] = rid
                payload["allow_sending_without_reply"] = True

        tok, use_proxy = bot_for(item.get("bot"))
        if not tok:
            STATE["outbox"].pop(0)
            continue

        try:
            r = requests.post(
                f"https://api.telegram.org/bot{tok}/sendMessage",
                json=payload,
                proxies=PROXIES if use_proxy else None,
                timeout=(5, 15),
            )
            try:
                data = r.json()
            except ValueError:
                data = {}

            if r.status_code == 200 and data.get("ok"):
                if item.get("track"):
                    mid = (data.get("result") or {}).get("message_id")
                    if mid:
                        mm = STATE.setdefault("msgmap", {})
                        mm[f"{item.get('chat_id')}|{item['track']}"] = mid
                        while len(mm) > 800:
                            mm.pop(next(iter(mm)))
                STATE["outbox"].pop(0)
                sent += 1
                time.sleep(0.3)  # stay below Telegram's per-chat rate limit
                continue

            description = str(data.get("description", ""))

            if r.status_code == 429:
                retry = data.get("parameters", {}).get("retry_after", 5)
                _next_send_ok = time.time() + retry + 1
                logger.warning("Telegram rate limit, retry in %ss", retry)
                return sent

            if r.status_code == 400 and "parse" in description.lower():
                item["text"] = _strip_html(item["text"])
                item["plain"] = True
                item["late"] = True
                continue

            if r.status_code in (400, 401, 403, 404):
                # this bot/chat refuses the message: retrying cannot help, and it must
                # not block the other bots' messages behind it
                logger.error("Dropping undeliverable message (bot %s, chat %s): %s",
                             item.get("bot") or "default", item.get("chat_id"), description)
                if PANEL:
                    try:
                        PANEL.log_event("system", f"⚠ Telegram refused a message to {item.get('chat_id')}: {description}")
                    except Exception:
                        pass
                STATE["outbox"].pop(0)
                continue

            logger.error("Telegram error %s: %s", r.status_code, description)
            _next_send_ok = time.time() + 30
            return sent

        except requests.exceptions.ProxyError as e:
            logger.error("Telegram proxy error: %s", e)
        except requests.exceptions.Timeout:
            logger.error("Telegram connection timed out")
        except Exception:
            logger.exception("Telegram delivery failed")

        _next_send_ok = time.time() + 10
        return sent

    return sent


# =========================================================
# FORMAT HELPERS
# =========================================================

_digits_cache = {}


def digits_of(symbol):
    if symbol not in _digits_cache:
        info = mt5.symbol_info(symbol)
        if info is None:
            return 5
        _digits_cache[symbol] = info.digits
    return _digits_cache[symbol]


def fmt_price(symbol, value):
    return f"{float(value):.{digits_of(symbol)}f}"


def money(value):
    return f"{float(value):,.2f}"


def signed(value):
    return f"{float(value):+,.2f}"


def fmt_duration(seconds):
    seconds = max(0, int(seconds))
    d, rem = divmod(seconds, 86400)
    h, rem = divmod(rem, 3600)
    m, s = divmod(rem, 60)
    if d:
        return f"{d}d {h}h {m}m"
    if h:
        return f"{h}h {m}m"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def _enum(names):
    result = {}
    for name, label in names:
        if hasattr(mt5, name):
            result[getattr(mt5, name)] = label
    return result


REASONS = _enum([
    ("DEAL_REASON_CLIENT", "Manual (Desktop)"),
    ("DEAL_REASON_MOBILE", "Manual (Mobile)"),
    ("DEAL_REASON_WEB", "Manual (Web)"),
    ("DEAL_REASON_EXPERT", "Expert Advisor"),
    ("DEAL_REASON_SL", "Stop Loss"),
    ("DEAL_REASON_TP", "Take Profit"),
    ("DEAL_REASON_SO", "Stop Out"),
    ("DEAL_REASON_ROLLOVER", "Rollover"),
    ("DEAL_REASON_VMARGIN", "Variation Margin"),
    ("DEAL_REASON_SPLIT", "Split"),
])

BALANCE_DEAL_LABELS = _enum([
    ("DEAL_TYPE_BALANCE", "Balance operation"),
    ("DEAL_TYPE_CREDIT", "Credit"),
    ("DEAL_TYPE_CHARGE", "Charge"),
    ("DEAL_TYPE_CORRECTION", "Correction"),
    ("DEAL_TYPE_BONUS", "Bonus"),
    ("DEAL_TYPE_INTEREST", "Interest"),
    ("DEAL_TYPE_DIVIDEND", "Dividend"),
    ("DEAL_TYPE_TAX", "Tax"),
])

# Deal types that really change the account balance
BALANCE_AFFECTING = set(_enum([
    ("DEAL_TYPE_BALANCE", ""),
    ("DEAL_TYPE_CHARGE", ""),
    ("DEAL_TYPE_CORRECTION", ""),
    ("DEAL_TYPE_INTEREST", ""),
    ("DEAL_TYPE_DIVIDEND", ""),
    ("DEAL_TYPE_TAX", ""),
]).keys())

ORDER_TYPE_LABELS = _enum([
    ("ORDER_TYPE_BUY_LIMIT", "BUY LIMIT"),
    ("ORDER_TYPE_SELL_LIMIT", "SELL LIMIT"),
    ("ORDER_TYPE_BUY_STOP", "BUY STOP"),
    ("ORDER_TYPE_SELL_STOP", "SELL STOP"),
    ("ORDER_TYPE_BUY_STOP_LIMIT", "BUY STOP LIMIT"),
    ("ORDER_TYPE_SELL_STOP_LIMIT", "SELL STOP LIMIT"),
])

ORDER_STATE_LABELS = _enum([
    ("ORDER_STATE_CANCELED", "CANCELED"),
    ("ORDER_STATE_FILLED", "FILLED (position opened)"),
    ("ORDER_STATE_EXPIRED", "EXPIRED"),
    ("ORDER_STATE_REJECTED", "REJECTED"),
])


def get_order_type(value):
    return ORDER_TYPE_LABELS.get(value, str(value))


def ea_name(magic):
    if not magic:
        return "Manual"
    names = CONFIG.get("ea_names", {}) or {}
    return names.get(str(magic)) or f"Magic {magic}"


def ea_text(magic):
    name = ea_name(magic)
    return name if name == f"Magic {magic}" else f"{name} (Magic {magic})"


def origin_label(reason, magic, comment=""):
    if magic:
        text = f"Expert Advisor: {ea_text(magic)}"
    else:
        text = REASONS.get(reason, f"Other (reason {reason})")
    if comment:
        text += f"\nComment: {comment}"
    return esc(text)


def order_origin(magic, comment=""):
    if magic:
        text = f"Expert Advisor: {ea_text(magic)}"
    else:
        text = "Manual / Unknown"
    if comment:
        text += f"\nComment: {comment}"
    return esc(text)


# =========================================================
# BROKER (SERVER) TIME HANDLING
# =========================================================
# MT5 returns all timestamps in *broker server time*, not UTC.
# We compute the offset so every message shows your real local time.

_last_offset_check = 0.0


def server_offset():
    return int(STATE.get("server_offset", 0))


def to_utc_ts(server_ts):
    return server_ts - server_offset()


def local_str(server_ts):
    return datetime.fromtimestamp(to_utc_ts(server_ts)).strftime("%Y-%m-%d %H:%M:%S")


def update_server_offset(force=False):
    global _last_offset_check

    manual = cfg("mt5", "server_utc_offset_hours", None)
    if manual is not None:
        STATE["server_offset"] = int(float(manual) * 3600)
        return

    if not force and time.time() - _last_offset_check < 3600:
        return
    _last_offset_check = time.time()

    symbols = []
    try:
        for p in mt5.positions_get() or ():
            symbols.append(p.symbol)
        symbols += [s.name for s in (mt5.symbols_get() or ()) if s.select][:30]
    except Exception:
        logger.exception("Could not collect symbols for server-time detection")

    freshest = 0
    for name in symbols[:40]:
        tick = mt5.symbol_info_tick(name)
        if tick and tick.time > freshest:
            freshest = tick.time

    if not freshest:
        return

    diff = freshest - time.time()
    rounded = round(diff / 900) * 900

    if abs(diff - rounded) <= 120 and abs(rounded) <= 15 * 3600:
        if rounded != STATE.get("server_offset"):
            logger.info("Broker server UTC offset detected: %+.2f h", rounded / 3600)
        STATE["server_offset"] = int(rounded)
    else:
        logger.warning(
            "Server time offset could not be detected reliably (market closed?). "
            "Using %+.2f h. Set mt5.server_utc_offset_hours in config.json to be exact.",
            server_offset() / 3600,
        )


def day_bounds():
    """Return (start_utc_ts, end_utc_ts, label, now_naive_in_day_clock)."""
    now = time.time()

    if DAY_MODE == "server":
        off = server_offset()
        n = datetime.fromtimestamp(now + off, tz=timezone.utc).replace(tzinfo=None)
        s = datetime(n.year, n.month, n.day)
        start = calendar.timegm(s.timetuple()) - off
    else:
        n = datetime.fromtimestamp(now)
        s = datetime(n.year, n.month, n.day)
        start = s.timestamp()

    return start, now + 60, s.strftime("%Y-%m-%d"), n


def deals_in_range(start_utc_ts, end_utc_ts):
    """Deals whose real (UTC) time is inside the range. None on failure."""
    frm = datetime.fromtimestamp(start_utc_ts, tz=timezone.utc) - timedelta(days=1)
    to = datetime.fromtimestamp(end_utc_ts, tz=timezone.utc) + timedelta(days=2)

    deals = mt5.history_deals_get(frm, to)

    if deals is None:
        if mt5.last_error()[0] == getattr(mt5, "RES_S_OK", 1):
            return []
        return None

    return [d for d in deals if start_utc_ts <= to_utc_ts(d.time) < end_utc_ts]


# =========================================================
# MT5 CONNECTION
# =========================================================

def connect_mt5():
    initialized = (
        mt5.initialize(path=TERMINAL_PATH) if TERMINAL_PATH else mt5.initialize()
    )

    if not initialized:
        logger.error("MT5 initialization failed: %s", mt5.last_error())
        return False

    account = mt5.account_info()
    if account is None:
        logger.error("Account information unavailable")
        return False

    logger.info("Connected to account %s", account.login)
    return True


def ensure_connection():
    terminal = mt5.terminal_info()
    account = mt5.account_info()

    if terminal is None or account is None:
        logger.warning("MT5 connection lost")
        mt5.shutdown()
        time.sleep(2)
        return connect_mt5()

    return True


# =========================================================
# SNAPSHOT HELPERS
# =========================================================

def _tuple(result, what):
    if result is None:
        if mt5.last_error()[0] == getattr(mt5, "RES_S_OK", 1):
            return ()
        raise RuntimeError(f"{what} failed: {mt5.last_error()}")
    return result


def get_positions():
    result = {}

    for p in _tuple(mt5.positions_get(), "positions_get"):
        result[str(p.identifier)] = {
            "ticket": p.ticket,
            "identifier": p.identifier,
            "symbol": p.symbol,
            "type": p.type,
            "volume": p.volume,
            "price_open": p.price_open,
            "sl": p.sl,
            "tp": p.tp,
            "magic": p.magic,
            "comment": p.comment,
            "time": p.time,
            "_ts": {},
        }

    return result


def get_orders():
    result = {}

    for o in _tuple(mt5.orders_get(), "orders_get"):
        result[str(o.ticket)] = {
            "ticket": o.ticket,
            "symbol": o.symbol,
            "type": o.type,
            "volume": o.volume_current,
            "price": o.price_open,
            "sl": o.sl,
            "tp": o.tp,
            "magic": o.magic,
            "comment": o.comment,
            "time_setup": o.time_setup,
        }

    return result


def pos_direction(value):
    return "BUY" if value == mt5.POSITION_TYPE_BUY else "SELL"


def deal_net(d):
    return d.profit + d.commission + d.swap + getattr(d, "fee", 0.0)


def position_summary(pid):
    """Entire history of one position (all its deals)."""
    try:
        deals = mt5.history_deals_get(position=pid)
    except Exception:
        return None

    if not deals:
        return None

    deals = sorted(deals, key=lambda d: (d.time_msc, d.ticket))
    ins = [d for d in deals if d.entry == mt5.DEAL_ENTRY_IN]
    if not ins:
        ins = [d for d in deals if d.entry == mt5.DEAL_ENTRY_INOUT]

    return {
        "deals": deals,
        "open_deal": ins[0] if ins else deals[0],
        "net": sum(deal_net(d) for d in deals),
    }


def account_line():
    a = mt5.account_info()
    if not a:
        return ""
    n = len(mt5.positions_get() or ())
    return (
        f"Balance: {money(a.balance)} | Equity: {money(a.equity)} {a.currency}\n"
        f"Open positions: {n}"
    )


# =========================================================
# TRADE EVENTS
# =========================================================

def calc_risk(deal, entry, sl):
    if not sl:
        return None
    action = (
        mt5.ORDER_TYPE_BUY if deal.type == mt5.DEAL_TYPE_BUY else mt5.ORDER_TYPE_SELL
    )
    try:
        r = mt5.order_calc_profit(action, deal.symbol, deal.volume, entry, sl)
    except Exception:
        return None
    return abs(r) if r is not None else None


def deal_delay(deal):
    return max(0.0, time.time() - to_utc_ts(deal.time))


def too_late(deal):
    """True when an event is too old to be posted as a live public signal."""
    limit = cfg("monitor", "signal_max_delay_sec", 0)
    return bool(limit) and deal_delay(deal) > limit


def delay_line(deal):
    d = deal_delay(deal)
    return f"⏱ Detected {fmt_duration(d)} after it happened" if d > 90 else ""


def position_opened(deal):
    got = mt5.positions_get(ticket=deal.position_id)
    pos = got[0] if got else None

    sym = deal.symbol
    direction = "BUY" if deal.type == mt5.DEAL_TYPE_BUY else "SELL"
    sl = pos.sl if pos else 0
    tp = pos.tp if pos else 0

    increased = bool(pos and pos.volume - deal.volume > 1e-9)
    title = "➕ <b>POSITION INCREASED</b>" if increased else "🟢 <b>NEW TRADE OPENED</b>"

    rr = None
    if sl and tp and abs(deal.price - sl) > 0:
        rr = abs(tp - deal.price) / abs(deal.price - sl)

    lines = [
        title,
        "",
        f"<b>{esc(sym)}</b>  {direction}  {deal.volume} lot",
        f"Entry: {fmt_price(sym, deal.price)}",
        f"Stop Loss: {fmt_price(sym, sl) if sl else ('n/a' if pos is None else 'Not Set')}",
        f"Take Profit: {fmt_price(sym, tp) if tp else ('n/a' if pos is None else 'Not Set')}",
    ]

    risk = calc_risk(deal, deal.price, sl)
    if risk is not None:
        row = f"Risk at SL: {money(risk)}"
        if rr:
            row += f"  |  R:R 1:{rr:.2f}"
        lines.append(row)

    lines += [
        "",
        "Origin:",
        origin_label(deal.reason, deal.magic, deal.comment),
        "",
        f"Position ID: {deal.position_id}",
        f"Time: {local_str(deal.time)}",
        *filter(None, [delay_line(deal)]),
        "",
        account_line(),
    ]

    public = None
    if not increased and not too_late(deal):
        pl = [
            "📡 <b>NEW SIGNAL</b>",
            "",
            f"<b>{esc(sym)}</b>  {direction}",
            f"Entry: {fmt_price(sym, deal.price)}",
            f"SL: {fmt_price(sym, sl) if sl else ('n/a' if pos is None else 'Not Set')}",
            f"TP: {fmt_price(sym, tp) if tp else ('n/a' if pos is None else 'Not Set')}",
        ]
        if rr:
            pl.append(f"R:R 1:{rr:.2f}")
        utc = datetime.fromtimestamp(to_utc_ts(deal.time), tz=timezone.utc)
        pl.append(f"🕒 {utc.strftime('%H:%M UTC')}")
        public = "\n".join(pl)

    notify("\n".join(lines), kind="open", public=public, magic=deal.magic, symbol=sym,
           track=deal.position_id)


def position_closed(deal):
    sym = deal.symbol
    closed_dir = "BUY" if deal.type == mt5.DEAL_TYPE_SELL else "SELL"

    summary = position_summary(deal.position_id)
    still_open = bool(mt5.positions_get(ticket=deal.position_id))

    this_net = deal_net(deal)
    total_net = summary["net"] if (summary and not still_open) else this_net

    if still_open:
        title, ptitle = "◐ <b>PARTIAL CLOSE</b>", "◐ <b>SIGNAL PARTIALLY CLOSED</b>"
    elif deal.reason == getattr(mt5, "DEAL_REASON_TP", -1):
        title, ptitle = "🎯 <b>TAKE PROFIT HIT</b>", "🎯 <b>TAKE PROFIT HIT</b>"
    elif deal.reason == getattr(mt5, "DEAL_REASON_SL", -1):
        title, ptitle = "🛑 <b>STOP LOSS HIT</b>", "🛑 <b>STOP LOSS HIT</b>"
    elif deal.reason == getattr(mt5, "DEAL_REASON_SO", -1):
        title, ptitle = "⚠️ <b>STOP OUT</b>", "⚠️ <b>SIGNAL CLOSED</b>"
    elif total_net >= 0:
        title, ptitle = "✅ <b>POSITION CLOSED</b>", "✅ <b>SIGNAL CLOSED IN PROFIT</b>"
    else:
        title, ptitle = "🔴 <b>POSITION CLOSED</b>", "🔴 <b>SIGNAL CLOSED</b>"

    lines = [title, "", f"<b>{esc(sym)}</b>  {closed_dir}  {deal.volume} lot"]
    pub = [ptitle, "", f"<b>{esc(sym)}</b>  {closed_dir}"]

    pts = None
    if summary:
        o = summary["open_deal"]
        lines.append(f"Entry: {fmt_price(sym, o.price)}")
        info = mt5.symbol_info(sym)
        if info and info.point:
            pts = (deal.price - o.price) / info.point
            if closed_dir == "SELL":
                pts = -pts
            lines.append(f"Exit: {fmt_price(sym, deal.price)}  ({pts:+.0f} points)")
        else:
            lines.append(f"Exit: {fmt_price(sym, deal.price)}")
        lines.append(f"Duration: {fmt_duration(deal.time - o.time)}")
        pub.append(f"Entry: {fmt_price(sym, o.price)}  →  Exit: {fmt_price(sym, deal.price)}")
    else:
        lines.append(f"Exit: {fmt_price(sym, deal.price)}")
        pub.append(f"Exit: {fmt_price(sym, deal.price)}")

    if pts is not None:
        pub.append(f"Result: <b>{pts:+.0f} points</b>")

    fee = getattr(deal, "fee", 0.0)
    lines += [
        "",
        f"Profit: {signed(deal.profit)}",
        f"Commission: {signed(deal.commission)}  |  Swap: {signed(deal.swap)}  |  Fee: {signed(fee)}",
        f"This deal net: <b>{signed(this_net)}</b>",
    ]
    if summary and not still_open:
        lines.append(f"Whole position net: <b>{signed(total_net)}</b>")

    magic = summary["open_deal"].magic if summary else deal.magic
    lines += [
        "",
        f"Closed by: {esc(REASONS.get(deal.reason, 'Other (reason ' + str(deal.reason) + ')'))}",
        f"Opened by: {origin_label(0, magic, '') if summary else '-'}",
        f"Position ID: {deal.position_id}",
        f"Time: {local_str(deal.time)}",
        *filter(None, [delay_line(deal)]),
        "",
        account_line(),
    ]

    notify("\n".join(lines), kind="close", public=None if too_late(deal) else "\n".join(pub), magic=magic, symbol=sym,
           reply=deal.position_id)


def balance_event(deal):
    label = BALANCE_DEAL_LABELS.get(deal.type)
    if label is None or not on("notify_balance_changes"):
        return

    if deal.type == getattr(mt5, "DEAL_TYPE_BALANCE", -1):
        label = "Deposit" if deal.profit >= 0 else "Withdrawal"

    notify_balance(
        "\n".join([
            f"💰 <b>ACCOUNT {esc(label.upper())}</b>",
            "",
            f"Amount: {signed(deal.profit)}",
            f"Comment: {esc(deal.comment or '-')}",
            f"Time: {local_str(deal.time)}",
            "",
            account_line(),
        ])
    )


def process_deal(deal):
    if deal.type not in (mt5.DEAL_TYPE_BUY, mt5.DEAL_TYPE_SELL):
        balance_event(deal)
        return

    if deal.entry in (
        mt5.DEAL_ENTRY_OUT,
        mt5.DEAL_ENTRY_OUT_BY,
        mt5.DEAL_ENTRY_INOUT,
    ):
        if on("notify_closed", True):
            position_closed(deal)

    if deal.entry in (mt5.DEAL_ENTRY_IN, mt5.DEAL_ENTRY_INOUT):
        if on("notify_opened", True):
            position_opened(deal)


# =========================================================
# DEAL HISTORY MONITOR
# =========================================================

def process_new_deals():
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=LOOKBACK_DAYS)

    last_msc = STATE["last_deal_msc"]
    if last_msc:
        start = max(
            start,
            datetime.fromtimestamp(last_msc / 1000, tz=timezone.utc) - timedelta(days=1),
        )

    deals = mt5.history_deals_get(start, now + timedelta(days=2))

    if deals is None:
        if mt5.last_error()[0] != getattr(mt5, "RES_S_OK", 1):
            logger.warning("History deals unavailable: %s", mt5.last_error())
        return

    deals = sorted(deals, key=lambda d: (d.time_msc, d.ticket))
    processed = set(STATE["processed_deals"])

    for deal in deals:
        ticket = str(deal.ticket)
        if ticket in processed:
            continue

        max_age = cfg("monitor", "max_catchup_hours", 0)
        stale = bool(max_age) and time.time() - to_utc_ts(deal.time) > max_age * 3600

        if STATE["initialized"] and not stale:
            try:
                process_deal(deal)
            except Exception:
                logger.exception("Failed to process deal %s", ticket)

        processed.add(ticket)
        STATE["last_deal_msc"] = max(STATE["last_deal_msc"], deal.time_msc)

    STATE["processed_deals"] = sorted(processed, key=int)[-10000:]


# =========================================================
# POSITION CHANGE DETECTION (SL / TP)
# =========================================================

def monitor_positions():
    current = get_positions()
    previous = STATE["positions"]
    now = time.time()
    min_interval = cfg("monitor", "sl_tp_notify_min_interval_sec", 60)

    for key, p in current.items():
        old = previous.get(key)
        if old is None:
            continue

        p["_ts"] = dict(old.get("_ts", {}))

        if not on("notify_position_updates"):
            continue

        for field, label in (("sl", "STOP LOSS"), ("tp", "TAKE PROFIT")):
            old_v, new_v = old[field], p[field]

            if abs(old_v - new_v) <= 1e-7:
                continue

            if new_v == 0:
                event = "REMOVED"
            elif old_v == 0:
                event = "ADDED"
            else:
                event = "UPDATED"

            # Trailing-stop EAs change SL constantly: throttle those messages.
            if event == "UPDATED" and now - p["_ts"].get(field, 0) < min_interval:
                p[field] = old_v
                continue

            p["_ts"][field] = now
            sym = p["symbol"]

            notify("\n".join([
                f"✏️ <b>{label} {event}</b>",
                "",
                f"<b>{esc(sym)}</b>  {pos_direction(p['type'])}  {p['volume']} lot",
                f"Entry: {fmt_price(sym, p['price_open'])}",
                f"Previous: {fmt_price(sym, old_v) if old_v else 'Not Set'}",
                f"New: {fmt_price(sym, new_v) if new_v else 'Not Set'}",
                f"By: {order_origin(p['magic'])}",
                f"Position ID: {p['identifier']}",
                f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            ]), kind="modify", magic=p["magic"], symbol=sym, reply=p["identifier"],
                public=f"✏️ <b>SIGNAL UPDATE</b>\n<b>{esc(sym)}</b>  {pos_direction(p['type'])}\n{label.title()}: {fmt_price(sym, new_v) if new_v else 'removed'}")

    STATE["positions"] = current


# =========================================================
# PENDING ORDER MONITOR
# =========================================================

def get_historical_order(ticket):
    result = mt5.history_orders_get(ticket=ticket)
    if result:
        return result[-1]
    return None


def monitor_orders():
    current = get_orders()
    previous = STATE["orders"]
    notify_orders = on("notify_pending_orders")

    for ticket, order in current.items():
        old = previous.get(ticket)
        sym = order["symbol"]

        if old is None:
            if notify_orders:
                notify_order("\n".join([
                    "📌 <b>NEW PENDING ORDER</b>",
                    "",
                    f"<b>{esc(sym)}</b>  {get_order_type(order['type'])}  {order['volume']} lot",
                    f"Price: {fmt_price(sym, order['price'])}",
                    f"SL: {fmt_price(sym, order['sl']) if order['sl'] else 'Not Set'}",
                    f"TP: {fmt_price(sym, order['tp']) if order['tp'] else 'Not Set'}",
                    "",
                    f"Origin: {order_origin(order['magic'], order['comment'])}",
                    f"Ticket: {ticket}",
                ]))
            continue

        if not notify_orders:
            continue

        changed = []
        for field in ("price", "sl", "tp", "volume"):
            if abs(float(old[field]) - float(order[field])) > 1e-7:
                changed.append(f"{field.upper()}: {old[field]} → {order[field]}")

        if changed:
            notify_order(
                "✏️ <b>PENDING ORDER UPDATED</b>\n\n"
                f"<b>{esc(sym)}</b>  Ticket: {ticket}\n\n"
                + esc("\n".join(changed))
            )

    for ticket, old in previous.items():
        if ticket in current or not notify_orders:
            continue

        historical = get_historical_order(int(ticket))
        if historical is None:
            status = "UNKNOWN / NOT VERIFIED"
        else:
            status = ORDER_STATE_LABELS.get(historical.state, str(historical.state))

        sym = old["symbol"]
        notify_order("\n".join([
            "📋 <b>PENDING ORDER REMOVED</b>",
            "",
            f"<b>{esc(sym)}</b>  Ticket: {ticket}",
            f"Type: {get_order_type(old['type'])}",
            f"Price: {fmt_price(sym, old['price'])}",
            "",
            f"Final status: <b>{esc(status)}</b>",
        ]))

    STATE["orders"] = current


# =========================================================
# ALERT HELPERS
# =========================================================

def cooldown_ok(key, seconds):
    now = time.time()
    if now - STATE["alert_ts"].get(key, 0) >= seconds:
        STATE["alert_ts"][key] = now
        return True
    return False


def transition(key, bad, msg_bad, msg_ok=None, cooldown=0):
    """Alert when a condition starts, optionally repeat, and alert on recovery."""
    was = STATE["flags"].get(key, False)

    if bad and not was:
        STATE["flags"][key] = True
        STATE["alert_ts"][key] = time.time()
        if msg_bad:
            notify(msg_bad, kind="risk")

    elif bad and was:
        if cooldown and cooldown_ok(key, cooldown) and msg_bad:
            notify(msg_bad, kind="risk")

    elif not bad and was:
        STATE["flags"][key] = False
        if msg_ok:
            notify(msg_ok, kind="risk")


# =========================================================
# DAY STATISTICS + REPORT
# =========================================================

def compute_day_stats():
    start, end, label, _ = day_bounds()
    deals = deals_in_range(start, end)

    if deals is None:
        return None

    account = mt5.account_info()

    all_net = commission = swap = deposits = other_ops = balance_ops = 0.0
    out_by_pid = defaultdict(list)

    for d in deals:
        if d.type in (mt5.DEAL_TYPE_BUY, mt5.DEAL_TYPE_SELL):
            fee = getattr(d, "fee", 0.0)
            all_net += d.profit + d.commission + d.swap + fee
            commission += d.commission + fee
            swap += d.swap
            if d.entry in (
                mt5.DEAL_ENTRY_OUT,
                mt5.DEAL_ENTRY_OUT_BY,
                mt5.DEAL_ENTRY_INOUT,
            ):
                out_by_pid[d.position_id].append(d)

        elif d.type in BALANCE_AFFECTING:
            balance_ops += d.profit
            if d.type == getattr(mt5, "DEAL_TYPE_BALANCE", -1):
                deposits += d.profit
            else:
                other_ops += d.profit

    trades = []
    for pid, outs in out_by_pid.items():
        summary = position_summary(pid)
        fully_closed = not mt5.positions_get(ticket=pid)
        out_net = sum(deal_net(d) for d in outs)

        net = summary["net"] if (summary and fully_closed) else out_net
        first = summary["open_deal"] if summary else outs[0]

        pts = 0.0
        info = mt5.symbol_info(outs[0].symbol)
        if summary and info and info.point:
            last = max(outs, key=lambda d: d.time_msc)
            sign = 1 if first.type == mt5.DEAL_TYPE_BUY else -1
            pts = sign * (last.price - first.price) / info.point

        trades.append({
            "pid": pid,
            "symbol": outs[0].symbol,
            "magic": first.magic,
            "net": net,
            "points": pts,
        })

    wins = [t for t in trades if t["net"] > 0]
    losses = [t for t in trades if t["net"] < 0]
    gross_profit = sum(t["net"] for t in wins)
    gross_loss = sum(t["net"] for t in losses)

    by_symbol = defaultdict(lambda: [0, 0.0, 0])
    by_magic = defaultdict(lambda: [0, 0.0, 0])
    for t in trades:
        for bucket, key in ((by_symbol, t["symbol"]), (by_magic, t["magic"])):
            bucket[key][0] += 1
            bucket[key][1] += t["net"]
            bucket[key][2] += 1 if t["net"] > 0 else 0

    return {
        "label": label,
        "trades": trades,
        "points": sum(t["points"] for t in trades),
        "count": len(trades),
        "wins": len(wins),
        "losses": len(losses),
        "breakeven": len(trades) - len(wins) - len(losses),
        "net": sum(t["net"] for t in trades),
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "best": max(trades, key=lambda t: t["net"]) if trades else None,
        "worst": min(trades, key=lambda t: t["net"]) if trades else None,
        "commission": commission,
        "swap": swap,
        "deposits": deposits,
        "other_ops": other_ops,
        "by_symbol": dict(by_symbol),
        "by_magic": dict(by_magic),
        "start_balance": (
            account.balance - all_net - balance_ops if account else None
        ),
    }


def build_daily_report(stats):
    a = mt5.account_info()
    cur = a.currency if a else ""
    live = mt5.positions_get() or ()
    floating = sum(p.profit + p.swap for p in live)
    pending = len(mt5.orders_get() or ())

    lines = [f"📊 <b>DAILY REPORT — {stats['label']}</b>", ""]

    if stats["count"] == 0:
        lines.append("No closed trades today.")
    else:
        n = stats["count"]
        decided = stats["wins"] + stats["losses"]
        wr = stats["wins"] / decided * 100 if decided else 0.0
        pf = (
            f"{stats['gross_profit'] / abs(stats['gross_loss']):.2f}"
            if stats["gross_loss"] < 0
            else ("∞" if stats["gross_profit"] > 0 else "-")
        )
        avg_win = stats["gross_profit"] / stats["wins"] if stats["wins"] else 0.0
        avg_loss = stats["gross_loss"] / stats["losses"] if stats["losses"] else 0.0
        icon = "🟢" if stats["net"] >= 0 else "🔴"

        lines += [
            f"{icon} Net P/L: <b>{signed(stats['net'])} {cur}</b>",
            f"Closed trades: {n}  (✅ {stats['wins']} / ❌ {stats['losses']} / ➖ {stats['breakeven']})",
            f"Win rate: {wr:.1f}%   Profit factor: {pf}",
            f"Gross profit: {signed(stats['gross_profit'])}   Gross loss: {signed(stats['gross_loss'])}",
            f"Avg win: {signed(avg_win)}   Avg loss: {signed(avg_loss)}",
        ]
        b, w = stats["best"], stats["worst"]
        lines.append(f"Best: {signed(b['net'])} ({esc(b['symbol'])})   Worst: {signed(w['net'])} ({esc(w['symbol'])})")
        lines.append(f"Commission+fees: {signed(stats['commission'])}   Swap: {signed(stats['swap'])}")

        lines += ["", "<b>By symbol</b>"]
        for sym, (cnt, net, w_cnt) in sorted(
            stats["by_symbol"].items(), key=lambda kv: kv[1][1], reverse=True
        ):
            lines.append(f"{esc(sym)}: {cnt} trades, {signed(net)}  (WR {w_cnt / cnt * 100:.0f}%)")

        lines += ["", "<b>By EA</b>"]
        for magic, (cnt, net, w_cnt) in sorted(
            stats["by_magic"].items(), key=lambda kv: kv[1][1], reverse=True
        ):
            lines.append(f"{esc(ea_name(magic))}: {cnt} trades, {signed(net)}  (WR {w_cnt / cnt * 100:.0f}%)")

    if stats["deposits"] or stats["other_ops"]:
        lines += [
            "",
            f"Deposits/withdrawals: {signed(stats['deposits'])}   Other ops: {signed(stats['other_ops'])}",
        ]

    lines.append("")
    if a:
        start_bal = stats["start_balance"]
        if start_bal is not None:
            lines.append(f"Balance: {money(start_bal)} → {money(a.balance)} {cur}")
        else:
            lines.append(f"Balance: {money(a.balance)} {cur}")
        lines.append(f"Equity: {money(a.equity)}   Floating P/L: {signed(floating)}")
        if a.margin > 0:
            lines.append(f"Margin level: {a.margin_level:.0f}%")
    lines.append(f"Open positions: {len(live)}   Pending orders: {pending}")

    return "\n".join(lines)


def trade_ok(d, t):
    f = d.get("filters") or {}
    if f.get("magics") and int(t["magic"]) not in [int(m) for m in f["magics"]]:
        return False
    if f.get("only_ea") and int(t["magic"]) == 0:
        return False
    if f.get("symbols") and t["symbol"] not in f["symbols"]:
        return False
    return True


def build_public_report(stats, dest=None):
    """Channel-safe recap: points only, no money, no balance, no lot sizes."""
    trades = [t for t in stats.get("trades", []) if dest is None or trade_ok(dest, t)]
    lines = [f"📊 <b>DAILY SIGNALS RECAP — {stats['label']}</b>", ""]

    if not trades:
        lines.append("No closed signals today.")
    else:
        wins = [t for t in trades if t["points"] > 0]
        losses = [t for t in trades if t["points"] < 0]
        decided = len(wins) + len(losses)
        total = sum(t["points"] for t in trades)
        icon = "🟢" if total >= 0 else "🔴"
        lines += [
            f"{icon} Net result: <b>{total:+.0f} points</b>",
            f"Signals closed: {len(trades)}   (✅ {len(wins)} / ❌ {len(losses)})",
            f"Win rate: {len(wins) / decided * 100:.0f}%" if decided else "Win rate: -",
        ]
        best = max(trades, key=lambda t: t["points"])
        worst = min(trades, key=lambda t: t["points"])
        lines.append(f"Best: {best['points']:+.0f} ({esc(best['symbol'])})   Worst: {worst['points']:+.0f} ({esc(worst['symbol'])})")

        per = defaultdict(lambda: [0, 0.0])
        for t in trades:
            per[t["symbol"]][0] += 1
            per[t["symbol"]][1] += t["points"]
        lines += ["", "<b>By symbol</b>"]
        for sym, (n, pts) in sorted(per.items(), key=lambda kv: kv[1][1], reverse=True):
            lines.append(f"{esc(sym)}: {n} signals, {pts:+.0f} points")

    lines += ["", "<i>Past results do not guarantee future performance.</i>"]
    return "\n".join(lines)


def send_reports(stats, only=None):
    for d in get_destinations():
        if not d.get("enabled", True):
            continue
        mode = d.get("report", "full" if d.get("mode", "full") == "full" else "public")
        if mode == "none" or (only and d["name"] != only):
            continue
        text = tag(build_daily_report(stats)) if mode == "full" else build_public_report(stats, d)
        enqueue(d["chat_id"], text, False, d.get("bot"))
    if PANEL:
        try:
            PANEL.log_event("report", "Daily report sent")
        except Exception:
            pass
    flush_outbox()


def check_daily_report():
    if not REPORT.get("enabled", True):
        return

    _, _, label, n = day_bounds()

    if STATE.get("last_report_date") == label:
        return
    if n.strftime("%H:%M") < str(REPORT.get("time", "23:55")):
        return
    if not cooldown_ok("report_try", 60):
        return

    stats = compute_day_stats()
    if stats is None:
        return  # will retry in a minute

    if stats["count"] == 0 and not REPORT.get("send_if_no_trades", False):
        STATE["last_report_date"] = label
        return

    send_reports(stats)
    STATE["last_report_date"] = label


# =========================================================
# RISK / HEALTH MONITOR
# =========================================================

def ensure_day_baseline(account):
    _, _, label, _ = day_bounds()
    day = STATE.get("day") or {}

    if day.get("date") == label:
        return day

    stats = compute_day_stats()
    start_bal = (
        stats["start_balance"]
        if stats and stats.get("start_balance") is not None
        else account.balance
    )

    STATE["day"] = {"date": label, "balance": start_bal}

    for k in [k for k in STATE["flags"] if k.startswith("daily_")]:
        STATE["flags"].pop(k, None)

    return STATE["day"]


def monitor_risk():
    account = mt5.account_info()
    terminal = mt5.terminal_info()

    if account is None or terminal is None:
        return

    cur = account.currency
    A = RuntimeAlerts()

    previous = STATE.get("account") or {}
    if previous.get("login") and previous["login"] != account.login:
        notify_risk(
            "⚠️ <b>ACCOUNT CHANGED</b>\n"
            f"Old: {previous['login']}\nNew: {account.login}\n"
            "Monitor state was re-baselined."
        )
        initialize_baseline(force=True)
        return

    STATE["account"] = {
        "login": account.login,
        "balance": account.balance,
        "equity": account.equity,
        "currency": cur,
    }

    day = ensure_day_baseline(account)
    acct = account_line()

    # ---- connection / permissions ----
    if A.get("broker_connection", True):
        transition(
            "broker_conn",
            not terminal.connected,
            "🔴 <b>MT5 TERMINAL DISCONNECTED FROM BROKER</b>\nEAs cannot trade until it reconnects.",
            "🟢 <b>MT5 reconnected to the broker</b>",
            cooldown=1800,
        )

    if A.get("autotrading", True):
        transition(
            "autotrading",
            not terminal.trade_allowed,
            "⚠️ <b>ALGO TRADING IS OFF</b>\nThe AutoTrading button in MT5 is disabled - EAs cannot open trades.",
            "✅ <b>Algo trading is ON again</b>",
            cooldown=3600,
        )
        transition(
            "account_trade",
            (not account.trade_allowed) or (not account.trade_expert),
            "⚠️ <b>TRADING / EAs NOT ALLOWED ON THIS ACCOUNT</b>\nThe broker or account settings block trading.",
            "✅ <b>Trading is allowed on the account again</b>",
            cooldown=3600,
        )

    # ---- margin level ----
    warn = A.get("margin_level_warn", 150)
    crit = A.get("margin_level_critical", 100)
    if warn:
        if account.margin > 0:
            ml = account.margin_level
            transition(
                "margin_warn",
                ml < warn,
                f"⚠️ <b>LOW MARGIN LEVEL</b>\nMargin level: {ml:.1f}% (alert below {warn}%)\n{acct}",
                f"✅ Margin level recovered: {ml:.1f}%",
                cooldown=1800,
            )
            if crit:
                transition(
                    "margin_crit",
                    ml < crit,
                    f"🚨 <b>CRITICAL MARGIN LEVEL</b>\nMargin level: {ml:.1f}% (alert below {crit}%)\nStop-out risk!\n{acct}",
                    None,
                    cooldown=600,
                )
        else:
            transition("margin_warn", False, "", None)
            transition("margin_crit", False, "", None)

    # ---- daily loss / profit (equity based, includes floating) ----
    day_balance = day.get("balance") or account.balance
    day_pnl = account.equity - day_balance
    day_pct = day_pnl / day_balance * 100 if day_balance else 0.0

    loss_pct = A.get("daily_loss_percent", 0)
    loss_amt = A.get("daily_loss_amount", 0)
    if loss_pct:
        transition(
            "daily_loss_pct",
            day_pct <= -abs(loss_pct),
            f"🚨 <b>DAILY LOSS LIMIT REACHED</b>\nToday: {signed(day_pnl)} {cur} ({day_pct:+.2f}%)\nLimit: -{abs(loss_pct)}%\n{acct}",
            None,
        )
    if loss_amt:
        transition(
            "daily_loss_amt",
            day_pnl <= -abs(loss_amt),
            f"🚨 <b>DAILY LOSS LIMIT REACHED</b>\nToday: {signed(day_pnl)} {cur}\nLimit: -{money(abs(loss_amt))}\n{acct}",
            None,
        )

    target = A.get("daily_profit_target", 0)
    if target:
        transition(
            "daily_profit",
            day_pnl >= target,
            f"🏆 <b>DAILY PROFIT TARGET REACHED</b>\nToday: {signed(day_pnl)} {cur} ({day_pct:+.2f}%)",
            None,
        )

    # ---- positions ----
    live = mt5.positions_get() or ()

    floating = sum(p.profit + p.swap for p in live)
    fl_pct = A.get("floating_loss_percent", 0)
    if fl_pct and account.balance > 0:
        pct = -floating / account.balance * 100
        transition(
            "floating_loss",
            floating < 0 and pct >= fl_pct,
            f"⚠️ <b>HIGH FLOATING LOSS</b>\nFloating P/L: {signed(floating)} {cur} ({pct:.2f}% of balance)\n{acct}",
            f"✅ Floating loss is back below {fl_pct}%",
            cooldown=1800,
        )

    max_pos = A.get("max_open_positions", 0)
    if max_pos:
        transition(
            "max_pos",
            len(live) > max_pos,
            f"⚠️ <b>TOO MANY OPEN POSITIONS</b>\n{len(live)} open (limit {max_pos})",
            f"✅ Open positions back to {len(live)}",
        )

    live_ids = {p.identifier for p in live}
    warned = STATE["warned"]
    warned["nosl"] = [i for i in warned["nosl"] if i in live_ids]
    warned["loss"] = [i for i in warned["loss"] if i in live_ids]

    grace = A.get("no_sl_grace_seconds", 90)
    pos_loss = A.get("position_loss_amount", 0)

    for p in live:
        age = time.time() - to_utc_ts(p.time)
        tag = (
            f"<b>{esc(p.symbol)}</b>  {pos_direction(p.type)}  {p.volume} lot\n"
            f"By: {order_origin(p.magic)}\nPosition ID: {p.identifier}"
        )

        if (
            A.get("warn_no_sl", True)
            and p.sl == 0
            and age > grace
            and p.identifier not in warned["nosl"]
        ):
            warned["nosl"].append(p.identifier)
            notify_risk(f"⚠️ <b>POSITION WITHOUT STOP LOSS</b>\n\n{tag}\nFloating: {signed(p.profit)} {cur}")

        if (
            pos_loss
            and p.profit <= -abs(pos_loss)
            and p.identifier not in warned["loss"]
        ):
            warned["loss"].append(p.identifier)
            notify_risk(
                f"🔻 <b>POSITION FLOATING LOSS</b>\n\n{tag}\n"
                f"Floating: {signed(p.profit)} {cur} (alert at -{money(abs(pos_loss))})"
            )


# =========================================================
# TELEGRAM COMMANDS  (/status /positions /report /help)
# =========================================================

_last_cmd_poll = 0.0


def build_status():
    a = mt5.account_info()
    t = mt5.terminal_info()
    if not a or not t:
        return "MT5 is not available right now."

    live = mt5.positions_get() or ()
    floating = sum(p.profit + p.swap for p in live)
    stats = compute_day_stats()

    lines = [
        "ℹ️ <b>STATUS</b>",
        "",
        f"Account: {a.login} @ {esc(a.server)}",
        f"Broker connection: {'✅' if t.connected else '🔴'}   Algo trading: {'✅' if t.trade_allowed else '🔴'}",
        f"Balance: {money(a.balance)}   Equity: {money(a.equity)} {a.currency}",
        f"Floating P/L: {signed(floating)}",
    ]
    if a.margin > 0:
        lines.append(f"Margin level: {a.margin_level:.0f}%")
    lines.append(f"Open positions: {len(live)}   Pending orders: {len(mt5.orders_get() or ())}")
    if stats:
        lines.append(f"Today: {stats['count']} closed trades, net {signed(stats['net'])}")
    lines.append(f"Server UTC offset: {server_offset() / 3600:+.1f} h")
    return "\n".join(lines)


def build_positions_text():
    live = mt5.positions_get() or ()
    if not live:
        return "No open positions."

    lines = ["📂 <b>OPEN POSITIONS</b>", ""]
    for p in live:
        s = p.symbol
        lines.append(
            f"<b>{esc(s)}</b> {pos_direction(p.type)} {p.volume}  "
            f"@ {fmt_price(s, p.price_open)}\n"
            f"SL {fmt_price(s, p.sl) if p.sl else '-'} | TP {fmt_price(s, p.tp) if p.tp else '-'} | "
            f"P/L {signed(p.profit)}  [{esc(ea_name(p.magic))}]"
        )
    return "\n".join(lines[:2]) + "\n".join(["\n\n".join(lines[2:])])


def handle_command(text, chat_id):
    parts = text.strip().split()
    cmd = parts[0].split("@")[0].lower()
    args = parts[1:]

    def reply(t):
        enqueue(chat_id, tag(t))
        flush_outbox()

    if cmd in ("/status", "/start"):
        reply(build_status())
    elif cmd == "/positions":
        reply(build_positions_text())
    elif cmd == "/report":
        stats = compute_day_stats()
        reply(build_daily_report(stats) if stats else "Could not read deal history.")
    elif cmd in ("/pause", "/resume", "/panic") and PANEL:
        if cmd == "/panic" and args[:1] != ["confirm"]:
            reply("Send <code>/panic confirm</code> to close everything and pause all EAs.")
            return
        name = {"/pause": "pause_all", "/resume": "resume_all", "/panic": "panic"}[cmd]
        r = PANEL.do_action(name, {}, "telegram")
        reply(esc(r.get("msg", "")))
    elif cmd == "/help":
        reply("Commands:\n/status\n/positions\n/report\n/pause\n/resume\n/panic confirm")


def poll_commands():
    global _last_cmd_poll

    if not COMMANDS.get("enabled", True):
        return
    if time.time() - _last_cmd_poll < COMMANDS.get("poll_interval", 3):
        return
    _last_cmd_poll = time.time()

    payload = {"timeout": 0, "allowed_updates": ["message"]}
    if STATE["tg_offset"]:
        payload["offset"] = STATE["tg_offset"]

    try:
        r = requests.post(
            f"{API}/getUpdates", json=payload, proxies=PROXIES, timeout=(5, 10)
        )
        data = r.json()
    except Exception:
        return

    if not data.get("ok"):
        if cooldown_ok("getupdates_err", 3600):
            logger.warning("getUpdates failed: %s", data.get("description"))
        return

    updates = data.get("result", [])
    if not updates:
        return

    # first run: skip anything that was sent while the monitor was off
    if not STATE["cmd_primed"]:
        STATE["tg_offset"] = updates[-1]["update_id"] + 1
        STATE["cmd_primed"] = True
        return

    for u in updates:
        STATE["tg_offset"] = u["update_id"] + 1
        msg = u.get("message") or {}
        if str(msg.get("chat", {}).get("id")) not in admin_chat_ids():
            continue
        text = msg.get("text", "")
        if text.startswith("/"):
            try:
                handle_command(text, str(msg["chat"]["id"]))
            except Exception:
                logger.exception("Command failed: %s", text)


# =========================================================
# INITIALIZATION
# =========================================================

def initialize_baseline(force=False):
    STATE["positions"] = get_positions()
    STATE["orders"] = get_orders()

    account = mt5.account_info()
    if account:
        STATE["account"] = {
            "login": account.login,
            "balance": account.balance,
            "equity": account.equity,
            "currency": account.currency,
        }

    now = datetime.now(timezone.utc)
    deals = mt5.history_deals_get(
        now - timedelta(days=LOOKBACK_DAYS), now + timedelta(days=2)
    )

    if deals is None:
        if mt5.last_error()[0] == getattr(mt5, "RES_S_OK", 1):
            deals = ()
        else:
            raise RuntimeError("Cannot initialize deal history")

    if force or not STATE["initialized"]:
        # Existing history is only a baseline - old trades are not reported.
        STATE["processed_deals"] = [str(d.ticket) for d in deals]
        STATE["last_deal_msc"] = max([d.time_msc for d in deals], default=0)

    if force:
        STATE["day"] = {}
        STATE["flags"] = {}
        STATE["warned"] = {"nosl": [], "loss": []}

    STATE["initialized"] = True
    save_state()


def startup_message():
    account = mt5.account_info()
    lines = [
        "🟢 <b>MT5 MONITOR STARTED</b>",
        "",
        f"Account: {account.login}",
        f"Server: {esc(account.server)}",
        f"Balance: {money(account.balance)}   Equity: {money(account.equity)} {account.currency}",
        f"Polling: {POLL_INTERVAL}s   Server UTC offset: {server_offset() / 3600:+.1f} h",
    ]

    if on("notify_existing_positions_on_start", True):
        live = mt5.positions_get() or ()
        orders = mt5.orders_get() or ()
        lines += ["", f"Open positions: {len(live)}   Pending orders: {len(orders)}"]
        for p in live:
            lines.append(
                f"• {esc(p.symbol)} {pos_direction(p.type)} {p.volume} @ "
                f"{fmt_price(p.symbol, p.price_open)}  P/L {signed(p.profit)}  [{esc(ea_name(p.magic))}]"
            )

    lines += ["", "Status: Monitoring Active", "Commands: /status /positions /report"]
    notify("\n".join(lines))


# =========================================================
# MAIN LOOP
# =========================================================

def heartbeat():
    hours = float(cfg("monitor", "heartbeat_hours", 0) or 0)
    if hours <= 0:
        return
    if time.time() - STATE.get("last_heartbeat", 0) < hours * 3600:
        return
    STATE["last_heartbeat"] = time.time()
    notify("💓 <b>Monitor is alive</b>\n" + account_line(), silent=True)


def cycle():
    """One monitoring pass. Returns the delay before the next one."""
    flush_outbox()

    if not ensure_connection():
        if on("notify_connection") and STATE["connection"] is not False:
            notify("🔴 <b>MT5 CONNECTION LOST</b>\nThe monitor cannot reach the MT5 terminal.")
        STATE["connection"] = False
        flush_outbox()
        save_state()
        return 5

    if STATE["connection"] is False:
        if on("notify_connection"):
            notify("🟢 <b>MT5 CONNECTION RESTORED</b>")
        update_server_offset(force=True)

    STATE["connection"] = True

    update_server_offset()
    process_new_deals()
    monitor_positions()
    monitor_orders()
    monitor_risk()
    check_daily_report()

    if PANEL:
        try:
            PANEL.tick()
        except Exception:
            logger.exception("Panel tick failed")

    poll_commands()
    heartbeat()

    flush_outbox()
    save_state()
    return POLL_INTERVAL


def main():
    if not connect_mt5():
        raise RuntimeError("MT5 connection failed")

    update_server_offset(force=True)
    initialize_baseline()

    STATE["last_heartbeat"] = time.time()

    if PANEL:
        try:
            PANEL.start(sys.modules[__name__])
        except Exception:
            logger.exception("Web panel could not start - continuing without it")

    if on("notify_startup"):
        startup_message()

    logger.info("Monitoring started")

    delay = POLL_INTERVAL
    while True:
        try:
            with MT5_LOCK:
                delay = cycle()

        except Exception as e:
            logger.exception("Monitoring cycle failed")
            delay = POLL_INTERVAL
            try:
                if cooldown_ok("cycle_error", 600):
                    notify(f"⚠️ <b>MONITOR ERROR</b>\n<code>{esc(repr(e))[:500]}</code>")
                save_state()
            except Exception:
                pass

        time.sleep(delay)


if __name__ == "__main__":
    exit_message = None

    try:
        main()

    except KeyboardInterrupt:
        logger.info("Monitor stopped by user")
        exit_message = "⚠️ <b>MT5 MONITOR STOPPED BY USER</b>"

    except Exception as e:
        logger.exception("Monitor crashed")
        exit_message = f"🔴 <b>MT5 MONITOR CRASHED</b>\n<code>{esc(repr(e))[:500]}</code>"

    finally:
        try:
            if exit_message:
                notify(exit_message)
            flush_outbox()
            save_state()
        except Exception:
            pass
        mt5.shutdown()
