# -*- coding: utf-8 -*-
"""
Trade analytics for MT5 Hub.

build_trades()  turns MT5 deal history into one row per fully closed position.
compute()       turns rows into everything the Analytics tab shows: heatmaps
                (weekday x hour, symbol x hour, EA x hour, calendar), breakdowns,
                equity/drawdown, streaks, EA health and plain-language insights.
No MT5 import here: pure functions, easy to test.
"""

import math
from collections import defaultdict
from datetime import datetime, timezone

WD = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
SESSIONS = [("Asia", 0, 9), ("London", 7, 16), ("New York", 12, 21)]      # UTC hours
DURATIONS = [("<1m", 60), ("1-5m", 300), ("5-15m", 900), ("15-60m", 3600),
             ("1-4h", 14400), ("4-24h", 86400), ("1d+", None)]


# =========================================================
# DEALS -> CLOSED POSITIONS
# =========================================================

def build_trades(mt5, core, deals):
    by_pos = defaultdict(list)
    for d in deals:
        if d.type in (mt5.DEAL_TYPE_BUY, mt5.DEAL_TYPE_SELL):
            by_pos[d.position_id].append(d)

    out = []
    for pid, ds in by_pos.items():
        if not any(d.entry in (mt5.DEAL_ENTRY_OUT, mt5.DEAL_ENTRY_OUT_BY, mt5.DEAL_ENTRY_INOUT) for d in ds):
            continue                                    # still open
        if not any(d.entry in (mt5.DEAL_ENTRY_IN, mt5.DEAL_ENTRY_INOUT) for d in ds):
            full = mt5.history_deals_get(position=pid)  # opened before the queried window
            if full:
                ds = [d for d in full if d.type in (mt5.DEAL_TYPE_BUY, mt5.DEAL_TYPE_SELL)]

        ds.sort(key=lambda d: (d.time_msc, d.ticket))
        ins = [d for d in ds if d.entry in (mt5.DEAL_ENTRY_IN, mt5.DEAL_ENTRY_INOUT)]
        outs = [d for d in ds if d.entry in (mt5.DEAL_ENTRY_OUT, mt5.DEAL_ENTRY_OUT_BY, mt5.DEAL_ENTRY_INOUT)]
        if not ins or not outs:
            continue

        vin = sum(d.volume for d in ins)
        vout = sum(d.volume for d in outs)
        if vin <= 0 or vout <= 0:
            continue
        if vout < vin - 1e-9 and mt5.positions_get(ticket=pid):
            continue                                    # only partially closed so far

        first, last = ins[0], outs[-1]
        buy = first.type == mt5.DEAL_TYPE_BUY
        open_price = sum(d.price * d.volume for d in ins) / vin
        close_price = sum(d.price * d.volume for d in outs) / vout
        info = mt5.symbol_info(first.symbol)
        point = info.point if info and info.point else 0
        points = (close_price - open_price) / point * (1 if buy else -1) if point else 0.0

        out.append({
            "pid": pid, "symbol": first.symbol, "side": "BUY" if buy else "SELL", "volume": vin,
            "open_ts": int(core.to_utc_ts(first.time)), "close_ts": int(core.to_utc_ts(last.time)),
            "open_price": open_price, "close_price": close_price,
            "gross": sum(d.profit for d in ds),
            "costs": sum(d.commission + d.swap + getattr(d, "fee", 0.0) for d in ds),
            "points": points, "magic": first.magic, "comment": (first.comment or "")[:40],
        })
    return out


# =========================================================
# HELPERS
# =========================================================

def _a():
    return {"n": 0, "net": 0.0, "w": 0, "gp": 0.0, "gl": 0.0, "pts": 0.0}


def _add(a, net, pts=0.0):
    a["n"] += 1
    a["net"] += net
    a["pts"] += pts
    if net > 0:
        a["w"] += 1
        a["gp"] += net
    elif net < 0:
        a["gl"] += net


def _out(a):
    return {"n": a["n"], "net": round(a["net"], 2), "w": a["w"], "gp": round(a["gp"], 2),
            "gl": round(a["gl"], 2), "pts": round(a["pts"], 1)}


def _pf(a):
    return a["gp"] / abs(a["gl"]) if a["gl"] < 0 else (None if a["gp"] > 0 else 0.0)


def _pct(vals, q):
    if not vals:
        return 0.0
    s = sorted(vals)
    k = (len(s) - 1) * q
    lo, hi = int(math.floor(k)), int(math.ceil(k))
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def _hl(h):
    return f"{h:02d}:00-{(h + 1) % 24:02d}:00"


def _fm(v):
    return f"{v:+,.2f}"


# =========================================================
# COMPUTE
# =========================================================

def compute(rows, p, ea_of, label_of, now_ts):
    tz = float(p.get("tz", 0))
    off = tz * 3600
    basis = p.get("basis", "open")
    min_n = max(1, int(p.get("min_n", 8)))

    all_symbols, all_eas = set(), set()
    T = []
    for r in rows:
        ea = ea_of(r["acc"], r["magic"])
        all_symbols.add(r["symbol"])
        all_eas.add(ea)
        if p.get("symbol") and r["symbol"] != p["symbol"]:
            continue
        if p.get("ea") and ea != p["ea"]:
            continue
        if p.get("side") and r["side"] != p["side"]:
            continue
        net = r["gross"] + r["costs"]
        ts = r["open_ts"] if basis == "open" else r["close_ts"]
        dt = datetime.fromtimestamp(ts + off, tz=timezone.utc)
        cd = datetime.fromtimestamp(r["close_ts"] + off, tz=timezone.utc)
        T.append({
            "acc": r["acc"], "pid": r["pid"], "symbol": r["symbol"], "side": r["side"], "vol": r["volume"],
            "net": net, "pts": r["points"], "ea": ea, "magic": r["magic"],
            "open_ts": r["open_ts"], "close_ts": r["close_ts"], "dur": max(0, r["close_ts"] - r["open_ts"]),
            "wd": dt.weekday(), "h": dt.hour, "uh": datetime.fromtimestamp(ts, tz=timezone.utc).hour,
            "month": dt.strftime("%Y-%m"), "cdate": cd.strftime("%Y-%m-%d"),
            "o": r["open_price"], "c": r["close_price"],
        })

    meta = {"symbols": sorted(all_symbols), "eas": sorted(all_eas), "tz": tz, "basis": basis, "min_n": min_n}
    n = len(T)
    if n == 0:
        return {"empty": True, "meta": meta}

    T.sort(key=lambda t: (t["close_ts"], t["pid"]))

    # ---- summary -------------------------------------------------
    tot = _a()
    for t in T:
        _add(tot, t["net"], t["pts"])
    wins = [t["net"] for t in T if t["net"] > 0]
    losses = [t["net"] for t in T if t["net"] < 0]
    avg_win = sum(wins) / len(wins) if wins else 0.0
    avg_loss = sum(losses) / len(losses) if losses else 0.0
    win_dur = [t["dur"] for t in T if t["net"] > 0]
    loss_dur = [t["dur"] for t in T if t["net"] < 0]

    cum = peak = max_dd = 0.0
    curve = []
    for t in T:
        cum += t["net"]
        peak = max(peak, cum)
        dd = peak - cum
        max_dd = max(max_dd, dd)
        curve.append([t["close_ts"], round(cum, 2), round(dd, 2)])
    cur_dd = peak - cum
    step = max(1, len(curve) // 400)
    curve_out = curve[::step] + ([curve[-1]] if (len(curve) - 1) % step else [])

    best_streak = worst_streak = run = 0
    cur_streak = 0
    for t in T:
        if t["net"] > 0:
            run = run + 1 if run > 0 else 1
        elif t["net"] < 0:
            run = run - 1 if run < 0 else -1
        else:
            run = 0
        best_streak = max(best_streak, run)
        worst_streak = min(worst_streak, run)
        cur_streak = run

    daily = defaultdict(lambda: [0.0, 0])
    for t in T:
        d = daily[t["cdate"]]
        d[0] += t["net"]
        d[1] += 1
    day_nets = [v[0] for v in daily.values()]
    first_ts, last_ts = T[0]["close_ts"], T[-1]["close_ts"]
    span_days = max(1.0, (last_ts - first_ts) / 86400)

    summary = {
        "trades": n, "net": round(tot["net"], 2), "win_rate": round(tot["w"] / n * 100, 1),
        "wins": tot["w"], "losses": len(losses), "gp": round(tot["gp"], 2), "gl": round(tot["gl"], 2),
        "pf": None if _pf(tot) is None else round(_pf(tot), 2), "pf_inf": _pf(tot) is None,
        "expectancy": round(tot["net"] / n, 2), "avg_win": round(avg_win, 2), "avg_loss": round(avg_loss, 2),
        "payoff": round(avg_win / abs(avg_loss), 2) if avg_loss else None,
        "best": round(max(t["net"] for t in T), 2), "worst": round(min(t["net"] for t in T), 2),
        "max_dd": round(max_dd, 2), "cur_dd": round(cur_dd, 2),
        "recovery": round(tot["net"] / max_dd, 2) if max_dd > 0 else None,
        "best_streak": best_streak, "worst_streak": -worst_streak, "cur_streak": cur_streak,
        "avg_dur_win": round(sum(win_dur) / len(win_dur)) if win_dur else 0,
        "avg_dur_loss": round(sum(loss_dur) / len(loss_dur)) if loss_dur else 0,
        "avg_lot": round(sum(t["vol"] for t in T) / n, 2), "points": round(tot["pts"], 0),
        "costs": round(sum(r["costs"] for r in rows), 2),
        "days": len(daily), "trades_per_day": round(n / max(1, len(daily)), 1),
        "win_days": round(sum(1 for v in day_nets if v > 0) / len(day_nets) * 100, 0),
        "best_day": round(max(day_nets), 2), "worst_day": round(min(day_nets), 2),
        "avg_day": round(sum(day_nets) / len(day_nets), 2),
        "from": first_ts, "to": last_ts, "span_days": round(span_days, 1),
    }

    # ---- time buckets ---------------------------------------------
    hour = [_a() for _ in range(24)]
    weekday = [_a() for _ in range(7)]
    hm = [[_a() for _ in range(24)] for _ in range(7)]
    sym_h, ea_h = {}, {}
    sess = {name: _a() for name, _, _ in SESSIONS}
    sess["Off-hours"] = _a()
    month = defaultdict(_a)
    dur_b = {lab: _a() for lab, _ in DURATIONS}
    sides = {"BUY": _a(), "SELL": _a()}
    syms, eas = defaultdict(_a), defaultdict(_a)

    for t in T:
        _add(hour[t["h"]], t["net"])
        _add(weekday[t["wd"]], t["net"])
        _add(hm[t["wd"]][t["h"]], t["net"])
        _add(sym_h.setdefault(t["symbol"], [_a() for _ in range(24)])[t["h"]], t["net"])
        _add(ea_h.setdefault(t["ea"], [_a() for _ in range(24)])[t["h"]], t["net"])
        hit = [name for name, a, b in SESSIONS if a <= t["uh"] < b]
        for name in (hit or ["Off-hours"]):
            _add(sess[name], t["net"])
        _add(month[t["month"]], t["net"])
        for lab, lim in DURATIONS:
            if lim is None or t["dur"] < lim:
                _add(dur_b[lab], t["net"])
                break
        _add(sides[t["side"]], t["net"])
        _add(syms[t["symbol"]], t["net"], t["pts"])
        _add(eas[t["ea"]], t["net"], t["pts"])

    # lot buckets
    vols = sorted(set(round(t["vol"], 2) for t in T))
    lots = []
    if len(vols) <= 6:
        edges = [(v, v) for v in vols]
    else:
        allv = sorted(t["vol"] for t in T)
        cuts = [_pct(allv, q) for q in (0, .2, .4, .6, .8, 1)]
        edges = [(cuts[i], cuts[i + 1]) for i in range(5)]
    for i, (lo, hi) in enumerate(edges):
        a = _a()
        for t in T:
            if (lo <= t["vol"] <= hi if len(vols) > 6 and i == len(edges) - 1 else
                    (lo <= t["vol"] < hi if len(vols) > 6 else round(t["vol"], 2) == lo)):
                _add(a, t["net"])
        lots.append(dict(_out(a), l=f"{lo:g}" if lo == hi else f"{lo:g}-{hi:g}"))

    # histogram of trade results
    nets = [t["net"] for t in T]
    hist = None
    if n >= 5:
        lo, hi = _pct(nets, .02), _pct(nets, .98)
        if hi <= lo:
            lo, hi = lo - 1, hi + 1
        bins = 20
        w = (hi - lo) / bins
        counts = [0] * bins
        for v in nets:
            counts[min(bins - 1, max(0, int((v - lo) / w)))] += 1
        hist = {"lo": round(lo, 2), "w": round(w, 4), "counts": counts}

    # ---- heatmaps ---------------------------------------------------
    top_sym = sorted(sym_h, key=lambda s: -sum(a["n"] for a in sym_h[s]))[:10]
    top_ea = sorted(ea_h, key=lambda s: -sum(a["n"] for a in ea_h[s]))[:10]

    # ---- tables -----------------------------------------------------
    def table(d):
        rows_ = []
        for k, a in d.items():
            pf = _pf(a)
            rows_.append(dict(_out(a), l=k, pf=None if pf is None else round(pf, 2), pf_inf=pf is None,
                              avg=round(a["net"] / a["n"], 2) if a["n"] else 0))
        return sorted(rows_, key=lambda r: -r["net"])

    # ---- EA health: last 30 trades vs the whole history ---------------
    health = []
    groups = defaultdict(list)
    for t in T:
        groups[(t["acc"], t["ea"])].append(t)
    for (acc, ea), g in groups.items():
        allv, rec = _a(), _a()
        for t in g:
            _add(allv, t["net"])
        for t in g[-30:]:
            _add(rec, t["net"])
        pa, pr = _pf(allv), _pf(rec)
        status, note = "ok", "in line with its history"
        if allv["n"] < 15:
            status, note = "new", "not enough trades yet"
        elif rec["net"] < 0 and allv["net"] > 0:
            status, note = "bad", "last 30 trades are negative while the overall result is positive"
        elif pa is not None and pr is not None and pa > 0 and pr < 0.6 * pa:
            status, note = "warn", "recent profit factor fell well below its average"
        elif rec["w"] / max(1, rec["n"]) < (allv["w"] / allv["n"]) - 0.15:
            status, note = "warn", "recent win rate dropped more than 15 points"
        health.append({"acc": label_of(acc), "ea": ea, "status": status, "note": note, "n": allv["n"],
                       "net": round(allv["net"], 2), "pf": None if pa is None else round(pa, 2),
                       "wr": round(allv["w"] / allv["n"] * 100, 0),
                       "rec_net": round(rec["net"], 2), "rec_pf": None if pr is None else round(pr, 2),
                       "rec_wr": round(rec["w"] / max(1, rec["n"]) * 100, 0)})
    health.sort(key=lambda h: {"bad": 0, "warn": 1, "ok": 2, "new": 3}[h["status"]])

    # ---- per account ----------------------------------------------------
    per_acc = defaultdict(_a)
    acc_dd = {}
    for t in T:
        _add(per_acc[t["acc"]], t["net"])
    for acc in per_acc:
        c = pk = dd = 0.0
        for t in T:
            if t["acc"] == acc:
                c += t["net"]
                pk = max(pk, c)
                dd = max(dd, pk - c)
        acc_dd[acc] = dd
    accounts = []
    for acc, a in per_acc.items():
        pf = _pf(a)
        accounts.append(dict(_out(a), l=label_of(acc), pf=None if pf is None else round(pf, 2),
                             pf_inf=pf is None, max_dd=round(acc_dd[acc], 2)))
    accounts.sort(key=lambda r: -r["net"])

    # ---- insights -----------------------------------------------------------
    ins = []

    def stat(a):
        return f"{_fm(a['net'])}, WR {a['w'] / a['n'] * 100:.0f}%, {a['n']} trades"

    if n < 50:
        ins.append(["info", f"Only {n} trades in this selection - treat the patterns below as hints, not conclusions."])

    hrs = [(h, a) for h, a in enumerate(hour) if a["n"] >= min_n]
    if len(hrs) >= 3:
        hrs.sort(key=lambda x: -x[1]["net"])
        good = [x for x in hrs[:3] if x[1]["net"] > 0]
        if good:
            ins.append(["good", "Most profitable hours: " + "; ".join(f"{_hl(h)} ({stat(a)})" for h, a in good)])
        bad = [x for x in hrs[::-1][:3] if x[1]["net"] < 0]
        if bad:
            ins.append(["bad", "Weakest hours: " + "; ".join(f"{_hl(h)} ({stat(a)})" for h, a in bad)])

    wds = [(d, a) for d, a in enumerate(weekday) if a["n"] >= min_n]
    if len(wds) >= 2:
        wds.sort(key=lambda x: -x[1]["net"])
        if wds[0][1]["net"] > 0:
            ins.append(["good", f"Best weekday: {WD[wds[0][0]]} ({stat(wds[0][1])})"])
        if wds[-1][1]["net"] < 0:
            ins.append(["bad", f"Worst weekday: {WD[wds[-1][0]]} ({stat(wds[-1][1])})"])

    cells = [(d, h, hm[d][h]) for d in range(7) for h in range(24) if hm[d][h]["n"] >= max(3, min_n // 2)]
    if len(cells) >= 4:
        cells.sort(key=lambda x: -x[2]["net"])
        d, h, a = cells[0]
        if a["net"] > 0:
            ins.append(["good", f"Best slot: {WD[d]} {_hl(h)} ({stat(a)})"])
        d, h, a = cells[-1]
        if a["net"] < 0:
            ins.append(["bad", f"Worst slot: {WD[d]} {_hl(h)} ({stat(a)})"])

    avoid = [h for h, a in hrs if a["net"] < 0 and a["w"] / a["n"] < 0.4]
    if avoid:
        ins.append(["warn", "Consider pausing in: " + ", ".join(_hl(h) for h in avoid[:6]) +
                    " (negative result and win rate under 40%). Check it holds on more data before acting."])

    ses = [(k, a) for k, a in sess.items() if a["n"] >= min_n]
    if len(ses) >= 2:
        ses.sort(key=lambda x: -x[1]["net"])
        ins.append(["info", f"Best session: {ses[0][0]} ({stat(ses[0][1])}); weakest: {ses[-1][0]} ({stat(ses[-1][1])}). Sessions are in UTC and may overlap."])

    if len(syms) >= 2:
        srt = sorted(((k, a) for k, a in syms.items() if a["n"] >= min_n), key=lambda x: -x[1]["net"])
        if len(srt) >= 2:
            ins.append(["info", f"Best symbol: {srt[0][0]} ({stat(srt[0][1])}); worst: {srt[-1][0]} ({stat(srt[-1][1])})"])
    if len(eas) >= 2:
        srt = sorted(((k, a) for k, a in eas.items() if a["n"] >= min_n), key=lambda x: -x[1]["net"])
        if len(srt) >= 2:
            ins.append(["info", f"Best EA: {srt[0][0]} ({stat(srt[0][1])}); worst: {srt[-1][0]} ({stat(srt[-1][1])})"])

    if summary["avg_dur_win"] and summary["avg_dur_loss"]:
        ratio = summary["avg_dur_loss"] / summary["avg_dur_win"]
        if ratio >= 1.8:
            ins.append(["warn", f"Losing trades are held {ratio:.1f}x longer than winners - a typical sign of cutting winners early / letting losers run."])
        elif ratio <= 0.5:
            ins.append(["info", f"Winners last {1 / ratio:.1f}x longer than losers - you are letting profits run."])

    if summary["payoff"] is not None:
        be_wr = 100 / (1 + summary["payoff"])
        ok = summary["win_rate"] >= be_wr
        ins.append(["good" if ok else "bad",
                    f"Payoff ratio {summary['payoff']}: you need a {be_wr:.0f}% win rate to break even, actual {summary['win_rate']:.0f}%."])
    if summary["worst_streak"] >= 5:
        ins.append(["warn", f"Longest losing streak: {summary['worst_streak']} trades. Make sure your position size can survive that."])
    if cur_dd > 0 and max_dd > 0 and cur_dd >= 0.7 * max_dd:
        ins.append(["bad", f"Currently in a drawdown of {cur_dd:,.2f} ({cur_dd / max_dd * 100:.0f}% of the worst ever {max_dd:,.2f})."])
    for h in health:
        if h["status"] in ("bad", "warn"):
            ins.append(["warn", f"EA health - {h['ea']} [{h['acc']}]: {h['note']} (last 30: {_fm(h['rec_net'])}, WR {h['rec_wr']:.0f}%)."])

    return {
        "empty": False, "meta": meta, "summary": summary, "curve": curve_out,
        "hour": [_out(a) for a in hour], "weekday": [_out(a) for a in weekday],
        "hm_wh": [[_out(a) for a in row] for row in hm],
        "hm_sh": {"rows": top_sym, "cells": [[_out(a) for a in sym_h[s]] for s in top_sym]},
        "hm_eh": {"rows": top_ea, "cells": [[_out(a) for a in ea_h[s]] for s in top_ea]},
        "calendar": [[k, round(v[0], 2), v[1]] for k, v in sorted(daily.items())],
        "sessions": [dict(_out(a), l=k) for k, a in sess.items()],
        "months": [dict(_out(a), l=k) for k, a in sorted(month.items())],
        "durations": [dict(_out(a), l=k) for k, a in dur_b.items()],
        "lots": lots, "sides": [dict(_out(a), l=k) for k, a in sides.items()],
        "hist": hist, "symbols": table(syms), "eas": table(eas), "accounts": accounts,
        "health": health, "insights": ins,
        "recent": [{"ts": t["close_ts"], "acc": label_of(t["acc"]), "symbol": t["symbol"], "side": t["side"],
                    "vol": t["vol"], "o": t["o"], "c": t["c"], "net": round(t["net"], 2), "dur": t["dur"],
                    "ea": t["ea"]} for t in T[-60:][::-1]],
    }
