#!/usr/bin/env python3
"""Slow, independent reference for rules.md, used to cross-check the C++ pipeline on
other seeds. Usage: reference.py DATA_DIR OUT_DIR  (writes user_daily.csv, campaign_daily.csv)"""
import bisect
import calendar
import csv
import datetime as dt
import glob
import json
import os
import sys

DAY, WEEK, GAP = 86_400_000, 7 * 86_400_000, 1_800_000
DIGITS = set("0123456789")


def lines(path):
    with open(path, "rb") as f:
        data = f.read().decode("utf-8")
    rows = data.split("\n")
    if rows and rows[-1] == "":
        rows.pop()
    return rows[1:]  # header


def fields(line, n):
    parts = line.split(",")
    if len(parts) != n:
        return None
    out = []
    for p in parts:
        if '"' in p:
            if len(p) >= 2 and p[0] == '"' and p[-1] == '"' and '"' not in p[1:-1]:
                p = p[1:-1]
            else:
                return None
        out.append(p if p != "" else None)
    return out


def ts_ms(s):
    if s is None:
        return None
    if all(c in DIGITS for c in s):
        return int(s)
    if len(s) not in (20, 25):
        return None
    d = s[:19]
    if not (d[4] == "-" and d[7] == "-" and d[10] == "T" and d[13] == ":" and d[16] == ":"):
        return None
    nums = [d[0:4], d[5:7], d[8:10], d[11:13], d[14:16], d[17:19]]
    if not all(all(c in DIGITS for c in x) for x in nums):
        return None
    y, mo, da, h, mi, se = map(int, nums)
    if not (1 <= mo <= 12 and 1 <= da <= calendar.monthrange(y, mo)[1] if y >= 1 else False):
        return None
    if h > 23 or mi > 59 or se > 59:
        return None
    off = 0
    if len(s) == 20:
        if s[19] != "Z":
            return None
    else:
        sg, oh, colon, om = s[19], s[20:22], s[22], s[23:25]
        if sg not in "+-" or colon != ":" or not all(c in DIGITS for c in oh + om):
            return None
        if int(oh) > 23 or int(om) > 59:
            return None
        off = (int(oh) * 60 + int(om)) * 60_000 * (1 if sg == "+" else -1)
    epoch = dt.datetime(y, mo, da, h, mi, se, tzinfo=dt.timezone.utc).timestamp()
    return int(epoch) * 1000 - off


def main():
    data, out = sys.argv[1], sys.argv[2]
    man = json.load(open(os.path.join(data, "manifest.json")))
    ws, we = ts_ms(man["window_start"]), ts_ms(man["window_end"])

    parent = {}

    def find(x):
        while parent.get(x, x) != x:
            x = parent[x]
        return x

    for f in sorted(glob.glob(os.path.join(data, "links", "*.csv"))):
        for line in lines(f):
            r = fields(line, 2)
            if r is None or r[0] is None or r[1] is None:
                continue
            a, b = find(r[0]), find(r[1])
            parent.setdefault(r[0], r[0])
            parent.setdefault(r[1], r[1])
            if a != b:
                parent[max(a, b)] = min(a, b)  # root is the smallest ID seen so far

    def person(uid):
        return find(uid)  # min-ID root, since unions always keep the smaller root

    products = {}
    for line in lines(os.path.join(data, "products.csv")):
        r = fields(line, 2)
        products[r[0]] = r[1]
    campaigns = {}
    for line in lines(os.path.join(data, "campaigns.csv")):
        r = fields(line, 2)
        campaigns[r[0]] = r[1]

    best = {}
    for f in sorted(glob.glob(os.path.join(data, "events", "*.csv"))):
        for line in lines(f):
            r = fields(line, 7)
            if r is None or r[0] is None:
                continue
            ing = ts_ms(r[3])
            if r[0] not in best or ing < best[r[0]][0]:
                best[r[0]] = (ing, r)
    events = {}
    for ing, r in best.values():
        eid, uid, ts, _, typ, prod, price = r
        t = ts_ms(ts)
        if uid is None or t is None or not (ws <= t < we) or prod is None:
            continue
        if typ not in ("view", "cart", "purchase"):
            continue
        cents = 0
        if typ == "purchase":
            if price is None or not all(c in DIGITS for c in price) or not (1 <= int(price) <= 10_000_000):
                continue
            cents = int(price)
        if ing - t > DAY:
            continue
        events.setdefault(person(uid), []).append((t, cents, products.get(prod, "unknown")))

    clicks = {}
    seen = set()
    for f in sorted(glob.glob(os.path.join(data, "clicks", "*.csv"))):
        for line in lines(f):
            r = fields(line, 4)
            if r is None or r[0] is None or r[0] in seen:
                continue
            seen.add(r[0])
            cid, uid, camp, ts = r
            t = ts_ms(ts)
            if uid is None or t is None or not (ws - WEEK <= t < we) or camp not in campaigns:
                continue
            clicks.setdefault(person(uid), []).append((t, cid, camp))

    ud, cd = {}, {}
    for p, evs in events.items():
        evs.sort(key=lambda e: e[0])
        cl = sorted(clicks.get(p, []))
        cts = [c[0] for c in cl]
        sessions, cur = [], None
        for e in evs:
            if cur is None or e[0] - cur[-1][0] > GAP:
                cur = [e]
                sessions.append(cur)
            else:
                cur.append(e)
        for sess in sessions:
            start = sess[0][0]
            day = dt.datetime.fromtimestamp(start // 1000, dt.timezone.utc).date()
            rev = sum(e[1] for e in sess)
            u = ud.setdefault((p, day), [0, 0, 0, {}])
            u[0] += 1
            u[1] += len(sess)
            u[2] += rev
            for e in sess:
                if e[1]:
                    u[3][e[2]] = u[3].get(e[2], 0) + e[1]
            lo, hi = bisect.bisect_left(cts, start - WEEK), bisect.bisect_right(cts, start)
            window = cl[lo:hi]
            if window:
                top = max(c[0] for c in window)
                credit = min(c[1] for c in window if c[0] == top)
                camp = next(c[2] for c in window if c[1] == credit)
                for a in {c[2] for c in window}:
                    cd.setdefault((a, day), [0, 0, 0, 0])[3] += 1
            else:
                camp = "organic"
            c = cd.setdefault((camp, day), [0, 0, 0, 0])
            c[0] += 1
            c[1] += rev > 0
            c[2] += rev

    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "user_daily.csv"), "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["user_id", "day", "sessions", "events", "revenue_cents", "top_category"])
        for (p, day), (s, n, rev, cats) in ud.items():
            top = min(cats, key=lambda k: (-cats[k], k.encode())) if cats else ""
            w.writerow([p, day.isoformat(), s, n, rev, top])
    with open(os.path.join(out, "campaign_daily.csv"), "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["campaign_id", "channel", "day", "sessions", "purchasing_sessions", "revenue_cents",
                    "assisted_sessions"])
        for (camp, day), (s, ps, rev, asst) in cd.items():
            ch = "organic" if camp == "organic" else campaigns[camp]
            w.writerow([camp, ch, day.isoformat(), s, ps, rev, asst])


if __name__ == "__main__":
    main()
