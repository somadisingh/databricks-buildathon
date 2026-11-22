#!/bin/sh
# bench.sh SET [N]: run the image N times (default 5) on a volume dataset; print per-phase median.
SET="$1"; N="${2:-5}"
for i in $(seq "$N"); do scripts/vrun.sh "$SET" 2>&1 | grep '^\['; echo ---; done | python3 -c '
import sys,statistics,re
runs=sys.stdin.read().split("---")[:-1]; phases={}
for r in runs:
    prev=0
    for m in re.finditer(r"\[\s*([\d.]+)\] (.+)", r):
        t=float(m.group(1)); phases.setdefault(m.group(2),[]).append(t-prev); prev=t
print("  ".join(f"{k}={statistics.median(v)*1000:.0f}" for k,v in phases.items()))
tot=[float(re.findall(r"\[\s*([\d.]+)\] written",r)[0]) for r in runs if "written" in r]
print("total ms: median %.0f  min %.0f  all %s" % (statistics.median(tot)*1000, min(tot)*1000, [round(t*1000) for t in tot]))'
