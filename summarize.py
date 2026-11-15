"""Summarize ext.sh results. Standard library only.

    python3 summarize.py results/xops.csv results/xdryrun.csv results/xsummary.csv

For each scenario and arm, a logical merge is the sum of its parts (merge, delete, resolve, keymap upkeep, queue)
for one rep and batch. The summary reports the median over those logical merges, the median per part, the dry-run
estimate next to the bytes billed, and the on-demand cost per day at one merge every five minutes.
"""

import csv
import statistics
import sys
from collections import defaultdict

USD_PER_TIB = 6.25
MERGES_PER_DAY = 288


def gb(b):
    return round(b / 1e9, 3)


def main(ops_path, dry_path, out_path):
    ops = list(csv.DictReader(open(ops_path)))
    dry = defaultdict(list)
    for r in csv.DictReader(open(dry_path)):
        if r["dryrun_bytes"]:
            dry[(r["scenario"], r["arm"], r["rep"], r["part"], r["batch"])].append(int(r["dryrun_bytes"]))

    logical = defaultdict(lambda: defaultdict(int))  # (scenario, arm) -> (rep, batch) -> bytes billed
    slots = defaultdict(lambda: defaultdict(int))
    parts = defaultdict(list)  # (scenario, arm, part) -> [(billed, processed, dryrun)]
    for r in ops:
        if not r["scenario"]:  # the keymap build
            continue
        key = (r["scenario"], r["arm"])
        unit = (r["rep"], r["batch"])
        billed = int(r["total_bytes_billed"] or 0)
        logical[key][unit] += billed
        slots[key][unit] += int(r["total_slot_ms"] or 0)
        d = dry.get((r["scenario"], r["arm"], r["rep"], r["part"], r["batch"]))
        parts[(r["scenario"], r["arm"], r["part"])].append(
            (billed, int(r["total_bytes_processed"] or 0), d[0] if d else None))

    rows = []
    for (scenario, arm), units in sorted(logical.items()):
        vals = list(units.values())
        s = list(slots[(scenario, arm)].values())
        rows.append({
            "scenario": scenario, "arm": arm, "part": "ALL", "merges": len(vals),
            "median_gb_billed": gb(statistics.median(vals)), "mean_gb_billed": gb(statistics.mean(vals)),
            "median_slot_s": round(statistics.median(s) / 1000, 1),
            "slot_s_spread": f"{round(min(s) / 1000, 1)}-{round(max(s) / 1000, 1)}",
            "median_gb_dryrun": "", "dryrun_over_billed": "",
            "usd_per_day_5min": round(statistics.mean(vals) * MERGES_PER_DAY / 2**40 * USD_PER_TIB, 2),
        })
        for (sc, ar, part), triples in sorted(parts.items()):
            if (sc, ar) != (scenario, arm):
                continue
            billed = [t[0] for t in triples]
            dr = [t[2] for t in triples if t[2] is not None]
            ratio = ""
            if dr and statistics.median(billed):
                ratio = round(statistics.median(dr) / statistics.median(billed), 2)
            rows.append({
                "scenario": scenario, "arm": arm, "part": part, "merges": len(triples),
                "median_gb_billed": gb(statistics.median(billed)), "mean_gb_billed": gb(statistics.mean(billed)),
                "median_slot_s": "", "slot_s_spread": "",
                "median_gb_dryrun": gb(statistics.median(dr)) if dr else "", "dryrun_over_billed": ratio,
                "usd_per_day_5min": "",
            })

    # Dry-run-only advisor rows (xadvisor): the recommendation is whichever estimate is lower.
    adv = defaultdict(dict)
    for (scenario, arm, rep, part, batch), v in dry.items():
        if scenario == "advisor":
            adv[rep][arm] = v[0]
    for rep in sorted(adv, key=lambda r: int(r[1:])):
        a, b = adv[rep].get("a"), adv[rep].get("b")
        if a is None or b is None:
            continue
        rows.append({
            "scenario": "advisor", "arm": "filter" if b < a else "no_filter", "part": rep, "merges": "",
            "median_gb_billed": "", "mean_gb_billed": "", "median_slot_s": "", "slot_s_spread": "",
            "median_gb_dryrun": f"a={gb(a)} b={gb(b)}", "dryrun_over_billed": "", "usd_per_day_5min": "",
        })

    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    for r in rows:
        print(",".join(str(v) for v in r.values()))


if __name__ == "__main__":
    main(*sys.argv[1:4])
