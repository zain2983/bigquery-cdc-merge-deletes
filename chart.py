"""Draw results/cost_by_fix.svg: daily on-demand cost against the share of batches with a key-only delete.

    python3 chart.py            # reads results/xops.csv and results/merges.csv, writes results/cost_by_fix.svg

Standard library only. The per-batch bytes are measured (means of total_bytes_billed in the 12-batch sequence, and
the filtered head-to-head merge). The lines between 0% and 100% are a model, not a measurement:

    no filter   p * A + (1 - p) * F
    split       p * S + (1 - p) * F
    lookup      p * L + (1 - p) * (F + keymap insert upkeep)
    deferred    (1 - p) * F + p * (upsert merge + queue insert), plus one flush DELETE for every FLUSH_EVERY batches
                in which at least one batch queued a delete: (1 - (1 - p) ** FLUSH_EVERY) / FLUSH_EVERY per batch

p is the share of batches that contain at least one delete without a partition value, taken as independent between
batches. F is the filtered merge when every row carries its partition value.
"""

import csv
import os
import statistics
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
RES = os.path.join(HERE, "results")
USD_PER_TIB = 6.25
MERGES_PER_DAY = 288
FLUSH_EVERY = 6
SEQ = "seq_recent"


def usd_day(bytes_per_batch):
    return bytes_per_batch * MERGES_PER_DAY / 2**40 * USD_PER_TIB


def load():
    part = defaultdict(list)  # (arm, part) -> [bytes billed]
    for r in csv.DictReader(open(os.path.join(RES, "xops.csv"))):
        if r["scenario"] == SEQ:
            part[(r["arm"], r["part"])].append(int(r["total_bytes_billed"]))
    mean = {k: statistics.mean(v) for k, v in part.items()}
    filt = statistics.mean(int(r["total_bytes_billed"]) for r in csv.DictReader(open(os.path.join(RES, "merges.csv")))
                           if r["scenario"] == "h2h" and r["arm"] == "b")
    return mean, filt


def model(mean, filt):
    a = mean[("a", "merge")]
    s = mean[("s", "merge")] + mean[("s", "delete")]
    lk = sum(mean[("l", p)] for p in ("resolve", "keymap_del", "merge", "keymap_ins"))
    l0 = filt + mean[("l", "keymap_ins")]
    d1 = mean[("d", "merge")] + mean[("d", "queue")]
    dflush = mean[("d", "delete")]
    return {
        "a": lambda p: usd_day(p * a + (1 - p) * filt),
        "s": lambda p: usd_day(p * s + (1 - p) * filt),
        "l": lambda p: usd_day(p * lk + (1 - p) * l0),
        "d": lambda p: usd_day((1 - p) * filt + p * d1 + (1 - (1 - p) ** FLUSH_EVERY) / FLUSH_EVERY * dflush),
        "f": lambda p: usd_day(filt),
    }


# Light-mode palette, categorical slots 1-4 in fixed order, plus neutral ink for the reference line.
SURFACE, INK, INK2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e6e5e1"
SERIES = [
    ("a", "No filter (the fallback)", "#2a78d6", ""),
    ("s", "Split: filtered merge + DELETE by key", "#eb6834", "6 5"),
    ("l", "Lookup table (key to partition)", "#1baf7a", ""),
    ("d", f"Deferred deletes, flushed every {FLUSH_EVERY} batches", "#eda100", ""),
    ("f", "Every delete carries its partition value", "#8a8984", ""),
]


def draw(fn, out):
    w, h, ml, mr, mt, mb = 860, 510, 64, 236, 168, 56
    pw, ph, ymax = w - ml - mr, h - mt - mb, 10.0
    x = lambda p: ml + p * pw
    y = lambda v: mt + ph - v / ymax * ph
    o = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" width="{w}" height="{h}" '
         f'font-family="-apple-system, Segoe UI, Helvetica, Arial, sans-serif" role="img" '
         f'aria-label="Daily cost by share of batches with a key-only delete, by fix">',
         f'<rect width="{w}" height="{h}" fill="{SURFACE}"/>',
         f'<text x="{ml}" y="30" font-size="17" font-weight="600" fill="{INK}">Daily cost by share of batches with a key-only delete</text>',
         f'<text x="{ml}" y="50" font-size="12.5" fill="{INK2}">One merge every 5 minutes into a 3-year, 51.5 GB table, on-demand at $6.25/TiB. '
         f'0% and 100% use measured bytes billed;</text>',
         f'<text x="{ml}" y="66" font-size="12.5" fill="{INK2}">the lines between are a model (see chart.py).</text>']
    # Legend: two columns, line keys carry identity, text stays in ink.
    lx, ly = ml, 88
    for i, (k, name, col, dash) in enumerate(SERIES):
        cx = lx + (i % 2) * 330
        cy = ly + (i // 2) * 20
        d = f' stroke-dasharray="{dash}"' if dash else ""
        o.append(f'<line x1="{cx}" y1="{cy}" x2="{cx + 22}" y2="{cy}" stroke="{col}" stroke-width="2.5" stroke-linecap="round"{d}/>')
        o.append(f'<text x="{cx + 29}" y="{cy + 4}" font-size="12" fill="{INK2}">{name}</text>')
    for v in range(0, 11, 2):
        o.append(f'<line x1="{ml}" y1="{y(v):.1f}" x2="{ml + pw}" y2="{y(v):.1f}" stroke="{GRID}" stroke-width="1"/>')
        o.append(f'<text x="{ml - 8}" y="{y(v) + 4:.1f}" font-size="12" fill="{INK2}" text-anchor="end">${v}</text>')
    for p in (0, 0.25, 0.5, 0.75, 1):
        o.append(f'<text x="{x(p):.1f}" y="{mt + ph + 20}" font-size="12" fill="{INK2}" text-anchor="middle">{int(p * 100)}%</text>')
    o.append(f'<text x="{ml + pw / 2:.1f}" y="{h - 12}" font-size="12.5" fill="{INK2}" text-anchor="middle">'
             'Share of batches with at least one delete that has no partition value</text>')
    o.append(f'<text x="{ml}" y="{mt - 10}" font-size="12" fill="{INK2}">USD per day</text>')
    pts = [i / 200 for i in range(201)]
    for k, name, col, dash in sorted(SERIES, key=lambda t: "fdlas".index(t[0])):
        d = f' stroke-dasharray="{dash}"' if dash else ""
        path = " ".join(f"{'M' if i == 0 else 'L'}{x(p):.1f},{y(fn[k](p)):.1f}" for i, p in enumerate(pts))
        o.append(f'<path d="{path}" fill="none" stroke="{col}" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"{d}/>')
    # End labels at 100%, in ink; leader lines where two labels would collide.
    ex = x(1)
    top = (fn["a"](1) + fn["s"](1)) / 2
    o.append(f'<text x="{ex + 10}" y="{y(top) - 3:.1f}" font-size="12.5" font-weight="600" fill="{INK}">${fn["a"](1):.2f} no filter</text>')
    o.append(f'<text x="{ex + 10}" y="{y(top) + 13:.1f}" font-size="12.5" font-weight="600" fill="{INK}">${fn["s"](1):.2f} split</text>')
    o.append(f'<text x="{ex + 10}" y="{y(fn["d"](1)) + 4:.1f}" font-size="12.5" font-weight="600" fill="{INK}">${fn["d"](1):.2f} deferred</text>')
    for k, word, dy in (("l", "lookup", -9), ("f", "date in the delete", 15)):
        yy = y(fn[k](1))
        o.append(f'<line x1="{ex + 3}" y1="{yy:.1f}" x2="{ex + 16}" y2="{yy + dy:.1f}" stroke="{INK2}" stroke-width="1"/>')
        o.append(f'<text x="{ex + 20}" y="{yy + dy + 4:.1f}" font-size="12.5" font-weight="600" fill="{INK}">${fn[k](1):.2f} {word}</text>')
    o.append("</svg>")
    with open(out, "w") as f:
        f.write("\n".join(o) + "\n")


def main():
    mean, filt = load()
    fn = model(mean, filt)
    draw(fn, os.path.join(RES, "cost_by_fix.svg"))
    print("share,no_filter,split,lookup,deferred,delete_carries_date")
    for p in (0, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 1):
        print(f"{p:.2f}," + ",".join(f"{fn[k](p):.2f}" for k in ("a", "s", "l", "d", "f")))


if __name__ == "__main__":
    main()
