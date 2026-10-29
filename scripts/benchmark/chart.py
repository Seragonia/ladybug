"""Turns run.sh results into an SVG chart and a Markdown table (no dependencies).

usage: chart.py <results.jsonl> <out.svg>   -> prints the Markdown table
Each row compares upstream and the fork on one measure; bars are scaled per row (lower is better).
The latest result of each (data, engine, phase) wins.
"""
import json
import sys
from html import escape

results_path, svg_path = sys.argv[1:3]
latest = {}
for line in open(results_path):
    r = json.loads(line)
    latest[(r["data"], r["engine"], r["phase"])] = r

rows = []  # (data, label, unit, upstream value or None, fork value or None, upstream error, fork error)
for data in sorted({k[0] for k in latest}):
    def get(engine, phase):
        return latest.get((data, engine, phase), {})

    for phase, label in (("load", "build graph"), ("extend", "extend +10% edges"), ("delete", "delete ~1% edges")):
        up, fork = get("upstream", phase), get("fork", phase)
        rows.append((data, label, "s", up.get("seconds") if up.get("ok") else None,
                     fork.get("seconds") if fork.get("ok") else None, up.get("error"), fork.get("error")))
        rows.append((data, f"{label}: peak RAM", "GB", up.get("peak_rss_gb"), fork.get("peak_rss_gb"), None, None))
    up_q, fork_q = get("upstream", "query"), get("fork", "query")
    for query in (fork_q.get("query_ms") or up_q.get("query_ms") or {}):
        rows.append((data, query, "ms", (up_q.get("query_ms") or {}).get(query), (fork_q.get("query_ms") or {}).get(query),
                     None if up_q.get("ok") else up_q.get("error"), None if fork_q.get("ok") else fork_q.get("error")))


def fmt(value, unit):
    if value is None:
        return "failed"
    return f"{value:,.0f} {unit}" if value >= 100 else f"{value:.2f} {unit}" if value < 10 else f"{value:.1f} {unit}"


# Markdown table
print("| Data | Measure | Upstream | Fork | Speed-up |")
print("|---|---|---|---|---|")
for data, label, unit, up, fork, up_err, _ in rows:
    gain = f"{up / fork:.1f}x" if up and fork else ("upstream fails" if up is None and fork else "")
    print(f"| {data} | {label} | {fmt(up, unit)} | {fmt(fork, unit)} | {gain} |")

# SVG: two bars per row, scaled to the row's larger value.
label_w, bar_w, value_w, row_h, top = 230, 330, 90, 34, 56
width = label_w + bar_w + value_w + 20
height = top + row_h * len(rows) + 20 * len({r[0] for r in rows}) + 20
colors = {"upstream": "#9aa0a6", "fork": "#d9480f"}
svg = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" font-family="sans-serif" font-size="12">',
       f'<rect width="{width}" height="{height}" fill="white"/>',
       '<text x="10" y="22" font-size="15" font-weight="bold">Fast-load fork vs upstream Ladybug (lower is better)</text>',
       f'<rect x="10" y="34" width="12" height="12" fill="{colors["upstream"]}"/><text x="27" y="44">upstream</text>',
       f'<rect x="100" y="34" width="12" height="12" fill="{colors["fork"]}"/><text x="117" y="44">fork</text>']
y, current = top, None
for data, label, unit, up, fork, up_err, fork_err in rows:
    if data != current:
        current = data
        svg.append(f'<text x="10" y="{y + 12}" font-weight="bold">{escape(data)}</text>')
        y += 20
    scale = max(v for v in (up, fork, 1e-9) if v is not None)
    svg.append(f'<text x="10" y="{y + 18}">{escape(label)}</text>')
    for i, (engine, value) in enumerate((("upstream", up), ("fork", fork))):
        by = y + 4 + i * 13
        if value is None:
            svg.append(f'<text x="{label_w}" y="{by + 10}" fill="#c92a2a">failed</text>')
            continue
        length = max(1, bar_w * value / scale)
        svg.append(f'<rect x="{label_w}" y="{by}" width="{length:.1f}" height="11" fill="{colors[engine]}"/>')
        svg.append(f'<text x="{label_w + length + 4:.1f}" y="{by + 10}" font-size="11">{escape(fmt(value, unit))}</text>')
    y += row_h
svg.append("</svg>")
open(svg_path, "w").write("\n".join(svg))
