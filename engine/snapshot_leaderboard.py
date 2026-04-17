#!/usr/bin/env python3
"""Hourly leaderboard snapshot + diff vs prior. Designed to run in GH Actions.

Saves snapshots to reports/leaderboard/snapshots/<iso>.json
Computes diff from last snapshot: new entrants, biggest movers (ROI delta).
Updates the leaderboard report and writes a /reports/movers/index.html.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from urllib import request

LEADERBOARD = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"
ROOT = Path(__file__).resolve().parent.parent  # offprem/
REPORTS = ROOT / "reports"
SNAP_DIR = REPORTS / "leaderboard" / "snapshots"
SNAP_DIR.mkdir(parents=True, exist_ok=True)


def fetch() -> list:
    with request.urlopen(LEADERBOARD, timeout=20) as r:
        return json.loads(r.read())["leaderboardRows"]


def perf(row: dict, window: str) -> dict:
    for w, p in row["windowPerformances"]:
        if w == window:
            try:
                return {
                    "pnl": float(p.get("pnl", 0)),
                    "roi": float(p.get("roi", 0)),
                    "vlm": float(p.get("vlm", 0)),
                }
            except (TypeError, ValueError):
                return {"pnl": 0, "roi": 0, "vlm": 0}
    return {"pnl": 0, "roi": 0, "vlm": 0}


def filter_top(rows: list, window: str, n: int = 100, min_vol: float = 1_000_000) -> list:
    enriched = []
    for r in rows:
        try:
            acct = float(r["accountValue"])
        except (TypeError, ValueError):
            continue
        p = perf(r, window)
        if p["vlm"] < min_vol or acct < 10_000:
            continue
        enriched.append({"addr": r["ethAddress"], "account": acct, **p})
    enriched.sort(key=lambda x: x["roi"], reverse=True)
    return enriched[:n]


def diff_snapshots(prev: list, curr: list) -> dict:
    prev_map = {r["addr"]: r for r in prev}
    curr_map = {r["addr"]: r for r in curr}
    new_entrants = [r for r in curr if r["addr"] not in prev_map]
    movers = []
    for r in curr:
        if r["addr"] in prev_map:
            old_roi = prev_map[r["addr"]]["roi"]
            delta = r["roi"] - old_roi
            movers.append({**r, "roi_delta": delta, "old_roi": old_roi})
    movers.sort(key=lambda x: abs(x["roi_delta"]), reverse=True)
    dropped = [r for r in prev if r["addr"] not in curr_map]
    return {"new": new_entrants[:10], "movers": movers[:20], "dropped": dropped[:10]}


def write_movers_html(diff: dict, window: str, generated: str):
    out = REPORTS / "movers"
    out.mkdir(parents=True, exist_ok=True)

    def row(r, with_delta=False):
        addr = r["addr"]
        short = f"{addr[:6]}…{addr[-4:]}"
        cells = [
            f'<td><a href="https://app.hyperliquid.xyz/explorer/address/{addr}">{short}</a></td>',
            f'<td>${r["account"]:,.0f}</td>',
            f'<td class="{"pos" if r["roi"]>=0 else "neg"}">{r["roi"]*100:+.1f}%</td>',
            f'<td>${r["pnl"]:,.0f}</td>',
            f'<td>${r["vlm"]/1e6:,.1f}M</td>',
        ]
        if with_delta:
            d = r["roi_delta"]
            cells.append(f'<td class="{"pos" if d>=0 else "neg"}">{d*100:+.2f}pp</td>')
            cells.append(f'<td>{r["old_roi"]*100:+.1f}%</td>')
        return "<tr>" + "".join(cells) + "</tr>"

    new_rows = "\n".join(row(r) for r in diff["new"])
    mover_rows = "\n".join(row(r, with_delta=True) for r in diff["movers"])

    html = f"""<!doctype html><html><head><meta charset="utf-8">
<title>HL leaderboard movers · {window}</title>
<style>
  body {{ background:#0e1117; color:#e6e6e6; font:14px/1.5 -apple-system,system-ui,sans-serif; margin:0; padding:32px; max-width:1300px; }}
  h1 {{ margin:0 0 6px; font-size:22px; }}
  h2 {{ margin:24px 0 8px; font-size:15px; color:#9fb0c0; }}
  .sub {{ color:#7d8a96; margin-bottom:24px; }}
  table {{ border-collapse:collapse; width:100%; font-size:13px; margin-bottom:24px; }}
  th, td {{ padding:8px 10px; border-bottom:1px solid #222; text-align:right; font-variant-numeric:tabular-nums; }}
  th:first-child, td:first-child {{ text-align:left; }}
  th {{ background:#161b22; color:#9fb0c0; font-weight:500; }}
  tr:hover {{ background:#1a2029; }}
  a {{ color:#5a9fd4; text-decoration:none; }}
  a:hover {{ text-decoration:underline; }}
  .pos {{ color:#16c784; }} .neg {{ color:#ea3943; }}
</style></head><body>
<h1>Movers · who's heating up</h1>
<div class="sub">{generated} · window {window} · cron'd hourly via GH Actions</div>
<h2>New entrants (top 10 not in prior snapshot)</h2>
<table><thead><tr><th>Wallet</th><th>Account</th><th>ROI</th><th>PnL</th><th>Volume</th></tr></thead>
<tbody>{new_rows or '<tr><td colspan="5">none</td></tr>'}</tbody></table>
<h2>Biggest ROI moves (vs last snapshot)</h2>
<table><thead><tr><th>Wallet</th><th>Account</th><th>ROI now</th><th>PnL</th><th>Volume</th><th>ROI Δ</th><th>ROI prior</th></tr></thead>
<tbody>{mover_rows or '<tr><td colspan="7">none</td></tr>'}</tbody></table>
</body></html>"""
    (out / "index.html").write_text(html)


def update_runs_log(diff: dict, window: str, generated: str):
    runs_log = REPORTS / "runs.json"
    runs = json.loads(runs_log.read_text()) if runs_log.exists() else []
    runs = [r for r in runs if r["slug"] != "movers"]
    n_new = len(diff["new"])
    n_movers = len(diff["movers"])
    runs.insert(0, {
        "slug": "movers", "label": f"HL movers ({n_new} new, {n_movers} shifts)",
        "source": "stats-data.hyperliquid.xyz", "dataset": window,
        "avg_return_pct": 0, "avg_sharpe": 0,
        "ts": generated,
    })
    runs_log.write_text(json.dumps(runs, indent=2))


def main():
    window = "month"
    print("fetching leaderboard...")
    rows = fetch()
    top = filter_top(rows, window)
    generated = datetime.now(timezone.utc).isoformat(timespec="seconds")

    snap_path = SNAP_DIR / f"{generated.replace(':', '')}.json"
    snap_path.write_text(json.dumps({"window": window, "generated": generated, "top": top}, indent=2))
    print(f"saved {snap_path.name} ({len(top)} traders)")

    snapshots = sorted(SNAP_DIR.glob("*.json"))
    if len(snapshots) < 2:
        print("first snapshot, no diff to compute")
        return

    prev = json.loads(snapshots[-2].read_text())["top"]
    diff = diff_snapshots(prev, top)
    write_movers_html(diff, window, generated)
    update_runs_log(diff, window, generated)
    print(f"diff: {len(diff['new'])} new, {len(diff['movers'])} movers, {len(diff['dropped'])} dropped")

    # cap stored snapshots to last 168 (~1 week of hourly)
    if len(snapshots) > 168:
        for old in snapshots[:-168]:
            old.unlink()


if __name__ == "__main__":
    main()
