"""Area 4: estimate time-on-matter accuracy using zero-instrumentation approach.

Method:
  - For each view event, classify by folder/client extracted from url_path or url_query.
  - Time on view = view.time_spent (nanoseconds) if present, else inferred from gap
    to next event.
  - Foreground-only: use in_foreground_periods durations instead of full time_spent.
  - Idle gaps between events ≥ 5 min are treated as breaks (not attributed).
"""
import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from dd import REPO_ROOT

OUT = REPO_ROOT / "samples"
UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


def classify_view(a):
    """Return (kind, id) for the view, e.g., ('folder', '<uuid>') or ('home', None)."""
    v = a.get("view", {})
    path = v.get("url_path") or ""
    m = re.match(r"^/folder/([0-9a-f-]+)(?:/|$)", path)
    if m:
        return ("folder", m.group(1))
    m = re.match(r"^/client/([0-9a-f-]+)(?:/|$)", path)
    if m:
        return ("client", m.group(1))
    if path == "/client/inbox":
        return ("client_inbox", None)
    if path == "/client":
        return ("client_list", None)
    if path == "/":
        # When path is '/', the folder may still be in url_query
        q = v.get("url_query", {}) or {}
        if q.get("ideFolderId"):
            return ("folder", q["ideFolderId"])
        return ("home", None)
    return ("other", path)


def parse_ts(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


for label in ["short_today", "long_today", "capped_tue"]:
    events = json.loads((OUT / f"05_maarten_{label}.json").read_text())
    views = [e for e in events if e["attributes"]["attributes"].get("type") == "view"]

    # Sort chronologically
    views.sort(key=lambda e: e["attributes"]["timestamp"])

    time_on = defaultdict(float)           # seconds, using view.time_spent
    foreground_on = defaultdict(float)     # seconds, using in_foreground_periods sum

    for v in views:
        a = v["attributes"]["attributes"]
        vi = a.get("view", {})
        kind, ident = classify_view(a)
        key = f"{kind}:{ident}" if ident else kind

        ts_ns = vi.get("time_spent") or 0
        time_on[key] += ts_ns / 1e9

        fg_ns = sum(p.get("duration", 0) for p in (vi.get("in_foreground_periods") or []))
        foreground_on[key] += fg_ns / 1e9

    total = sum(time_on.values())
    total_fg = sum(foreground_on.values())
    print(f"\n=== {label} — time on matter breakdown ===")
    print(f"  sum(view.time_spent) = {total:.0f}s ({total/60:.1f} min)")
    print(f"  sum(foreground)       = {total_fg:.0f}s ({total_fg/60:.1f} min)")
    print(f"  (ratio fg/total: {100*total_fg/total:.0f}%)" if total else "")
    print(f"  top buckets by time_spent:")
    for key, secs in sorted(time_on.items(), key=lambda x: -x[1])[:12]:
        fg = foreground_on[key]
        pct = 100 * secs / total if total else 0
        print(f"    {secs:>7.0f}s ({pct:>4.1f}%)  fg={fg:>6.0f}s  {key}")

    # Gap-based idle detection using timestamps
    ts_list = [parse_ts(e["attributes"]["timestamp"]) for e in events]
    ts_list.sort()
    if len(ts_list) >= 2:
        session_span = (ts_list[-1] - ts_list[0]).total_seconds()
        gaps = [(ts_list[i+1] - ts_list[i]).total_seconds() for i in range(len(ts_list)-1)]
        idle_over_5 = sum(g for g in gaps if g >= 300)
        idle_over_15 = sum(g for g in gaps if g >= 900)
        print(f"  span={session_span:.0f}s  idle(≥5min)={idle_over_5:.0f}s  idle(≥15min)={idle_over_15:.0f}s")
        active = session_span - idle_over_5
        print(f"  → 'active' (span minus ≥5min gaps): {active:.0f}s = {active/60:.1f} min")
