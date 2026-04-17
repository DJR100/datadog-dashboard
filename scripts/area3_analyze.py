"""Area 3 analysis: event shape, URL patterns, minute timelines, matter-id hunt."""
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from dd import REPO_ROOT

OUT = REPO_ROOT / "samples"
SESSIONS = ["short_today", "long_today", "capped_tue"]


def parse_ts(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def walk_keys(d, prefix=""):
    """Yield all dotted key paths that exist in a nested dict (stop at leaves)."""
    if isinstance(d, dict):
        for k, v in d.items():
            p = f"{prefix}.{k}" if prefix else k
            yield p
            if isinstance(v, dict):
                yield from walk_keys(v, p)


# --- 1. Key inventory across view + action events
all_keys = Counter()
for label in SESSIONS:
    events = json.loads((OUT / f"05_maarten_{label}.json").read_text())
    for e in events:
        a = e["attributes"]["attributes"]
        for k in walk_keys(a):
            all_keys[k] += 1

print("Top 60 nested-attribute paths across the 3 sessions:")
for k, c in all_keys.most_common(60):
    print(f"  {c:>6}  {k}")

# Look for anything that smells like "matter" / "case" / "context" / custom fields
print("\n--- Smells-like-matter paths ---")
for k, c in all_keys.most_common(500):
    kl = k.lower()
    if any(t in kl for t in ["matter", "case", "client", "folder", "doc", "ide", "chat", "context", "custom", "engagement", "task"]):
        print(f"  {c:>6}  {k}")

# --- 2. Build minute-by-minute timeline for each session
def minute_timeline(events, label):
    print(f"\n=== Timeline: {label} (n={len(events)}) ===")
    by_minute = defaultdict(lambda: {"views": [], "actions": [], "urls": set()})

    view_events = [e for e in events if e["attributes"]["attributes"].get("type") == "view"]
    action_events = [e for e in events if e["attributes"]["attributes"].get("type") == "action"]
    print(f"  views={len(view_events)}  actions={len(action_events)}")

    # Action targets — what kinds of things does he click?
    action_names = Counter()
    action_types = Counter()
    for e in action_events:
        a = e["attributes"]["attributes"]
        act = a.get("action", {})
        action_types[act.get("type")] += 1
        tgt_name = (act.get("target") or {}).get("name")
        if tgt_name:
            action_names[tgt_name] += 1
    print(f"  action types: {dict(action_types.most_common(10))}")
    print(f"  top 15 action target names:")
    for name, c in action_names.most_common(15):
        print(f"    {c:>4}  {name[:120]!r}")

    # URL paths
    view_paths = Counter()
    view_url_query_keys = Counter()
    for e in view_events:
        a = e["attributes"]["attributes"]
        v = a.get("view", {})
        view_paths[v.get("url_path")] += 1
        for k in (v.get("url_query") or {}).keys():
            view_url_query_keys[k] += 1
    print(f"  view url paths: {dict(view_paths.most_common(15))}")
    print(f"  view url_query keys: {dict(view_url_query_keys.most_common(20))}")

    # Time distribution: actions per minute bucket
    for e in view_events + action_events:
        ts = parse_ts(e["attributes"]["timestamp"])
        key = ts.strftime("%H:%M")
        if e["attributes"]["attributes"].get("type") == "view":
            by_minute[key]["views"].append(e)
        else:
            by_minute[key]["actions"].append(e)
        v = e["attributes"]["attributes"].get("view", {})
        if v.get("url_path"):
            by_minute[key]["urls"].add(v.get("url_path"))

    # Gap analysis: biggest idle gaps between consecutive events
    ts_sorted = sorted(parse_ts(e["attributes"]["timestamp"]) for e in events)
    if len(ts_sorted) >= 2:
        gaps = [(ts_sorted[i+1] - ts_sorted[i]).total_seconds() for i in range(len(ts_sorted)-1)]
        gaps_sorted = sorted(gaps, reverse=True)[:10]
        print(f"  top 10 idle gaps (sec): {[f'{g:.0f}' for g in gaps_sorted]}")
        over_5min = sum(1 for g in gaps if g >= 300)
        over_15min = sum(1 for g in gaps if g >= 900)
        print(f"  gaps ≥ 5min: {over_5min}  gaps ≥ 15min: {over_15min}")

    return by_minute


minute_summaries = {}
for label in SESSIONS:
    events = json.loads((OUT / f"05_maarten_{label}.json").read_text())
    mt = minute_timeline(events, label)
    # Compact summary
    minute_summaries[label] = {
        t: {"views": len(d["views"]), "actions": len(d["actions"]), "urls": sorted(d["urls"])}
        for t, d in sorted(mt.items())
    }

(OUT / "06_timelines.json").write_text(json.dumps(minute_summaries, indent=2))
print(f"\nSaved → samples/06_timelines.json")

# --- 3. Pull one example view event fully to inspect structure
example_views = []
example_actions = []
for label in SESSIONS[:1]:
    events = json.loads((OUT / f"05_maarten_{label}.json").read_text())
    for e in events:
        if e["attributes"]["attributes"].get("type") == "view" and not example_views:
            example_views.append(e)
        if e["attributes"]["attributes"].get("type") == "action" and not example_actions:
            example_actions.append(e)
        if example_views and example_actions:
            break

(OUT / "07_event_examples.json").write_text(json.dumps({
    "view": example_views,
    "action": example_actions,
}, indent=2))
print(f"Saved → samples/07_event_examples.json")
