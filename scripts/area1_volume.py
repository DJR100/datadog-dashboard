"""Area 1: volume, event types, users, sessions — last 24h and last 7d."""
import json
from pathlib import Path

from dd import REPO_ROOT, aggregate_rum

APP_ID = "db0d76f0-bf2e-4328-a098-d711035a664c"
BASE_QUERY = f"@application.id:{APP_ID}"

OUT_DIR = REPO_ROOT / "samples"
OUT_DIR.mkdir(exist_ok=True)


def run(label: str, frm: str, to: str):
    print(f"\n=== {label} ({frm} → {to}) ===")
    out: dict = {"label": label, "frm": frm, "to": to}

    # 1. Total event count + unique sessions + unique users
    r = aggregate_rum(
        compute=[
            {"type": "total", "aggregation": "count"},
            {"type": "total", "aggregation": "cardinality", "metric": "@session.id"},
            {"type": "total", "aggregation": "cardinality", "metric": "@usr.id"},
        ],
        query=BASE_QUERY,
        frm=frm,
        to=to,
    )
    out["totals"] = r
    buckets = r.get("data", {}).get("buckets", [])
    if buckets:
        c = buckets[0].get("computes", {})
        print("  total events:", c.get("c0"))
        print("  unique sessions:", c.get("c1"))
        print("  unique users:", c.get("c2"))

    # 2. Breakdown by event type
    r = aggregate_rum(
        compute=[{"type": "total", "aggregation": "count"}],
        group_by=[{"facet": "@type", "limit": 20}],
        query=BASE_QUERY,
        frm=frm,
        to=to,
    )
    out["by_type"] = r
    print("  by @type:")
    for b in r.get("data", {}).get("buckets", []):
        print(f"    {b.get('by', {}).get('@type'):>12}  {b.get('computes', {}).get('c0')}")

    # 3. Breakdown by user email
    r = aggregate_rum(
        compute=[
            {"type": "total", "aggregation": "count"},
            {"type": "total", "aggregation": "cardinality", "metric": "@session.id"},
        ],
        group_by=[{"facet": "@usr.email", "limit": 30}],
        query=BASE_QUERY,
        frm=frm,
        to=to,
    )
    out["by_user"] = r
    print("  by @usr.email:")
    for b in r.get("data", {}).get("buckets", []):
        email = b.get("by", {}).get("@usr.email") or "(none)"
        c = b.get("computes", {})
        print(f"    {email:<40}  events={c.get('c0'):<8}  sessions={c.get('c1')}")

    return out


results = {}
results["24h"] = run("Last 24h", "now-24h", "now")
results["7d"] = run("Last 7d", "now-7d", "now")

(OUT_DIR / "02_volume.json").write_text(json.dumps(results, indent=2))
print(f"\nSaved → samples/02_volume.json")
