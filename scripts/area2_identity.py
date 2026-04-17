"""Area 2: verify identity coverage.

- What % of events have @usr.email populated?
- What % have @usr.id?
- Any anonymous events, and which types?
- Confirm Maarten filter works end-to-end; confirm Tanya filter is ready.
"""
import json
from pathlib import Path

from dd import REPO_ROOT, aggregate_rum, search_rum_events

APP_ID = "db0d76f0-bf2e-4328-a098-d711035a664c"
BASE = f"@application.id:{APP_ID}"
OUT = REPO_ROOT / "samples"

# 1. Event count with vs without usr.email, 24h
with_email = aggregate_rum(
    compute=[{"type": "total", "aggregation": "count"}],
    query=f"{BASE} _exists_:@usr.email",
    frm="now-24h", to="now",
)
without_email = aggregate_rum(
    compute=[{"type": "total", "aggregation": "count"}],
    query=f"{BASE} -_exists_:@usr.email",
    frm="now-24h", to="now",
)
w = with_email["data"]["buckets"][0]["computes"]["c0"] if with_email["data"]["buckets"] else 0
wo = without_email["data"]["buckets"][0]["computes"]["c0"] if without_email["data"]["buckets"] else 0
total = w + wo
print(f"Last 24h events: {total}")
print(f"  with    @usr.email: {w}  ({100*w/total:.1f}%)")
print(f"  without @usr.email: {wo}  ({100*wo/total:.1f}%)")

# 2. Break down "without @usr.email" by event type
no_email_by_type = aggregate_rum(
    compute=[{"type": "total", "aggregation": "count"}],
    group_by=[{"facet": "@type", "limit": 10}],
    query=f"{BASE} -_exists_:@usr.email",
    frm="now-24h", to="now",
)
print("\nEvents without @usr.email, by @type (24h):")
for b in no_email_by_type["data"]["buckets"]:
    print(f"  {b['by'].get('@type'):>12}  {b['computes']['c0']}")

# 3. Confirm Maarten filter pulls events
maarten_email = "maarten.schellingerhout@alaro.ai"
m_count = aggregate_rum(
    compute=[
        {"type": "total", "aggregation": "count"},
        {"type": "total", "aggregation": "cardinality", "metric": "@session.id"},
    ],
    query=f'{BASE} @usr.email:"{maarten_email}"',
    frm="now-24h", to="now",
)
mc = m_count["data"]["buckets"][0]["computes"] if m_count["data"]["buckets"] else {}
print(f"\nMaarten (24h): events={mc.get('c0')} sessions={mc.get('c1')}")

# 4. Pre-check Tanya filter (should be 0 events; she starts Mon)
tanya_count = aggregate_rum(
    compute=[{"type": "total", "aggregation": "count"}],
    query=f'{BASE} @usr.email:"tanya@alaro.ai"',
    frm="now-7d", to="now",
)
tc = tanya_count["data"]["buckets"][0]["computes"]["c0"] if tanya_count["data"]["buckets"] else 0
print(f"Tanya events (7d): {tc}  [expected 0 — starts 2026-04-20]")

# 5. Any interesting @usr.* fields beyond email/id/name?
# Grab a recent Maarten event and dump all @usr.* keys
sample_events = list(search_rum_events(
    query=f'{BASE} @usr.email:"{maarten_email}"',
    frm="now-24h", to="now", limit=5, max_pages=1,
))
if sample_events:
    usr = sample_events[0]["attributes"]["attributes"].get("usr", {})
    print("\nMaarten's @usr object keys (sample):", list(usr.keys()))
    print("  full object:", json.dumps(usr, indent=2))

# Save
(OUT / "04_identity.json").write_text(json.dumps({
    "coverage_24h": {"with_email": w, "without_email": wo, "total": total},
    "no_email_by_type": no_email_by_type,
    "maarten_24h": m_count,
    "tanya_7d": tanya_count,
    "usr_sample": sample_events[0]["attributes"]["attributes"].get("usr") if sample_events else None,
}, indent=2))
print("\nSaved → samples/04_identity.json")
