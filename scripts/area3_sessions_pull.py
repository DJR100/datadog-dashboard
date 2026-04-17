"""Area 3: pull full event streams (views + actions) for chosen Maarten sessions.

Skipping resource events to keep things tractable — they're 84% of volume and mostly
noise for the time-on-task question. We'll sample some resource URL patterns separately.
"""
import json
from pathlib import Path

from dd import REPO_ROOT, search_rum_events

APP_ID = "db0d76f0-bf2e-4328-a098-d711035a664c"
OUT = REPO_ROOT / "samples"
OUT.mkdir(exist_ok=True)

# Three sessions chosen for variety:
# - short today (~1hr, moderate activity)
# - long today (~3.3hrs, heavy activity)
# - 4hr-capped (heaviest session in the week)
SESSIONS = [
    ("short_today",  "6ee517ed", "2026-04-17T12:10:00Z", "2026-04-17T13:30:00Z"),
    ("long_today",   "81673ea7", "2026-04-17T07:50:00Z", "2026-04-17T11:30:00Z"),
    ("capped_tue",   "06c1217c", "2026-04-14T09:50:00Z", "2026-04-14T14:10:00Z"),
]

for label, sess_prefix, frm, to in SESSIONS:
    # Search by session ID. Prefix-match won't work; we need full UUID.
    # Cheaper: pull by time window + Maarten's email + session type filters.
    # First find the full session id.
    pass

# Simpler approach: pull view+action events by session.id lookup needs full UUID.
# From 03_sessions_7d.json we have those. Load and map.
s7 = json.loads((OUT / "03_sessions_7d.json").read_text())
by_prefix = {r["session_id"][:8]: r["session_id"] for r in s7 if r.get("session_id")}

for label, sess_prefix, frm, to in SESSIONS:
    sess_id = by_prefix.get(sess_prefix)
    if not sess_id:
        print(f"!! {label}: no full session id for prefix {sess_prefix}")
        continue
    print(f"\n[{label}] session_id={sess_id}  window={frm} → {to}")

    # Pull views and actions for this session. Use type filter, then session.id filter.
    events = list(search_rum_events(
        query=f'@application.id:{APP_ID} @session.id:{sess_id} (@type:view OR @type:action)',
        frm=frm, to=to,
        limit=1000, max_pages=5,
        sort="timestamp",  # chronological
    ))
    print(f"  pulled {len(events)} view+action events")

    fn = OUT / f"05_maarten_{label}.json"
    fn.write_text(json.dumps(events, indent=2))
    print(f"  saved → samples/{fn.name}")
