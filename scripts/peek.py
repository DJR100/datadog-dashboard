"""Quick peek at last-hour Alaro RUM events to inspect event shape."""
import json
from pathlib import Path

from dd import REPO_ROOT, search_rum_events

APP_ID = "db0d76f0-bf2e-4328-a098-d711035a664c"

OUT = REPO_ROOT / "samples" / "01_peek_last_hour.json"
OUT.parent.mkdir(exist_ok=True)

events = list(
    search_rum_events(
        query=f"@application.id:{APP_ID}",
        frm="now-1h",
        to="now",
        limit=20,
        max_pages=1,
    )
)

OUT.write_text(json.dumps(events, indent=2))
print(f"Pulled {len(events)} events → {OUT.relative_to(REPO_ROOT)}")

if events:
    e0 = events[0]
    print("\nFirst event top-level keys:", list(e0.keys()))
    attrs = e0.get("attributes", {})
    print("attributes top-level:", list(attrs.keys()))
    a = attrs.get("attributes", {})
    print("nested attributes keys:", list(a.keys())[:50])
    print("\nEvent type:", a.get("type"))
    print("URL:", (a.get("view") or {}).get("url"))
    print("usr:", a.get("usr"))
