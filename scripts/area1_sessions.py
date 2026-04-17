"""Area 1 cont.: session shape — durations, event counts, views per session.

Pulls session-type events (Datadog emits one summary event per session on end).
"""
import json
import statistics
from pathlib import Path

from dd import REPO_ROOT, search_rum_events

APP_ID = "db0d76f0-bf2e-4328-a098-d711035a664c"
OUT = REPO_ROOT / "samples"
OUT.mkdir(exist_ok=True)


def percentiles(xs, ps):
    xs = sorted(xs)
    n = len(xs)
    out = {}
    for p in ps:
        idx = max(0, min(n - 1, int(round(p / 100 * (n - 1)))))
        out[p] = xs[idx]
    return out


# Pull all session-summary events (type:session) for last 7 days across all users.
# session count was 344 over 7d — well under 5k limit, one page likely fine.
sessions = list(
    search_rum_events(
        query=f"@application.id:{APP_ID} @type:session",
        frm="now-7d",
        to="now",
        limit=1000,
        max_pages=5,
    )
)
print(f"Pulled {len(sessions)} session events (7d, all users)")

rows = []
for s in sessions:
    a = s["attributes"]["attributes"]
    sess = a.get("session", {})
    rows.append(
        {
            "session_id": sess.get("id"),
            "user_email": (a.get("usr") or {}).get("email"),
            "user_name": (a.get("usr") or {}).get("name"),
            "duration_ns": sess.get("time_spent"),
            "view_count": (sess.get("view") or {}).get("count"),
            "action_count": (sess.get("action") or {}).get("count"),
            "error_count": (sess.get("error") or {}).get("count"),
            "long_task_count": (sess.get("long_task") or {}).get("count"),
            "resource_count": (sess.get("resource") or {}).get("count"),
            "is_active": sess.get("is_active"),
            "timestamp": s["attributes"].get("timestamp"),
        }
    )

# Overall distribution
durs_sec = [r["duration_ns"] / 1e9 for r in rows if r["duration_ns"]]
print("\n-- Session duration (seconds) across ALL sessions, 7d --")
print(f"  n={len(durs_sec)}")
if durs_sec:
    p = percentiles(durs_sec, [50, 75, 90, 95, 99])
    print(f"  median={p[50]:.0f}s  p75={p[75]:.0f}s  p90={p[90]:.0f}s  p95={p[95]:.0f}s  p99={p[99]:.0f}s")
    print(f"  mean={statistics.mean(durs_sec):.0f}s  max={max(durs_sec):.0f}s")

# Maarten only
maarten_email = "maarten.schellingerhout@alaro.ai"
m = [r for r in rows if r["user_email"] == maarten_email]
m_durs = [r["duration_ns"] / 1e9 for r in m if r["duration_ns"]]
print(f"\n-- Maarten's session shape (7d, n={len(m)}) --")
if m_durs:
    p = percentiles(m_durs, [50, 75, 90, 95, 99])
    print(f"  duration sec: median={p[50]:.0f}s  p75={p[75]:.0f}s  p90={p[90]:.0f}s  p95={p[95]:.0f}s  max={max(m_durs):.0f}s")
    acts = [r["action_count"] for r in m if r["action_count"] is not None]
    vws = [r["view_count"] for r in m if r["view_count"] is not None]
    res = [r["resource_count"] for r in m if r["resource_count"] is not None]
    if acts:
        print(f"  actions per session: median={statistics.median(acts):.0f}  max={max(acts)}  total={sum(acts)}")
    if vws:
        print(f"  views per session: median={statistics.median(vws):.0f}  max={max(vws)}  total={sum(vws)}")
    if res:
        print(f"  resources per session: median={statistics.median(res):.0f}  max={max(res)}  total={sum(res)}")

print("\n-- Maarten's sessions listed --")
for r in sorted(m, key=lambda x: x["timestamp"] or ""):
    dur_s = (r["duration_ns"] or 0) / 1e9
    print(
        f"  {r['timestamp']}  {r['session_id'][:8]}  "
        f"dur={dur_s:7.0f}s ({dur_s/60:5.1f}min)  "
        f"views={r['view_count']}  actions={r['action_count']}  "
        f"resources={r['resource_count']}  errors={r['error_count']}"
    )

# Save
(OUT / "03_sessions_7d.json").write_text(json.dumps(rows, indent=2))
print(f"\nSaved → samples/03_sessions_7d.json")
