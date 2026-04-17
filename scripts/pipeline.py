"""E2C prototype pipeline — Option A (zero-instrumentation).

Pulls view + action events for a user across a time window, attributes foreground
time per matter folder, auto-infers a human name for each matter, and writes
`data.json` alongside `dashboard.html`.

Usage:
    python scripts/pipeline.py --user maarten.schellingerhout@alaro.ai --days 7
    python scripts/pipeline.py --user tanya@alaro.ai --days 30

Output: data.json at repo root. Open dashboard.html in a browser.
"""
from __future__ import annotations

import argparse
import bisect
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from math import log
from pathlib import Path

from dd import REPO_ROOT, search_rum_events

# --- Accuracy tuning knobs (all thresholds live here) ---
ACTIVITY_GRACE_SEC = 300        # an action within this window keeps foreground alive
LONG_FG_THRESHOLD_SEC = 300     # foreground intervals longer than this must have activity
WORK_BLOCK_GAP_SEC = 15 * 60    # ≥ this gap starts a new work block

APP_ID = "db0d76f0-bf2e-4328-a098-d711035a664c"

# Action target names that are generic UI labels — skip when inferring matter names.
# Compared case-insensitively.
UI_NOISE = {
    s.lower() for s in [
        "Submit", "Delete", "Upload", "Upload New Version", "Promote", "Order",
        "Editor", "Chat panel", "Document panel", "New chat", "Clients",
        "Switch workspace", "Current workspace 1", "Current workspace 2",
        "Current workspace", "Document name", "Cancel", "Save", "Close", "Open",
        "Zoom in", "Zoom out", "Zoom", "Clear", "Reset", "Back", "Forward",
        "Next", "Previous", "breadcrumb", "Menu", "Settings", "Profile",
        "Logout", "Login", "Sign out", "Sign in", "Rename", "Copy", "Paste",
        "Edit", "Files", "Folders", "Search", "Filter", "Sort", "More",
        "Details", "Actions", "Options", "Expand", "Collapse", "Toggle",
        "Share", "Download", "Export", "Import", "Refresh", "Stop",
        "Play", "Pause", "Drag", "Drop", "Select", "Select all", "Deselect",
        "Home", "Dashboard", "Notifications", "Help", "Support",
        "Admin", "Internal", "Show", "Hide", "View", "Create",
        "Message", "Thread", "Reply", "Send", "Comment",
    ]
}
# Strings starting with these prefixes are almost never matter names
UI_PREFIXES_LOWER = (
    "type @",  # chat input placeholder
    "review the",
    "provide ",
    "can we ",
    "when items ",
    "if you ",
    "see ",
    "prepare ",
    "draft ",
    "what you must",
    "identify every",
    "click on",
)
# Words that strongly suggest a doc section / legal-document title rather than a matter name.
# Presence of any of these in a candidate label docks its inference score heavily.
DOC_SECTION_TERMS = {
    "agreement", "schedule", "clause", "confidential", "confidentiality",
    "report", "letter", "memorandum", "section", "exhibit", "appendix",
    "annex", "addendum", "amendment", "attachment", "recipients", "notice",
    "disclosure", "statement", "provision", "opinion", "draft", "final",
    "summary", "index", "register", "deed", "heads of terms", "nda", "msa",
    "sow", "dpa", "side letter", "term sheet", "undertakings",
}

UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


def parse_ts(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def classify_bucket(a: dict) -> tuple[str, str | None]:
    """Return (bucket_type, matter_id).

    bucket_type is one of:
      "matter"  — view is on a folder/matter (id is the folder UUID)
      "triage"  — view is on /client, /client/inbox, or /client/:id
      "overhead" — view is on '/' with no folder id, or anywhere else
    """
    v = a.get("view", {}) or {}
    path = v.get("url_path") or ""

    m = re.match(r"^/folder/([0-9a-f-]+)(?:/|$)", path)
    if m:
        return "matter", m.group(1)

    if path == "/client" or path == "/client/inbox":
        return "triage", None
    m = re.match(r"^/client/([0-9a-f-]+)(?:/|$)", path)
    if m:
        return "triage", None

    if path == "/":
        q = v.get("url_query") or {}
        if q.get("ideFolderId"):
            return "matter", q["ideFolderId"]
        return "overhead", None

    return "overhead", None


# Backwards compat alias used elsewhere in the file
classify_view = classify_bucket


def looks_like_matter_name(text: str) -> bool:
    """Fast reject filter for things that clearly aren't matter names."""
    if not text:
        return False
    t = text.strip()
    if len(t) < 3 or len(t) > 40:
        return False
    tl = t.lower()
    if tl in UI_NOISE:
        return False
    if any(tl.startswith(p) for p in UI_PREFIXES_LOWER):
        return False
    if UUID_RE.match(t):
        return False
    if t.isdigit():
        return False
    if "..." in t or "\u2026" in t:
        return False
    # Must contain at least one capital letter (matter names are proper nouns)
    if not any(c.isupper() for c in t):
        return False
    return True


def _name_shape_bonus(label: str) -> float:
    """Prefer sidebar-label shapes (short, proper-noun-heavy) over doc titles."""
    tokens = label.split()
    bonus = 1.0
    # Two-word names beat single-word unless the single word has internal caps (ElevenLabs)
    if len(tokens) == 1 and not any(c.isupper() for c in label[1:]):
        bonus *= 0.7
    # Short labels (≤3 words) look more like sidebar labels
    if len(tokens) <= 3:
        bonus *= 1.15
    # File extension → strongly looks like a document title
    if re.search(r"\.(docx|pdf|xlsx|doc|ppt|pptx|csv)\b", label, re.I):
        bonus *= 0.25
    # Doc-section vocabulary → probably a section title, not a matter
    tl = label.lower()
    if any(term in tl for term in DOC_SECTION_TERMS):
        bonus *= 0.2
    return bonus


def infer_matter_name(folder_id: str, folder_texts: dict[str, Counter]) -> str:
    """TF-IDF-like scoring: prefer labels uniquely associated with this folder."""
    own = folder_texts.get(folder_id, Counter())
    if not own:
        return f"Matter {folder_id[:8]}"

    global_count = Counter()
    for c in folder_texts.values():
        global_count.update(c)

    scored = []
    for label, n_here in own.items():
        if not looks_like_matter_name(label):
            continue
        n_global = global_count[label] or 1
        uniqueness = n_here / n_global
        strength = log(n_here + 1)
        shape = _name_shape_bonus(label)
        score = uniqueness * strength * shape
        scored.append((score, -len(label), label))

    if not scored:
        return f"Matter {folder_id[:8]}"
    scored.sort(reverse=True)
    return scored[0][2]


def pull_events(user_email: str, days: int) -> list[dict]:
    """Pull view + action + session events for a user across the window."""
    all_events: list[dict] = []
    for ev_type, max_pages in [("view", 40), ("action", 100), ("session", 10)]:
        q = (
            f"@application.id:{APP_ID} "
            f'@usr.email:"{user_email}" '
            f"@type:{ev_type}"
        )
        print(f"  pulling @type:{ev_type} ...", end=" ", flush=True)
        events = list(search_rum_events(
            query=q,
            frm=f"now-{days}d",
            to="now",
            limit=1000,
            max_pages=max_pages,
            sort="timestamp",
        ))
        print(f"{len(events)}")
        all_events.extend(events)
    return all_events


def filter_within_window(events: list[dict], days: int, now: datetime) -> list[dict]:
    """Keep only events whose timestamp falls inside the last `days` days."""
    cutoff = now.timestamp() - days * 86400
    out = []
    for e in events:
        ts = parse_ts(e["attributes"]["timestamp"])
        if ts.timestamp() >= cutoff:
            out.append(e)
    return out


def _build_intervals(view_events: list[dict]) -> list[tuple]:
    """Turn every foreground period into (abs_start, abs_end, bucket_type, matter_id, view_event)."""
    out = []
    for v in view_events:
        a = v["attributes"]["attributes"]
        vi = a.get("view", {}) or {}
        btype, bid = classify_bucket(a)
        # Treat the event timestamp as the view's start time. It's approximate — views
        # emit on flush/end — but the sum of foreground periods across views in wall-clock
        # order gives a coherent timeline for overlap detection.
        view_start = parse_ts(v["attributes"]["timestamp"]).timestamp()
        for p in vi.get("in_foreground_periods") or []:
            dur = (p.get("duration") or 0) / 1e9
            if dur <= 0:
                continue
            start = view_start + (p.get("start") or 0) / 1e9
            out.append((start, start + dur, btype, bid, v))
    out.sort(key=lambda x: (x[0], x[1]))
    return out


def _merge_overlapping(intervals: list[tuple]) -> list[tuple]:
    """First-wins merge: every wall-clock second is credited to exactly one bucket."""
    merged: list[tuple] = []
    current_end = 0.0
    for start, end, btype, bid, v in intervals:
        if end <= start:
            continue
        effective_start = max(start, current_end)
        if effective_start >= end:
            continue  # fully covered by an earlier (already-credited) interval
        merged.append((effective_start, end, btype, bid, v))
        if end > current_end:
            current_end = end
    return merged


def _apply_activity_filter(intervals: list[tuple], action_epochs: list[float]) -> list[tuple]:
    """Drop long foreground intervals with no nearby action activity."""
    kept: list[tuple] = []
    for start, end, btype, bid, v in intervals:
        duration = end - start
        if duration < LONG_FG_THRESHOLD_SEC:
            kept.append((start, end, btype, bid, v))
            continue
        lo = bisect.bisect_left(action_epochs, start - ACTIVITY_GRACE_SEC)
        hi = bisect.bisect_right(action_epochs, end + ACTIVITY_GRACE_SEC)
        if hi > lo:
            kept.append((start, end, btype, bid, v))
        # else: drop — long idle foreground (tab left open)
    return kept


def _compute_work_blocks(timestamps: list[float]) -> int:
    """Number of contiguous work blocks separated by ≥ WORK_BLOCK_GAP_SEC."""
    if not timestamps:
        return 0
    ts = sorted(timestamps)
    blocks = 1
    for i in range(1, len(ts)):
        if ts[i] - ts[i - 1] >= WORK_BLOCK_GAP_SEC:
            blocks += 1
    return blocks


def attribute_time(events: list[dict]) -> dict:
    """Compute attributed foreground seconds per matter/triage/overhead.

    Pipeline:
      1. Build absolute (start,end,bucket) intervals from every view's foreground periods.
      2. Merge overlapping intervals → each wall-clock second counts once (multi-tab dedup).
      3. Drop long-idle intervals (no actions nearby) → activity-filtered "active" time.
      4. Aggregate by bucket and by day; compute per-matter work blocks.
    """
    view_events    = [e for e in events if e["attributes"]["attributes"].get("type") == "view"]
    action_events  = [e for e in events if e["attributes"]["attributes"].get("type") == "action"]
    session_events = [e for e in events if e["attributes"]["attributes"].get("type") == "session"]

    action_epochs = sorted(parse_ts(e["attributes"]["timestamp"]).timestamp() for e in action_events)

    # Raw foreground (before any filter) — useful as a sanity-check denominator.
    raw_fg_seconds = 0.0
    for v in view_events:
        vi = v["attributes"]["attributes"].get("view", {}) or {}
        for p in vi.get("in_foreground_periods") or []:
            raw_fg_seconds += (p.get("duration") or 0) / 1e9

    # Step 1-3: build + merge + filter
    intervals = _build_intervals(view_events)
    deduped = _merge_overlapping(intervals)
    active = _apply_activity_filter(deduped, action_epochs)

    # Step 4: aggregate
    matter_total_sec: dict[str, float] = defaultdict(float)
    matter_daily: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    matter_last_active: dict[str, datetime] = {}
    matter_session_ids: dict[str, set] = defaultdict(set)
    matter_event_epochs: dict[str, list[float]] = defaultdict(list)
    triage_total = 0.0
    overhead_total = 0.0
    daily_triage: dict[str, float] = defaultdict(float)
    daily_overhead: dict[str, float] = defaultdict(float)

    for start, end, btype, bid, v in active:
        dur = end - start
        day = datetime.fromtimestamp(start, tz=timezone.utc).strftime("%Y-%m-%d")
        if btype == "matter" and bid:
            matter_total_sec[bid] += dur
            matter_daily[day][bid] += dur
            ts = parse_ts(v["attributes"]["timestamp"])
            prev = matter_last_active.get(bid)
            if not prev or ts > prev:
                matter_last_active[bid] = ts
            sess_id = (v["attributes"]["attributes"].get("session") or {}).get("id")
            if sess_id:
                matter_session_ids[bid].add(sess_id)
            matter_event_epochs[bid].append(start)
        elif btype == "triage":
            triage_total += dur
            daily_triage[day] += dur
        else:
            overhead_total += dur
            daily_overhead[day] += dur

    # Total active foreground (sum of kept intervals, deduped)
    active_fg_sec = sum(end - start for start, end, *_ in active)
    deduped_fg_sec = sum(end - start for start, end, *_ in deduped)

    # --- Name inference (using ALL view/action events, before filter) ---
    folder_texts: dict[str, Counter] = defaultdict(Counter)
    for e in action_events:
        a = e["attributes"]["attributes"]
        btype, bid = classify_bucket(a)
        if btype == "matter" and bid:
            nm = ((a.get("action") or {}).get("target") or {}).get("name")
            if nm:
                folder_texts[bid][nm] += 1

    timed = sorted(view_events + action_events, key=lambda e: e["attributes"]["timestamp"])
    for i, e in enumerate(timed):
        a = e["attributes"]["attributes"]
        if a.get("type") != "action":
            continue
        nm = ((a.get("action") or {}).get("target") or {}).get("name")
        if not nm:
            continue
        for j in range(i + 1, min(i + 4, len(timed))):
            aj = timed[j]["attributes"]["attributes"]
            if aj.get("type") != "view":
                continue
            btype, bid = classify_bucket(aj)
            if btype == "matter" and bid:
                folder_texts[bid][nm] += 3  # sidebar-click signal: 3× weight
                break

    # --- Build matter list ---
    matters = []
    for folder_id, secs in matter_total_sec.items():
        name = infer_matter_name(folder_id, folder_texts)
        last = matter_last_active.get(folder_id)
        work_blocks = _compute_work_blocks(matter_event_epochs[folder_id])
        matters.append({
            "folder_id": folder_id,
            "name": name,
            "hours": round(secs / 3600, 2),
            "seconds": round(secs, 1),
            "session_count": len(matter_session_ids[folder_id]),
            "work_blocks": work_blocks,
            "last_active": last.isoformat() if last else None,
            "top_labels": [t for t, _ in folder_texts.get(folder_id, Counter()).most_common(5)],
        })
    matters.sort(key=lambda m: -m["hours"])

    # --- Daily shape (matters + triage + overhead) ---
    days_sorted = sorted(set(list(matter_daily.keys())
                             + list(daily_triage.keys())
                             + list(daily_overhead.keys())))
    daily = []
    for day in days_sorted:
        by_matter = {fid: round(s / 3600, 2) for fid, s in matter_daily.get(day, {}).items()}
        row = {
            "date": day,
            "by_matter": by_matter,
            "triage_hours":   round(daily_triage.get(day, 0) / 3600, 2),
            "overhead_hours": round(daily_overhead.get(day, 0) / 3600, 2),
        }
        row["total_hours"] = round(
            sum(by_matter.values()) + row["triage_hours"] + row["overhead_hours"], 2
        )
        daily.append(row)

    # --- Session totals ---
    total_session_seconds = 0.0
    for s in session_events:
        sess = s["attributes"]["attributes"].get("session", {}) or {}
        total_session_seconds += (sess.get("time_spent") or 0) / 1e9

    # --- Overall work blocks (all matters combined) ---
    all_matter_epochs = sorted(
        epoch for epochs in matter_event_epochs.values() for epoch in epochs
    )
    total_work_blocks = _compute_work_blocks(all_matter_epochs)

    summary = {
        "total_focused_hours": round(active_fg_sec / 3600, 2),          # ← now activity-filtered
        "total_matter_hours":  round(sum(matter_total_sec.values()) / 3600, 2),
        "total_triage_hours":  round(triage_total / 3600, 2),
        "total_overhead_hours":round(overhead_total / 3600, 2),
        "total_session_hours": round(total_session_seconds / 3600, 2),
        "attention_ratio": round(
            active_fg_sec / total_session_seconds if total_session_seconds else 0, 3
        ),
        "num_sessions": len(session_events),
        "num_matters":  len(matters),
        "num_work_blocks": total_work_blocks,
        # Diagnostic fields — useful for comparing to the raw numbers
        "raw_foreground_hours":     round(raw_fg_seconds / 3600, 2),
        "deduped_foreground_hours": round(deduped_fg_sec / 3600, 2),
        "idle_stripped_hours":      round((deduped_fg_sec - active_fg_sec) / 3600, 2),
        "multi_tab_overlap_hours":  round((raw_fg_seconds - deduped_fg_sec) / 3600, 2),
    }
    return {"matters": matters, "daily": daily, "summary": summary}


# Human-readable display name + "starts on" note for users who haven't appeared yet.
USER_PROFILES = {
    "maarten.schellingerhout@alaro.ai": {"display": "Maarten", "role": "Senior lawyer"},
    "tanya@alaro.ai": {"display": "Tanya", "role": "Paralegal", "starts_on": "2026-04-20"},
}


def compute_user_payload(email: str, windows: list[int], max_days: int, now: datetime) -> dict:
    print(f"\n=== {email} ===")
    events = pull_events(email, max_days)
    print(f"Total events pulled: {len(events)}")

    # Pick display name from events if available; otherwise fall back to profile.
    profile = USER_PROFILES.get(email, {})
    user_name = profile.get("display")
    for e in events:
        usr = e["attributes"]["attributes"].get("usr") or {}
        if usr.get("email") == email and usr.get("name"):
            # Title-case if the stored name is all lowercase (e.g. "maarten schellingerhout")
            raw = usr["name"]
            user_name = raw.title() if raw == raw.lower() else raw
            break

    windows_out: dict[str, dict] = {}
    for d in windows:
        subset = filter_within_window(events, d, now) if d < max_days else events
        attributed = attribute_time(subset)
        windows_out[str(d)] = {
            "days": d,
            "from": f"now-{d}d",
            "event_count": len(subset),
            **attributed,
        }
        s = attributed["summary"]
        print(f"  {d:>3}d: {len(subset):>6} events  "
              f"focused={s['total_focused_hours']:>5.1f}h  "
              f"matter={s['total_matter_hours']:>5.1f}h  "
              f"matters={s['num_matters']:>2}  sessions={s['num_sessions']}")

    has_any_data = len(events) > 0
    return {
        "email": email,
        "name": user_name or email,
        "display": profile.get("display") or (user_name or email).split()[0],
        "role": profile.get("role"),
        "starts_on": profile.get("starts_on"),
        "has_data": has_any_data,
        "event_count_max_window": len(events),
        "windows": windows_out,
        "available_windows": [str(d) for d in windows],
        "default_window": "7" if "7" in [str(d) for d in windows] else str(windows[0]),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--users", default="maarten.schellingerhout@alaro.ai,tanya@alaro.ai",
                    help="comma-separated list of user emails")
    ap.add_argument("--windows", default="1,3,7,14,30",
                    help="comma-separated list of day-windows to compute")
    ap.add_argument("--out", default="data.json")
    args = ap.parse_args()

    users = [u.strip() for u in args.users.split(",") if u.strip()]
    windows = sorted({int(d.strip()) for d in args.windows.split(",") if d.strip()})
    max_days = max(windows)
    now = datetime.now(timezone.utc)

    print(f"Users: {users}")
    print(f"Windows (days): {windows}  → pulling once at max = {max_days}d")

    users_out: dict[str, dict] = {}
    for email in users:
        users_out[email] = compute_user_payload(email, windows, max_days, now)

    # Default user: first one that actually has data, else first listed.
    default_user = next((u for u in users if users_out[u]["has_data"]), users[0])

    result = {
        "generated_at": now.isoformat(),
        "available_users": users,
        "default_user": default_user,
        "users": users_out,
    }

    out_path = REPO_ROOT / args.out
    out_path.write_text(json.dumps(result, indent=2, default=str))
    js_path = REPO_ROOT / "data.js"
    js_path.write_text(
        "window.DASHBOARD_DATA = " + json.dumps(result, default=str) + ";\n"
    )
    print(f"\nWrote {out_path.relative_to(REPO_ROOT)}")
    print(f"Wrote {js_path.relative_to(REPO_ROOT)}  (loaded by dashboard.html)")


if __name__ == "__main__":
    main()
