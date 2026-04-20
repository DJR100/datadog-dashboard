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


def _group_overhead_chunks(
    intervals: list[tuple],           # [(start_ts, end_ts, view_event), ...]
    matter_names: dict[str, str],
    site: str = "datadoghq.eu",
) -> list[dict]:
    """Merge contiguous overhead intervals into chunks with session + referrer hints.

    For each chunk we record the most common referrer-folder ("probably came from X")
    so Dillon can see likely-misattributed vs. genuine home-page time in one list.
    """
    if not intervals:
        return []

    def ref_folder_of(v):
        ru = (v["attributes"]["attributes"].get("view") or {}).get("referrer_url") or {}
        path = ru.get("url_path") or ""
        m = re.match(r"^/folder/([0-9a-f-]+)(?:/|$)", path)
        if m:
            return m.group(1)
        q = ru.get("url_query") or {}
        return q.get("ideFolderId")

    def sess_id_of(v):
        return (v["attributes"]["attributes"].get("session") or {}).get("id")

    ivs = sorted(intervals, key=lambda x: x[0])
    CHUNK_GAP = 120  # seconds — intervals within this are the same chunk

    chunks = []

    def flush(start, end, active_sec, sessions, refs):
        sess_list = sorted(sessions)
        ref_id = None
        ref_name = None
        if refs:
            ref_id = refs.most_common(1)[0][0]
            ref_name = matter_names.get(ref_id)
        replay_url = None
        if sess_list:
            # Use the Explorer URL (session replay has shorter retention than event data).
            start_ms = int((start - 30) * 1000)
            end_ms   = int((end + 30) * 1000)
            replay_url = _explorer_session_url(
                sess_list[0],
                None,  # we don't have the session event ID from a view event
                start_ms, end_ms, site,
            )
        chunks.append({
            "start": datetime.fromtimestamp(start, tz=timezone.utc).isoformat(),
            "end":   datetime.fromtimestamp(end,   tz=timezone.utc).isoformat(),
            "active_min": round(active_sec / 60, 1),
            "span_min":   round((end - start) / 60, 1),
            "session_ids": sess_list,
            "referrer_folder_id":   ref_id,
            "referrer_folder_name": ref_name,
            "replay_url": replay_url,
        })

    cur_start, cur_end, first_v = ivs[0]
    cur_active = cur_end - cur_start
    cur_sessions = {sess_id_of(first_v)} if sess_id_of(first_v) else set()
    cur_refs = Counter()
    if ref_folder_of(first_v):
        cur_refs[ref_folder_of(first_v)] += 1

    for s, e, v in ivs[1:]:
        if s - cur_end > CHUNK_GAP:
            flush(cur_start, cur_end, cur_active, cur_sessions, cur_refs)
            cur_start = s
            cur_end = e
            cur_active = e - s
            cur_sessions = set()
            cur_refs = Counter()
        else:
            cur_end = max(cur_end, e)
            cur_active += (e - s)
        sid = sess_id_of(v)
        if sid:
            cur_sessions.add(sid)
        ref = ref_folder_of(v)
        if ref:
            cur_refs[ref] += 1

    flush(cur_start, cur_end, cur_active, cur_sessions, cur_refs)
    chunks.sort(key=lambda c: -c["active_min"])
    return chunks


def _group_blocks_with_sessions(intervals: list[tuple]) -> list[dict]:
    """From a list of (start, end, view_event) intervals for one matter, produce work-block dicts.

    Each block: start_ts, end_ts, duration_sec, active_sec, session_ids.
    """
    if not intervals:
        return []
    ivs = sorted(intervals, key=lambda x: x[0])
    blocks = []
    cur_start = ivs[0][0]
    cur_end = ivs[0][1]
    cur_active = ivs[0][1] - ivs[0][0]
    cur_sessions = set()
    sess_id = ((ivs[0][2]["attributes"]["attributes"].get("session") or {}).get("id"))
    if sess_id:
        cur_sessions.add(sess_id)

    for s, e, v in ivs[1:]:
        sid = ((v["attributes"]["attributes"].get("session") or {}).get("id"))
        if s - cur_end >= WORK_BLOCK_GAP_SEC:
            blocks.append({
                "start": cur_start, "end": cur_end,
                "duration_sec": cur_end - cur_start,
                "active_sec": cur_active,
                "session_ids": sorted(cur_sessions),
            })
            cur_start = s
            cur_sessions = set()
            cur_active = 0
        cur_end = max(cur_end, e)
        cur_active += (e - s)
        if sid:
            cur_sessions.add(sid)

    blocks.append({
        "start": cur_start, "end": cur_end,
        "duration_sec": cur_end - cur_start,
        "active_sec": cur_active,
        "session_ids": sorted(cur_sessions),
    })
    return blocks


def _infer_doc_name(doc_id: str, name_counter: Counter) -> str:
    """Pick the most likely filename for a doc from action-target text seen on it."""
    if not name_counter:
        return f"Document {doc_id[:8]}"
    # Prefer entries that clearly look like a filename
    for nm, _ in name_counter.most_common(20):
        if re.search(r"\.(docx|pdf|xlsx|doc|ppt|pptx|csv|txt|md)\b", nm, re.I):
            # Strip trailing junk after the extension
            m = re.match(r"^(.*?\.(?:docx|pdf|xlsx|doc|ppt|pptx|csv|txt|md))\b", nm, re.I)
            return m.group(1) if m else nm
    return name_counter.most_common(1)[0][0]


def _compute_matter_details(
    matter_id: str,
    per_matter_intervals: list[tuple],  # [(start, end, view_event), ...]
    views: list[dict],
    actions: list[dict],
    all_matter_daily_sec: dict[str, dict[str, float]],
    matter_names: dict[str, str],
    session_event_id_by_uuid: dict[str, str] | None = None,
    site: str = "datadoghq.eu",
) -> dict:
    """Detail payload for a single matter: documents, chats, action mix, heatmap, etc."""
    # --- Work blocks (detailed)
    work_blocks = _group_blocks_with_sessions(per_matter_intervals)

    # --- Documents (by docId in url_query)
    doc_fg_sec: dict[str, float] = defaultdict(float)
    doc_versions: dict[str, set] = defaultdict(set)
    doc_view_count: Counter = Counter()
    doc_last_seen: dict[str, float] = {}
    for v in views:
        a = v["attributes"]["attributes"]
        vi = a.get("view", {}) or {}
        q = vi.get("url_query") or {}
        doc_id = q.get("docId")
        if not doc_id:
            continue
        fg_ns = sum(p.get("duration", 0) for p in (vi.get("in_foreground_periods") or []))
        doc_fg_sec[doc_id] += fg_ns / 1e9
        if q.get("versionGroupId"):
            doc_versions[doc_id].add(q["versionGroupId"])
        doc_view_count[doc_id] += 1
        ts_epoch = parse_ts(v["attributes"]["timestamp"]).timestamp()
        if ts_epoch > doc_last_seen.get(doc_id, 0):
            doc_last_seen[doc_id] = ts_epoch

    # Doc name inference: action target text on events while docId was active
    doc_name_candidates: dict[str, Counter] = defaultdict(Counter)
    doc_click_count: Counter = Counter()
    for e in actions:
        a = e["attributes"]["attributes"]
        v = a.get("view") or {}
        q = v.get("url_query") or {}
        doc_id = q.get("docId")
        if not doc_id:
            continue
        doc_click_count[doc_id] += 1
        nm = ((a.get("action") or {}).get("target") or {}).get("name")
        if nm:
            doc_name_candidates[doc_id][nm] += 1

    documents = []
    for doc_id, secs in sorted(doc_fg_sec.items(), key=lambda kv: -kv[1]):
        documents.append({
            "doc_id": doc_id,
            "name": _infer_doc_name(doc_id, doc_name_candidates.get(doc_id, Counter())),
            "hours": round(secs / 3600, 2),
            "version_count": len(doc_versions[doc_id]),
            "view_count": doc_view_count[doc_id],
            "click_count": doc_click_count[doc_id],
            "last_seen": datetime.fromtimestamp(doc_last_seen[doc_id], tz=timezone.utc).isoformat()
                if doc_id in doc_last_seen else None,
        })

    # --- AI chats (by chatId in url_query)
    chat_fg_sec: dict[str, float] = defaultdict(float)
    chat_first_ts: dict[str, float] = {}
    chat_last_ts: dict[str, float] = {}
    chat_submit_count: Counter = Counter()
    chat_first_prompts: dict[str, str] = {}
    chat_long_texts: dict[str, Counter] = defaultdict(Counter)
    for v in views:
        a = v["attributes"]["attributes"]
        vi = a.get("view", {}) or {}
        q = vi.get("url_query") or {}
        chat_id = q.get("ideChatId")
        if not chat_id:
            continue
        fg_ns = sum(p.get("duration", 0) for p in (vi.get("in_foreground_periods") or []))
        chat_fg_sec[chat_id] += fg_ns / 1e9
        ts_epoch = parse_ts(v["attributes"]["timestamp"]).timestamp()
        if chat_id not in chat_first_ts or ts_epoch < chat_first_ts[chat_id]:
            chat_first_ts[chat_id] = ts_epoch
        if ts_epoch > chat_last_ts.get(chat_id, 0):
            chat_last_ts[chat_id] = ts_epoch

    for e in actions:
        a = e["attributes"]["attributes"]
        v = a.get("view") or {}
        q = v.get("url_query") or {}
        chat_id = q.get("ideChatId")
        if not chat_id:
            continue
        nm = ((a.get("action") or {}).get("target") or {}).get("name") or ""
        if nm.strip() == "Submit":
            chat_submit_count[chat_id] += 1
        # Collect likely message-content text. Heuristics to skip UI-cascade labels:
        # require (a) natural-language shape (30%+ lowercase-starting words),
        # (b) not a file-tree cascade ("Explorer", "File Explorer", menu items with "New ").
        stripped = nm.strip()
        if not (15 <= len(stripped) <= 200):
            continue
        if stripped.startswith("Type @") or stripped.startswith("Explorer "):
            continue
        if "File Explorer" in stripped or "Context menu" in stripped:
            continue
        words = stripped.split()
        if len(words) < 4:
            continue
        lower_start = sum(1 for w in words if w and w[0].islower())
        if lower_start / len(words) < 0.3:
            continue
        chat_long_texts[chat_id][stripped] += 1

    chats = []
    for chat_id in sorted(chat_fg_sec.keys(), key=lambda k: -chat_fg_sec[k]):
        first_prompt = None
        if chat_long_texts[chat_id]:
            first_prompt = chat_long_texts[chat_id].most_common(1)[0][0]
        chats.append({
            "chat_id": chat_id,
            "hours": round(chat_fg_sec[chat_id] / 3600, 2),
            "submit_count": chat_submit_count[chat_id],
            "first_prompt": first_prompt,
            "first_seen": datetime.fromtimestamp(chat_first_ts[chat_id], tz=timezone.utc).isoformat()
                if chat_id in chat_first_ts else None,
            "last_seen": datetime.fromtimestamp(chat_last_ts[chat_id], tz=timezone.utc).isoformat()
                if chat_id in chat_last_ts else None,
        })

    # --- Action breakdown
    action_target_counter: Counter = Counter()
    for e in actions:
        nm = ((e["attributes"]["attributes"].get("action") or {}).get("target") or {}).get("name") or "(unknown)"
        action_target_counter[nm] += 1

    # Categorize into groups
    categories = {
        "Authoring (Upload/Promote/Delete)": 0,
        "AI chat (New chat/Submit)": 0,
        "Navigation (panels/workspace)": 0,
        "Content interaction (doc/text clicks)": 0,
        "Other UI": 0,
    }
    AUTHORING = {"upload", "upload new version", "promote", "delete", "rename", "order"}
    AI_CHAT = {"new chat", "submit"}
    NAV = {"chat panel", "document panel", "switch workspace", "clients",
           "current workspace", "current workspace 1", "current workspace 2", "editor"}
    for nm, n in action_target_counter.items():
        nml = nm.strip().lower()
        if nml in AUTHORING:
            categories["Authoring (Upload/Promote/Delete)"] += n
        elif nml in AI_CHAT:
            categories["AI chat (New chat/Submit)"] += n
        elif nml in NAV:
            categories["Navigation (panels/workspace)"] += n
        elif nm.startswith("Type @") or len(nm) >= 40:
            categories["Content interaction (doc/text clicks)"] += n
        else:
            categories["Other UI"] += n

    top_actions = [
        {"label": k, "count": v}
        for k, v in action_target_counter.most_common(15)
    ]

    # --- Heatmap (7 days × 24 hours) of active time
    heatmap = [[0.0] * 24 for _ in range(7)]  # [weekday][hour] = seconds
    # Use the merged/filtered per-matter intervals for the heatmap
    for start, end, _v in per_matter_intervals:
        cur = start
        while cur < end:
            dt = datetime.fromtimestamp(cur, tz=timezone.utc)
            weekday = dt.weekday()  # Mon=0 … Sun=6
            hour = dt.hour
            # Minutes remaining in this hour
            next_hour = dt.replace(minute=0, second=0, microsecond=0).timestamp() + 3600
            chunk_end = min(end, next_hour)
            heatmap[weekday][hour] += (chunk_end - cur) / 60  # minutes
            cur = chunk_end
    heatmap_rounded = [[round(v, 1) for v in row] for row in heatmap]

    # --- Co-touched matters: other matters active on the same day as this one
    own_days = set()
    for d, bm in all_matter_daily_sec.items():
        if matter_id in bm:
            own_days.add(d)
    co_touched = defaultdict(float)  # folder_id → shared-day hours
    for d in own_days:
        for fid, secs in all_matter_daily_sec.get(d, {}).items():
            if fid != matter_id:
                co_touched[fid] += secs
    co_list = [
        {"folder_id": fid, "name": matter_names.get(fid, f"Matter {fid[:8]}"),
         "shared_day_hours": round(secs / 3600, 2)}
        for fid, secs in sorted(co_touched.items(), key=lambda kv: -kv[1])
    ][:10]

    # --- Session replay links
    session_hours: dict[str, float] = defaultdict(float)
    session_starts: dict[str, float] = {}
    session_ends: dict[str, float] = {}
    for blk in work_blocks:
        for sid in blk["session_ids"]:
            # Allocate block duration to all its sessions equally (rough)
            session_hours[sid] += blk["active_sec"] / max(1, len(blk["session_ids"]))
            if sid not in session_starts or blk["start"] < session_starts[sid]:
                session_starts[sid] = blk["start"]
            if blk["end"] > session_ends.get(sid, 0):
                session_ends[sid] = blk["end"]

    sessions_for_replay = []
    sid_to_eid = session_event_id_by_uuid or {}
    for sid, secs in sorted(session_hours.items(), key=lambda kv: -kv[1]):
        start_ms = int((session_starts[sid] - 60) * 1000)
        end_ms   = int((session_ends[sid]   + 60) * 1000)
        sessions_for_replay.append({
            "session_id": sid,
            "hours_on_matter": round(secs / 3600, 2),
            "start": datetime.fromtimestamp(session_starts[sid], tz=timezone.utc).isoformat(),
            "end":   datetime.fromtimestamp(session_ends[sid],   tz=timezone.utc).isoformat(),
            "replay_url": _explorer_session_url(sid, sid_to_eid.get(sid), start_ms, end_ms, site),
        })

    # --- Format work blocks for JSON
    work_blocks_out = []
    for b in work_blocks:
        work_blocks_out.append({
            "start": datetime.fromtimestamp(b["start"], tz=timezone.utc).isoformat(),
            "end":   datetime.fromtimestamp(b["end"],   tz=timezone.utc).isoformat(),
            "duration_min": round(b["duration_sec"] / 60, 1),
            "active_min":   round(b["active_sec"]   / 60, 1),
            "session_ids":  b["session_ids"],
        })

    return {
        "work_blocks": work_blocks_out,
        "documents": documents,
        "chats": chats,
        "action_categories": [{"label": k, "count": v} for k, v in categories.items() if v > 0],
        "top_actions": top_actions,
        "heatmap_minutes": heatmap_rounded,  # [weekday 0..6][hour 0..23]
        "co_touched": co_list,
        "sessions_for_replay": sessions_for_replay,
    }


APP_ID = "db0d76f0-bf2e-4328-a098-d711035a664c"


def _explorer_session_url(
    session_id: str,
    session_event_id: str | None,
    start_ms: int,
    end_ms: int,
    site: str = "datadoghq.eu",
) -> str:
    """Build a Datadog RUM Explorer URL that opens the given session.

    Uses `event=<id>` when we have the session event ID (auto-opens the panel);
    otherwise filters by `@session.id:<uuid>` so the Explorer lands on that row.
    """
    from urllib.parse import urlencode
    if session_event_id:
        q = f"@type:session @application.id:{APP_ID}"
        params = {
            "query": q,
            "event": session_event_id,
            "from_ts": start_ms,
            "to_ts": end_ms,
            "live": "false",
        }
    else:
        q = f"@type:session @application.id:{APP_ID} @session.id:{session_id}"
        params = {
            "query": q,
            "from_ts": start_ms,
            "to_ts": end_ms,
            "live": "false",
        }
    return f"https://app.{site}/rum/sessions?{urlencode(params)}"


def _explorer_matter_url(
    folder_id: str, start_ms: int, end_ms: int, site: str = "datadoghq.eu"
) -> str:
    """Build a RUM Explorer URL filtered to this matter (folder_id)."""
    from urllib.parse import urlencode
    q = f"@application.id:{APP_ID} @view.url:*{folder_id}*"
    params = {"query": q, "from_ts": start_ms, "to_ts": end_ms, "live": "false"}
    return f"https://app.{site}/rum/explorer?{urlencode(params)}"


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

    # Map session UUID → outer event ID so URLs can auto-open the session panel.
    session_event_id_by_uuid: dict[str, str] = {}
    for se in session_events:
        sid = (se["attributes"]["attributes"].get("session") or {}).get("id")
        if sid and se.get("id"):
            session_event_id_by_uuid[sid] = se["id"]

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
    # First pass: collect matter intervals by folder (from activity-filtered list)
    # so we can compute per-matter details below.
    per_matter_intervals: dict[str, list[tuple]] = defaultdict(list)
    overhead_interval_list: list[tuple] = []  # (start, end, view_event)
    for start, end, btype, bid, v in active:
        if btype == "matter" and bid:
            per_matter_intervals[bid].append((start, end, v))
        elif btype == "overhead":
            overhead_interval_list.append((start, end, v))

    # Also build {day → {folder_id → seconds}} for co-touched computation
    all_matter_daily_sec = {
        day: dict(bm) for day, bm in matter_daily.items()
    }

    # Compute names first (cheap), then use them in matter_names for co-touched lookups
    matter_names = {
        fid: infer_matter_name(fid, folder_texts)
        for fid in matter_total_sec.keys()
    }

    # Partition view/action events by matter (once, for re-use in detail computation)
    matter_views: dict[str, list[dict]] = defaultdict(list)
    matter_actions: dict[str, list[dict]] = defaultdict(list)
    for v in view_events:
        btype, bid = classify_bucket(v["attributes"]["attributes"])
        if btype == "matter" and bid:
            matter_views[bid].append(v)
    for e in action_events:
        btype, bid = classify_bucket(e["attributes"]["attributes"])
        if btype == "matter" and bid:
            matter_actions[bid].append(e)

    matters = []
    for folder_id, secs in matter_total_sec.items():
        last = matter_last_active.get(folder_id)
        wb_count = _compute_work_blocks(matter_event_epochs[folder_id])
        details = _compute_matter_details(
            matter_id=folder_id,
            per_matter_intervals=per_matter_intervals.get(folder_id, []),
            views=matter_views.get(folder_id, []),
            actions=matter_actions.get(folder_id, []),
            all_matter_daily_sec=all_matter_daily_sec,
            matter_names=matter_names,
            session_event_id_by_uuid=session_event_id_by_uuid,
        )
        matters.append({
            "folder_id": folder_id,
            "name": matter_names[folder_id],
            "hours": round(secs / 3600, 2),
            "seconds": round(secs, 1),
            "session_count": len(matter_session_ids[folder_id]),
            "work_blocks": wb_count,
            "last_active": last.isoformat() if last else None,
            "top_labels": [t for t, _ in folder_texts.get(folder_id, Counter()).most_common(5)],
            "details": details,
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
    # Overhead chunks — list of unattributed-time periods with replay links
    overhead_chunks = _group_overhead_chunks(overhead_interval_list, matter_names)

    return {
        "matters": matters,
        "daily": daily,
        "summary": summary,
        "overhead_chunks": overhead_chunks,
    }


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
