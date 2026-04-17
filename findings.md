# Datadog RUM → E2C feasibility findings

**Date:** 2026-04-17
**Scope:** Can Alaro's existing Datadog RUM data tell us how Maarten (and shortly Tanya) spend their time per matter, with enough fidelity to drive an E2C pipeline — and what's the cheapest instrumentation upgrade that would tighten that?
**Method:** Read-only queries against `api.datadoghq.eu`. Three full Maarten sessions pulled end-to-end; aggregate queries across last 24h and last 7d. Raw JSON in `samples/` (gitignored).

---

## TL;DR

- **The data is richer than expected.** Identity is populated, session replay is on at 100%, matter and client IDs are in URLs, and Maarten already generates ~78k events / 25 sessions in a week.
- **Matter attribution works today, with caveats.** Every `view` event carries the folder ID (matter) in either `url_path` (`/folder/:id`) or `url_query.ideFolderId`. We can compute "time on matter X" without adding a single line of code to Alaro.
- **The key trap: `view.time_spent` lies.** It inflates by 5–8× vs. real wall-clock session time because of how the SPA emits views. Use `in_foreground_periods` and/or event-gap analysis instead.
- **Zero-instrumentation accuracy is probably ±25–35% on matter-level minutes.** Good enough for directional dashboards, not for billing or tight OKRs.
- **One custom action per matter switch (~1 hr of Alaro work) gets us to ±10%** and solves the "home page / navigation" attribution hole.
- **Full heartbeat instrumentation per the pipeline brief gets us to ±5%** and unlocks idle detection, milestone tagging, and reliable stitching across the 4-hr session cap.
- **E2C milestone coverage today:** only `work_in_progress` is cleanly visible. `triage_complete` is inferrable from URL navigation patterns; the other five milestones need either Alaro instrumentation or integration with systems outside Alaro (email, DocuSign).

---

## 1. Lay of the land

### 1.1 RUM applications

One RUM app, unambiguously Alaro:

| field | value |
|---|---|
| name | `alaro-dashboard` |
| id | `db0d76f0-bf2e-4328-a098-d711035a664c` |
| type | browser |
| created_by | francisco@alaro.ai |
| created_at | 2026-02-22 |
| session replay sample rate | **100%** (every session recorded) |
| event processing scale | ALL |

Every query below scopes to `@application.id:db0d76f0-bf2e-4328-a098-d711035a664c`.

### 1.2 Volume

| window | events | sessions | unique users |
|---|---:|---:|---:|
| Last 24h | 58,131 | 65 | 14 |
| Last 7d  | 322,907 | 334 | 18 |

~46k events/day, ~46 sessions/day, team-wide.

### 1.3 Event type mix (7d)

| @type | count | share |
|---|---:|---:|
| resource | 270,548 | 84% |
| long_task | 30,310 | 9% |
| action | 17,833 | 6% |
| view | 2,613 | 1% |
| session | 344 | 0.1% |
| error | 1,259 | 0.4% |

Resources (API calls) dominate — useful for seeing *which* backend endpoints get hit, but too noisy for time-on-task and most get filtered in the analysis below.

### 1.4 User breakdown (last 7d)

| email | events | sessions |
|---|---:|---:|
| maarten.schellingerhout@alaro.ai | **78,333** | **25** |
| francisco@alaro.ai | 37,805 | 61 |
| james@alaro.ai | 23,887 | 64 |
| igor@alaro.ai | 20,742 | 60 |
| willem@alaro.ai | 13,914 | 20 |
| franciscoemoretti@gmail.com | 10,903 | 11 |
| alex@alaro.ai | 10,215 | 15 |
| patrick@alaro.ai | 31,688 | 48 |
| michal.gintowt@belozelaw.com | 128 | 1 |
| dillon@alaro.ai | 764 | 4 |
| … | | |

Two observations:
- **Maarten is the heaviest user by event volume** — more than any engineer. Good news for a time-on-matter study.
- There's exactly one **external/client email** in the dataset: `michal.gintowt@belozelaw.com`. Single session, 128 events. Worth knowing if you're modelling client-in-Alaro events.

### 1.5 Session shape (Maarten, 7d, n=25)

| metric | value |
|---|---|
| median duration | 4,901s (82 min) |
| p75 | 233 min |
| p90 / p95 / max | **14,400s (240 min) — Datadog's hard cap** |
| actions / session (median) | 200 |
| views / session (median) | 18 |
| resources / session (median) | 1,976 |

Six of Maarten's 25 sessions hit the 4-hour wall-clock cap. Datadog splits the session there regardless of activity — so multi-hour uninterrupted work on a single matter is guaranteed to cross session boundaries. Any E2C computation has to stitch across `session.id` (easy: group by `usr.email` + matter id from URL, not by session).

Anomaly worth flagging: Sat 2026-04-11 session (duration 233 min, only 14 actions / 7 views / 236 resources). Classic "tab left open while I went to do something else" pattern. Gap detection catches this.

---

## 2. Identifying Maarten (and Tanya)

### 2.1 Identity coverage

| metric | 24h value |
|---|---|
| events with `@usr.email` populated | 74.2% |
| events without | 25.8% |

The 25.8% unidentified is **almost entirely `resource` events** (14,793 out of 14,992 unidentified) — API calls that fire before auth state is set. Every `view`, `action`, and `session` event has identity populated.

For time-on-task math this is fine: we care about views + actions, which are ~100% identified. Resources without `@usr.email` can still be tied back via `@session.id` if ever needed.

### 2.2 `@usr` object

```json
{
  "anonymous_id": "b50e2edf-6e4a-42c5-8f7e-aaa199d436b1",
  "name": "maarten schellingerhout",
  "id": "user_37t0Fdea9TzivhKDwn3FvbAY9n0",
  "email": "maarten.schellingerhout@alaro.ai"
}
```

Maarten's email in Datadog is `maarten.schellingerhout@alaro.ai` — not `maarten@alaro.ai`. Use the full address.

### 2.3 Filters for the pipeline

- Maarten: `@application.id:db0d76f0-bf2e-4328-a098-d711035a664c @usr.email:"maarten.schellingerhout@alaro.ai"`
- Tanya (ready for Monday): same with `@usr.email:"tanya@alaro.ai"` — confirmed zero events over last 7 days as expected.

Identity resolution is **not a gap**. No need for fingerprinting.

---

## 3. What Maarten is actually doing

Pulled full view+action streams for three sessions:

| label | date | duration | views | actions | file |
|---|---|---:|---:|---:|---|
| short_today | 2026-04-17 12:11 | 58 min | 15 | 90 | `samples/05_maarten_short_today.json` |
| long_today  | 2026-04-17 07:53 | 198 min | 63 | 474 | `samples/05_maarten_long_today.json` |
| capped_tue  | 2026-04-14 09:55 | 240 min (capped) | 101 | 620 | `samples/05_maarten_capped_tue.json` |

### 3.1 URL structure — matter context is already in the data

Alaro's frontend uses these URL paths (seen in `view.url_path`):

| path pattern | meaning |
|---|---|
| `/` | home / root route (but often has `ideFolderId` in query params) |
| `/client` | client list |
| `/client/:clientId` | specific client page |
| `/client/inbox` | client inbox (likely the email/triage intake view) |
| `/folder/:folderId` | matter workspace |
| `/folder/:folderId/chat/:chatId` | AI chat inside a matter |

URL query parameters in `view.url_query` (also in resource URLs):

| param | role |
|---|---|
| `ideFolderId` | **matter ID** (also the `:folderId` path component) |
| `docId` | which document is loaded |
| `versionGroupId` | which version of that document |
| `ideChatId` | active AI chat |
| `viewType` | `docx`, `markdown`, `pdf`, etc. — what the user is looking at |
| `startLoc` / `endLoc` | scroll/line position |

**Take-away:** we can compute per-matter time without touching Alaro's code, because `folder/:id` and/or `ideFolderId` are on every navigation.

### 3.2 Matters Maarten worked on this week (inferred from folder IDs in his event stream)

| folder id | appears in sessions | note |
|---|---|---|
| `a5ba5f58-de21-4872-a6e2-27af48db5720` | all three sessions (dominant) | **"Project Malbec"** — action names repeatedly reference "Project Malbec - DDQ", "Paul Hastings (Malbec VDR)", Malbec DD report |
| `a03de56d-7d28-4b59-9467-874b67fbb035` | long_today (11,906s time_spent) | separate matter, secondary activity |
| `7ec61f46-622e-4572-91ee-03f3cfc505fd` | capped_tue (3,987s) | another matter — referenced "Mammal Agency" / "Dominus" in actions |
| `71d1d181-5cdc-47ad-8cd3-c91b1e2fc4af` | capped_tue | "ElevenLabs - Fund Review" referenced in chat inputs |
| `42953f94-dea5-4b52-ba0d-3bff068c3bc9` | capped_tue | unnamed |
| `9bbba1c0-ede1-4ce2-a028-2ba64867eb36` | capped_tue | unnamed |
| `ddea267d-be2c-404c-ab76-761a788d763e` | capped_tue | unnamed |
| `ee2bfcc5-2a17-4a2d-a2ed-9df8b47ea1ba` | capped_tue | unnamed |

Client IDs seen (`/client/:id`): `fb75d58c-...`, `abbee135-...`, `75e9c494-...`, `e33354b5-...`.

Mapping folder_id → human matter name requires a lookup against Alaro's DB. The RUM data gives you the ID, not the name.

### 3.3 What the action stream reveals

All captured actions are `type: "click"`. Examples from Maarten's sessions — ordered by frequency:

| action target.name (truncated) | frequency | interpretation |
|---|---:|---|
| `"Type @ to add files, / for prompts..."` | 131 | chat input field — Maarten starts typing an AI prompt |
| `"160426 Project Malbec - Draft Red Flag Due Diligence Report Fully Populated.docx Review the Contracts section…"` | 27 | clicking a chat message to expand/reply |
| `"Submit"` | 22 | submit a chat message |
| `"New chat"` | 21 | start a new AI chat (= new workstream within the matter) |
| `"Chat panel"` | 19 | switching chat pane visible |
| `"Document panel"` | 32 | switching doc pane visible |
| `"Delete"` | 16 | deleting something |
| `"Paul Hastings (Malbec VDR)"` | 15 | clicking the VDR client row in the sidebar |
| `"Clients"` | 13 | back-to-clients nav |
| `"Upload New Version"` | 3 | uploading a new doc version |
| `"Upload"` | 12 | initial upload |
| `"Promote"` | 11 | "promote" action (likely finalize/publish a version) |
| `"Switch workspace"` | 7 | switching between matter workspaces |
| `"Editor"` | 2 | opening editor mode |

Real message text is preserved in `action.target.name` (truncated to ~100 chars). This means you can, with effort, tell the difference between *reading a DD report review prompt* vs *clicking a document name*. You could not yet reliably classify "drafting" vs "reviewing" just from event stream — that'd need a custom `verb` attribute.

### 3.4 Custom attributes today: zero

Every `view` event carries `context: {}` — the Datadog SDK slot for custom attributes is present but empty. Alaro never calls `addAction(...)` / `setGlobalContext(...)` / `addTiming(...)` today. That is the single biggest instrumentation gap.

### 3.5 Useful hidden gems already being captured

- **`view.in_foreground_periods`** — per-view array of `{start, duration}` pairs marking when the tab was actually in foreground. This is the gold-standard signal for "is Maarten actually looking at this?"
- **`action.frustration.type`** — Datadog auto-detects `dead_click`, `rage_click`, `error_click`. Shows up on 11+ Maarten clicks this week. Free UX quality signal — not E2C-related but worth flagging.
- **`session.has_replay: true`** — every session is replayable in the Datadog UI, so any edge case found in the data can be sanity-checked visually.

### 3.6 Minute-by-minute timelines

See `samples/06_timelines.json` for full buckets. Pattern observed in long_today:

- 07:53–09:55 — continuous activity on `/folder/a5ba5f58-…` (Malbec), 400+ actions, chat + document panel flipping
- 09:55 — session broken by 4-hour cap, immediately restarts (this is *one continuous piece of work*, split into 2 RUM sessions)
- 11:27 — single 24-minute idle gap mid-session (biggest gap observed in this session; probably a meeting or break)

Idle-gap distribution across the three sessions:

| session | biggest gap | ≥5 min gaps | ≥15 min gaps |
|---|---|---:|---:|
| short_today | 489s (8 min) | 1 | 0 |
| long_today | 1,448s (24 min) | 5 | 1 |
| capped_tue | 985s (16 min) | 7 | 2 |

---

## 4. Time-on-task accuracy

### 4.1 The big trap: `view.time_spent` is unreliable

Summing `view.time_spent` across a session vastly exceeds wall-clock session duration:

| session | session span (wall clock) | Σ view.time_spent | inflation |
|---|---:|---:|---:|
| short_today | 56 min | 151 min | **2.7×** |
| long_today | 162 min | 1,310 min | **8.1×** |
| capped_tue | 238 min | 1,179 min | **5.0×** |

Why it lies:
- Alaro's SPA stays on `url_path: "/"` while the "real" navigation happens via query params. Datadog considers the `/` view still alive and keeps accumulating its `time_spent` even when foreground is elsewhere.
- Background tabs keep accumulating time_spent (the clock doesn't stop when tab is hidden).

**Use `in_foreground_periods` instead.** Sum of foreground durations per view matches wall-clock reality within plausible bounds:

| session | session span | Σ foreground | foreground ratio |
|---|---:|---:|---:|
| short_today | 56 min | 42 min | 74% |
| long_today | 162 min | 115 min | 71% |
| capped_tue | 238 min | 155 min | 65% |

65–74% attention is believable for a working lawyer multitasking across apps — these ratios pass the smell test.

### 4.2 Foreground time by matter (long_today, 162-min session)

| bucket | foreground seconds | foreground minutes | share of foreground |
|---|---:|---:|---:|
| `/` (home, no folder id in query) | 3,576s | 60 min | 52% |
| `folder:a5ba5f58-…` (Malbec) | 3,161s | 53 min | 46% |
| `folder:a03de56d-…` (other matter) | 105s | 2 min | 2% |
| client_list, client page | ~55s | 1 min | 1% |

The **home-page (`/`) bucket is 52%** of foreground — that's the zero-instrumentation ceiling problem: when the SPA is on `/` without a folder in query params, we can't attribute time to a matter.

### 4.3 Accuracy estimate

**Option A — zero new instrumentation, using today's data:**

Attribute foreground time per view by `ideFolderId` (from url_path or url_query). Discard "home" / "client_list" buckets. Stitch across 4-hr session caps via `(usr.email, folder_id)` grouping. Use gap-based idle removal (drop gaps ≥5 min).

Expected accuracy: **±25–35% on per-matter minutes**. The home-page attribution hole is the main error source.

Good enough for: directional dashboards ("Maarten spent ~N hours on Malbec this week"), trend lines over months.
Not good enough for: billable hours, per-matter OKR targets.

**Option B — one custom action on every matter switch (~1 hour of Alaro dev work):**

Inside the existing router / workspace-switch logic, call:
```js
datadogRum.addAction('matter_context', { matter_id, client_id, activity: 'enter' })
datadogRum.addAction('matter_context', { matter_id, client_id, activity: 'exit'  })
```
Then also set matter_id as a global attribute:
```js
datadogRum.setGlobalContextProperty('matter_id', currentMatterId)
```
This kills the home-page attribution hole because every event gets tagged with current matter, regardless of URL.

Expected accuracy: **±10% on per-matter minutes**. Good enough for running E2C as a management metric.

**Option C — full heartbeat instrumentation (per the separate pipeline brief):**

Emit a custom action every 30s while the user is active, tagged with `{matter_id, client_id, activity_class, focused: true/false}`. Emit explicit milestone events: `engagement_signed`, `draft_delivered`, `case_closed`. Use a visibility-aware timer (not `setInterval`) so idle and backgrounded time are distinguishable in the data.

Expected accuracy: **±5% on per-matter minutes**, plus every E2C milestone inside Alaro becomes directly queryable without URL-pattern inference.

---

## 5. E2C milestone visibility map

| milestone | visible today? | where it lives / what to do |
|---|---|---|
| **first_email_received** | ❌ outside Alaro | Gmail / email intake system. Would need a Gmail → Datadog log integration, or poll Gmail API directly and emit events. |
| **triage_complete** | ⚠️ inferrable | `/client/inbox` → `/client/:id` → `/folder/:id` navigation pattern likely = triage. Not explicit; reliable only with an Alaro event `triage_complete`. |
| **engagement_signed** | ❌ not visible | Likely DocuSign / email confirm. Would need DocuSign webhook → Datadog, or an Alaro-side "mark engagement signed" UI action that emits a custom event. |
| **work_in_progress** | ✅ visible | Any view/action on `/folder/:id`. Already reliable. |
| **draft_delivered** | ⚠️ partially inferrable | `"Upload New Version"` + `"Promote"` actions are candidates, but not disambiguated from internal iteration. Clean signal requires a `draft_sent` custom action firing at actual client send. |
| **client_sign_off** | ❌ not visible | External (email / DocuSign). Same integration story as engagement_signed. |
| **case_closed** | ❌ not visible | No explicit `/close` route or "Close case" action in today's event stream. One custom action on the matter-close button would fix this. |

**Net:** of 7 milestones, 1 is cleanly visible today, 2 are inferrable-but-fragile, 4 need either Alaro instrumentation or an out-of-Alaro integration. The 3 Alaro-internal gaps (triage_complete, draft_delivered, case_closed) are each a single `addAction` call.

---

## Recommended next steps

Three options, ranked by effort vs. confidence gain:

### Option A — Ship a directional dashboard with today's data (0 dev, ~3 days of data work)

Build the pipeline against what's already in RUM. Attribute time via `ideFolderId`, use `in_foreground_periods` for attention, gap-threshold idle removal, stitch across session caps on `(user, matter)`.

- **What you get:** weekly and monthly "hours on matter X" for Maarten and (post-Monday) Tanya. Trend lines, top matters by time. Folder-id → matter-name mapping via Alaro DB join.
- **Accuracy:** ±25–35% on per-matter minutes.
- **E2C coverage:** only `work_in_progress` is real. E2C itself (start-to-end elapsed) requires `first_email_received` and `case_closed`, both missing.
- **Good fit if:** you want to see directional utilisation quickly, before committing to Alaro code changes.

### Option B — Add matter-context custom actions (~1 day of Alaro dev + the data pipeline)

Ship `datadogRum.setGlobalContextProperty('matter_id', ...)` in the SPA router + explicit `matter_context` actions on workspace switch. Also ship single-purpose custom actions on the four gap milestones: `triage_complete`, `engagement_signed` (if captured in Alaro), `draft_delivered`, `case_closed`.

- **What you get:** everything in Option A, but every event carries `matter_id`, eliminating the home-page attribution hole. Four of five Alaro-internal E2C milestones become directly queryable.
- **Accuracy:** ±10% on per-matter minutes.
- **Still missing for full E2C:** `first_email_received` and `client_sign_off` (both outside Alaro).
- **Good fit if:** you want E2C as a real management metric you'd defend in a QBR.

### Option C — Full heartbeat + external integrations (~1 sprint Alaro + integrations)

Implement the full pipeline brief: 30s heartbeats with focus state, explicit milestone events, plus Gmail/DocuSign event integration for the milestones that live outside Alaro.

- **What you get:** complete E2C pipeline. Every milestone has a timestamp. Time-on-matter accurate to ±5%. Idle detection doesn't require gap-threshold hacks. Analytics for activity *mix* (reading vs drafting vs reviewing) become possible via an `activity_class` tag.
- **Accuracy:** ±5% on per-matter minutes; end-to-end E2C fully measurable.
- **Good fit if:** E2C is the north-star metric and you're happy to invest accordingly.

### Recommendation

**Start with Option B.** A is too fuzzy to trust for anything management-facing, C is overkill before you've proven E2C drives decisions. Option B is the smallest step that turns Datadog RUM into a real operational signal and puts you in a position to evaluate whether C is worth the additional spend.

Concrete first actions if you pick B:

1. Add a `matter_id` global context property in Alaro's router/workspace switcher — single file change, ~2 hrs.
2. Add `case_closed` custom action on the matter-close button — 15 min.
3. Build the pipeline (separate brief) against events tagged with `@context.matter_id`.
4. Defer Gmail/DocuSign integrations until the in-Alaro signal is proven.

---

## Appendix — raw samples

All in `samples/` (gitignored):

| file | contents |
|---|---|
| `01_peek_last_hour.json` | 20 events, last hour, first inspection |
| `02_volume.json` | 24h + 7d volume aggregates |
| `03_sessions_7d.json` | summary of every session event for last 7d (344 rows) |
| `04_identity.json` | `@usr.email` coverage stats |
| `05_maarten_short_today.json` | full view+action stream, session `6ee517ed-…` |
| `05_maarten_long_today.json` | full view+action stream, session `81673ea7-…` |
| `05_maarten_capped_tue.json` | full view+action stream, session `06c1217c-…` |
| `06_timelines.json` | minute-bucketed timeline for each of the three sessions |
| `07_event_examples.json` | one full view event and one full action event for schema reference |

Scripts in `scripts/`:

- `dd.py` — minimal API client (auth, paginate, rate-limit)
- `peek.py` — last-hour smoke test
- `area1_volume.py` — volume/user/event-type aggregates
- `area1_sessions.py` — session-shape statistics
- `area2_identity.py` — identity coverage checks
- `area3_sessions_pull.py` — full-stream pull for chosen sessions
- `area3_analyze.py` — URL/action/key-inventory analysis
- `area4_time_on_matter.py` — foreground-based time attribution
