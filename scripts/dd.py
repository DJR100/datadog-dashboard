"""Minimal Datadog API client for the RUM exploration.

Read-only. Uses DD_API_KEY / DD_APP_KEY / DD_SITE from .env at repo root.
"""
from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Iterator

import requests
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(REPO_ROOT / ".env")

SITE = os.environ.get("DD_SITE", "datadoghq.eu").strip()
API_KEY = os.environ["DD_API_KEY"].strip()
APP_KEY = os.environ["DD_APP_KEY"].strip()
BASE = f"https://api.{SITE}"

HEADERS = {
    "DD-API-KEY": API_KEY,
    "DD-APPLICATION-KEY": APP_KEY,
    "Content-Type": "application/json",
}


def _request(method: str, path: str, *, params: dict | None = None, json_body: dict | None = None) -> dict:
    url = f"{BASE}{path}"
    for attempt in range(6):
        r = requests.request(method, url, headers=HEADERS, params=params, json=json_body, timeout=60)
        if r.status_code == 429:
            reset = float(r.headers.get("X-RateLimit-Reset", "5"))
            time.sleep(min(reset, 30))
            continue
        if r.status_code >= 500:
            time.sleep(2 ** attempt)
            continue
        if not r.ok:
            raise RuntimeError(f"{method} {path} → {r.status_code}: {r.text[:500]}")
        return r.json()
    raise RuntimeError(f"{method} {path} failed after retries")


def list_rum_applications() -> list[dict]:
    """GET /api/v2/rum/applications — list all RUM apps in the org."""
    data = _request("GET", "/api/v2/rum/applications")
    return data.get("data", [])


def search_rum_events(
    query: str,
    *,
    frm: str = "now-1h",
    to: str = "now",
    limit: int = 100,
    sort: str = "-timestamp",
    max_pages: int = 1,
) -> Iterator[dict]:
    """POST /api/v2/rum/events/search — paginated event iterator.

    frm / to accept absolute timestamps (ms epoch) or relative like 'now-1h'.
    Yields individual event dicts. Stops after max_pages.
    """
    cursor: str | None = None
    for _ in range(max_pages):
        body = {
            "filter": {"from": frm, "to": to, "query": query},
            "page": {"limit": limit},
            "sort": sort,
        }
        if cursor:
            body["page"]["cursor"] = cursor
        data = _request("POST", "/api/v2/rum/events/search", json_body=body)
        for ev in data.get("data", []):
            yield ev
        cursor = (data.get("meta", {}).get("page") or {}).get("after")
        if not cursor:
            return


def aggregate_rum(
    compute: list[dict],
    *,
    query: str = "",
    group_by: list[dict] | None = None,
    frm: str = "now-1h",
    to: str = "now",
) -> dict:
    """POST /api/v2/rum/analytics/aggregate — for counts/group-by without pulling events."""
    body: dict[str, Any] = {
        "filter": {"from": frm, "to": to, "query": query},
        "compute": compute,
    }
    if group_by:
        body["group_by"] = group_by
    return _request("POST", "/api/v2/rum/analytics/aggregate", json_body=body)


if __name__ == "__main__":
    # Smoke test: list applications
    import json

    apps = list_rum_applications()
    print(f"Found {len(apps)} RUM applications:")
    for a in apps:
        attrs = a.get("attributes", {})
        print(f"  - id={a.get('id')}  name={attrs.get('name')!r}  type={attrs.get('type')}")
    print()
    print(json.dumps(apps, indent=2)[:2000])
