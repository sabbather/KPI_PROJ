#!/usr/bin/env python3
"""
Fetch all Core Projects and Core Tasks, then gather their subtasks' due dates
and allocated effort.

Uses:
  - Core Projects custom item type: IEAGWGLXPIAHEHH3
  - Core Tasks    custom item type: IEAGWGLXPIAHEHEZ

Outputs a summary JSON with:
  - parent item id/title/type
  - each subtask id/title/due/effortAllocation

Usage:
  python fetch_core_subtasks.py [-o core_subtasks.json]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = os.getenv("WRIKE_BASE_URL", "https://app-eu.wrike.com/api/v4")
API_KEY = os.getenv("WRIKE_API_KEY")

CORE_PROJECT_TYPE = "IEAGWGLXPIAHEHH3"
CORE_TASK_TYPE = "IEAGWGLXPIAHEHEZ"

# Fields for parent items
TASK_PARENT_FIELDS = ["subTaskIds", "title", "customItemTypeId", "permalink"]
FOLDER_PARENT_FIELDS = ["title", "customItemTypeId", "permalink"]
# Fields for subtasks
SUBTASK_FIELDS = ["title", "dates", "effortAllocation", "permalink"]


def require_api_key() -> str:
    if not API_KEY:
        raise SystemExit("Missing WRIKE_API_KEY in environment")
    return API_KEY


def auth_headers() -> Dict[str, str]:
    return {"Authorization": f"bearer {require_api_key()}"}


def fetch_items(
    endpoint: str,
    custom_item_type_id: str,
    fields: Optional[List[str]] = None,
    page_size: int = 100,
) -> List[Dict[str, Any]]:
    """Fetch items (folders or tasks) filtered by custom item type with pagination."""
    url = f"{BASE_URL}/{endpoint}"
    base_params: Dict[str, Any] = {
        "pageSize": page_size,
        "customItemTypes": json.dumps([custom_item_type_id]),
    }

    # Try with provided fields, then without if rejected
    field_options: List[Optional[List[str]]] = [fields, None]
    items: List[Dict[str, Any]] = []

    for field_set in field_options:
        params = dict(base_params)
        if field_set:
            params["fields"] = json.dumps(field_set)
        next_token: Optional[str] = None
        items.clear()

        while True:
            if next_token:
                params["nextPageToken"] = next_token
            else:
                params.pop("nextPageToken", None)

            resp = requests.get(url, headers=auth_headers(), params=params, timeout=30)

            if resp.status_code == 400 and "fields" in resp.text.lower() and field_set:
                # try without fields
                break

            if resp.status_code != 200:
                raise SystemExit(
                    f"Failed to fetch {endpoint} (status {resp.status_code}): {resp.text}"
                )

            data = resp.json()
            items.extend(data.get("data", []))
            next_token = data.get("nextPageToken")
            if not next_token:
                return items

    return items


def fetch_tasks_by_ids(ids: List[str], fields: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Batch fetch tasks by ID (max 100 per request)."""
    tasks: List[Dict[str, Any]] = []
    for i in range(0, len(ids), 100):
        batch = ids[i : i + 100]
        ids_segment = ",".join(batch)
        params: Dict[str, Any] = {}
        if fields:
            params["fields"] = json.dumps(fields)
        url = f"{BASE_URL}/tasks/{ids_segment}"
        resp = requests.get(url, headers=auth_headers(), params=params, timeout=30)
        if resp.status_code != 200:
            raise SystemExit(f"Failed batch task fetch ({resp.status_code}): {resp.text}")
        tasks.extend(resp.json().get("data", []))
    return tasks


def fetch_tasks_for_folder(folder_id: str, fields: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Fetch tasks under a folder (descendants + subtasks)."""
    url = f"{BASE_URL}/folders/{folder_id}/tasks"
    field_options: List[Optional[List[str]]] = [fields, SUBTASK_FIELDS, None]
    for field_set in field_options:
        params: Dict[str, Any] = {
            "descendants": True,
            "subTasks": True,
            "pageSize": 100,
        }
        if field_set:
            params["fields"] = json.dumps(field_set)

        tasks: List[Dict[str, Any]] = []
        next_token: Optional[str] = None

        while True:
            if next_token:
                params["nextPageToken"] = next_token
            else:
                params.pop("nextPageToken", None)

            resp = requests.get(url, headers=auth_headers(), params=params, timeout=30)
            if resp.status_code == 400 and "fields" in resp.text.lower() and field_set:
                # try next field_set
                break
            if resp.status_code != 200:
                raise SystemExit(f"Failed to fetch tasks for folder {folder_id} ({resp.status_code}): {resp.text}")

            data = resp.json()
            tasks.extend(data.get("data", []))
            next_token = data.get("nextPageToken")
            if not next_token:
                return tasks

    return []


def collect_parent_items() -> List[Dict[str, Any]]:
    projects = fetch_items("folders", CORE_PROJECT_TYPE, fields=FOLDER_PARENT_FIELDS)
    tasks = fetch_items("tasks", CORE_TASK_TYPE, fields=TASK_PARENT_FIELDS)
    return projects + tasks


def build_subtask_summary(
    parent: Dict[str, Any], children: List[Dict[str, Any]]
) -> Dict[str, Any]:
    return {
        "parent_id": parent.get("id"),
        "parent_title": parent.get("title"),
        "parent_type": "project" if parent.get("customItemTypeId") == CORE_PROJECT_TYPE else "task",
        "parent_permalink": parent.get("permalink"),
        "subtasks": children,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect Core projects/tasks and their subtasks' effort/due.")
    parser.add_argument("-o", "--out", default="core_subtasks.json", help="Output JSON path")
    args = parser.parse_args()

    parents = collect_parent_items()
    print(f"[INFO] Found {len(parents)} core parents (projects + tasks)")

    subtask_ids = sorted({sid for p in parents for sid in p.get("subTaskIds", [])})
    print(f"[INFO] Found {len(subtask_ids)} unique subtask IDs")

    subtasks_list = fetch_tasks_by_ids(subtask_ids, fields=SUBTASK_FIELDS) if subtask_ids else []
    subtasks_by_id = {t["id"]: t for t in subtasks_list}

    output = []
    for parent in parents:
        children: List[Dict[str, Any]] = []
        if parent.get("subTaskIds"):
            for sid in parent.get("subTaskIds", []):
                t = subtasks_by_id.get(sid)
                if not t:
                    continue
                children.append(
                    {
                        "id": t["id"],
                        "title": t.get("title"),
                        "due": t.get("dates", {}).get("due"),
                        "effortAllocation": t.get("effortAllocation"),
                        "permalink": t.get("permalink"),
                    }
                )
        elif parent.get("customItemTypeId") == CORE_PROJECT_TYPE:
            tasks_in_folder = fetch_tasks_for_folder(parent.get("id"), fields=SUBTASK_FIELDS)
            for t in tasks_in_folder:
                children.append(
                    {
                        "id": t["id"],
                        "title": t.get("title"),
                        "due": t.get("dates", {}).get("due"),
                        "effortAllocation": t.get("effortAllocation"),
                        "permalink": t.get("permalink"),
                    }
                )

        output.append(build_subtask_summary(parent, children))

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"[OK] Saved summary to {args.out}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
