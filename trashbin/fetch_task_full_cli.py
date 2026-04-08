#!/usr/bin/env python3
"""
Fetch full Wrike task data starting from a UI ID (e.g., 4397662421).
1) Converts UI ID to API ID.
2) Requests a broad, API-safe field set.
3) Saves pretty JSON to file.

Usage:
    python fetch_task_full_cli.py --ui-id 4397662421 [-o task_4397662421_full.json]
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

BASE_URL = "https://app-eu.wrike.com/api/v4"
DEFAULT_FIELDS: List[str] = [
    "customItemTypeId",
    "customFields",
    "finance",
    "billingType",
    "effortAllocation",
    "responsiblePlaceholderIds",
    "attachmentCount",
    "recurrent",
    "subTaskIds",
    "superTaskIds",
    "parentIds",
    "status",
    "importance",
    "permalink",
    "dates",
    "scope",
    "createdDate",
    "updatedDate",
    "dependencyIds",
    "description",
    "descriptionHtml",
    "responsibleIds",
    "authorId",
    "followerIds",
]

# Progressive fallbacks if Wrike rejects the fields parameter
SAFE_FIELDS: List[str] = [
    "customItemTypeId",
    "finance",
    "billingType",
    "effortAllocation",
    "responsiblePlaceholderIds",
    "attachmentCount",
    "recurrent",
    "subTaskIds",
    "superTaskIds",
    "parentIds",
    "status",
    "importance",
    "permalink",
    "dates",
    "scope",
    "createdDate",
    "updatedDate",
    "dependencyIds",
    "description",
    "descriptionHtml",
    "responsibleIds",
    "authorId",
    "followerIds",
]

MINIMAL_FIELDS: List[str] = [
    "customItemTypeId",
    "effortAllocation",
    "subTaskIds",
    "superTaskIds",
    "parentIds",
    "status",
    "permalink",
    "dates",
]

EFFORT_ONLY_FIELDS: List[str] = ["effortAllocation"]


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing environment variable {name}")
    return value


def convert_ui_to_api_id(ui_id: str, api_key: str) -> Optional[str]:
    url = f"{BASE_URL}/ids"
    headers = {"Authorization": f"bearer {api_key}"}
    params = {"type": "ApiV2Task", "ids": f"[{ui_id}]"}
    resp = requests.get(url, headers=headers, params=params, timeout=30)
    if resp.status_code != 200:
        sys.stderr.write(f"[ERROR] ID conversion failed ({resp.status_code}): {resp.text}\n")
        return None
    data = resp.json().get("data") or []
    return data[0].get("id") if data else None


def fetch_task(api_id: str, api_key: str, fields: List[str]) -> Optional[Dict[str, Any]]:
    headers = {"Authorization": f"bearer {api_key}"}
    url = f"{BASE_URL}/tasks/{api_id}"
    field_sets: List[Optional[List[str]]] = [fields, SAFE_FIELDS, MINIMAL_FIELDS, EFFORT_ONLY_FIELDS, None]

    for idx, field_set in enumerate(field_sets):
        params = {}
        label = "requested fields" if idx == 0 else f"fallback {idx}"
        if field_set is not None:
            params["fields"] = json.dumps(field_set)
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code == 200:
            data = resp.json().get("data") or []
            if data:
                if idx > 0:
                    print(f"[INFO] Used {label}: {field_set}")
                return data[0]
        elif resp.status_code == 400 and "fields" in resp.text.lower():
            # try next field set
            continue
        else:
            sys.stderr.write(f"[ERROR] Task fetch failed ({resp.status_code}) with {label}: {resp.text}\n")
            return None

    sys.stderr.write("[ERROR] All field sets rejected by API.\n")
    return None


def save_json(obj: Any, path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch full Wrike task details by UI ID.")
    parser.add_argument("--ui-id", required=True, help="UI task ID (e.g., 4397662421)")
    parser.add_argument("-o", "--out", help="Output JSON path (default: task_<ui-id>_full.json)")
    args = parser.parse_args()

    api_key = require_env("WRIKE_API_KEY")
    output_path = args.out or f"task_{args.ui_id}_full.json"

    api_id = convert_ui_to_api_id(args.ui_id, api_key)
    if not api_id:
        sys.stderr.write(f"[FATAL] Could not convert UI ID {args.ui_id} to API ID.\n")
        return 1

    task = fetch_task(api_id, api_key, DEFAULT_FIELDS)
    if not task:
        sys.stderr.write(f"[FATAL] No task data returned for API ID {api_id}.\n")
        return 1

    save_json(task, output_path)
    print(f"Saved full task data (UI {args.ui_id} -> API {api_id}) to: {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
