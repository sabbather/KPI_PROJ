#!/usr/bin/env python3
"""
Streamlit dashboard for monitoring Wrike KPI's for Core Projects and Core Tasks.

Environment (.env):
  WRIKE_API_KEY=...
  WRIKE_BASE_URL=https://app-eu.wrike.com/api/v4         # optional
  WRIKE_CLIENT_PROJECTS_FOLDER_ID=<folder with client projects>
  WRIKE_CORE_TASK_TYPE_ID=IEAGWGLXPIAHEHEZ               # optional override
  WRIKE_CORE_PROJECT_TYPE_ID=IEAGWGLXPIAHEHH3            # optional override
  WRIKE_PLANNED_EFFORT_FIELD_ID=IEAGWGLXJUALG3VY         # optional override
  WRIKE_COMPLETED_STATUS_ID=IEAGWGLXJMGYX4ND             # optional override
  KONSTRUKTORZY=JK,MP,AW                                # optional seed for constructor initials
"""

from __future__ import annotations

import json
import math
import os
import hashlib
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from functools import lru_cache
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple
import re
from pathlib import Path
from urllib.parse import quote

import altair as alt
import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv

load_dotenv()


# ---- Config (overridable in sidebar) ---------------------------------------
DEFAULT_BASE_URL = os.getenv("WRIKE_BASE_URL", "https://app-eu.wrike.com/api/v4")
DEFAULT_API_KEY = os.getenv("WRIKE_API_KEY")
DEFAULT_CLIENT_FOLDER = os.getenv("WRIKE_CLIENT_PROJECTS_FOLDER_ID", "")
DEFAULT_CORE_TASK_TYPE = os.getenv("WRIKE_CORE_TASK_TYPE_ID", "IEAGWGLXPIAHEHEZ")
DEFAULT_BOM_TASK_TYPE = os.getenv("WRIKE_BOM_TASK_TYPE_ID", "")
DEFAULT_PLANNED_FIELD_ID = os.getenv("WRIKE_PLANNED_EFFORT_FIELD_ID", "IEAGWGLXJUALG3VY")
DEFAULT_COMPLETED_STATUS_ID = os.getenv("WRIKE_COMPLETED_STATUS_ID", "IEAGWGLXJMGYX4ND")

MS_GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
MS_SHAREPOINT_HOST = os.getenv("MS_SHAREPOINT_HOST", "pidpolska.sharepoint.com")
MS_SHAREPOINT_SITE_PATH = os.getenv("MS_SHAREPOINT_SITE_PATH", "/sites/Multicontrol")
MS_TENANT_ID = os.getenv("TENANT_ID", "")
MS_CLIENT_ID = os.getenv("CLIENT_ID", "")
MS_CLIENT_SECRET = os.getenv("KPI_MONITOR_SECRET", "")
POZYCJE_LIST_ID = os.getenv("POZYCJE_ID", "")
NAGLOWEK_LIST_ID = os.getenv("NAGLOWEK_ID", "")
MC_SITE_ID = os.getenv("DOK_OPER_ID", "")
POZYCJE_PART_FIELD = os.getenv("POZYCJE_PART_FIELD", "field_2")
POZYCJE_HEADER_REF_FIELD = os.getenv("POZYCJE_HEADER_REF_FIELD", "LinkTitle")
HEADER_ID_FIELD = os.getenv("HEADER_ID_FIELD", "id")
HEADER_ORDER_FIELD = os.getenv("HEADER_ORDER_FIELD", "LinkTitle")
WRIKE_COMMENTS_CACHE_TTL_SEC = int(os.getenv("WRIKE_COMMENTS_CACHE_TTL_SEC", "43200"))
WRIKE_DATA_CACHE_TTL_SEC = int(os.getenv("WRIKE_DATA_CACHE_TTL_SEC", "43200"))
KPI_AGG_CACHE_TTL_SEC = int(os.getenv("KPI_AGG_CACHE_TTL_SEC", "3600"))
SOLIDWORKS_CACHE_FILE = Path(".cache") / "solidworks_cache.json"
WRIKE_COMMENTS_CACHE_FILE = Path(".cache") / "wrike_comments_cache.json"
WRIKE_DATA_CACHE_FILE = Path(".cache") / "wrike_data_cache.json"
DAILY_HOURS_FILE = Path(".cache") / "daily_hours.json"
COMPLETED_OVERRIDES_FILE = Path(".cache") / "completed_overrides.json"
CONSTRUCTORS_FILE = Path(".cache") / "constructors.json"
CONSTRUCTOR_ASSIGNMENTS_FILE = Path(".cache") / "constructor_assignments.json"
REQUIRED_CONSTRUCTORS_RANGE_FILE = (
    Path(".cache") / "required_constructors_range.json"
)
DEFAULT_CONSTRUCTORS: List[str] = [
    item.strip().upper()
    for item in os.getenv("KONSTRUKTORZY", "").replace(";", ",").split(",")
    if item.strip()
]


def load_completed_overrides() -> Dict[str, date]:
    data = _read_json_file(COMPLETED_OVERRIDES_FILE, {})
    if not isinstance(data, dict):
        return {}
    result: Dict[str, date] = {}
    for tid, v in data.items():
        try:
            result[tid] = date.fromisoformat(str(v))
        except (ValueError, TypeError):
            pass
    return result


def save_completed_overrides(overrides: Dict[str, date]) -> None:
    _write_json_file(COMPLETED_OVERRIDES_FILE, {tid: d.isoformat() for tid, d in overrides.items()})


# ---- Logging ---------------------------------------------------------------
def _init_logs() -> None:
    if "logs" not in st.session_state:
        st.session_state["logs"] = []


def log(msg: str) -> None:
    _init_logs()
    st.session_state["logs"].append(msg)


def reset_logs() -> None:
    st.session_state["logs"] = []


def _now_ts() -> int:
    return int(time.time())


def _read_json_file(path: Path, fallback: Any) -> Any:
    try:
        if not path.exists():
            return fallback
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        log(f"Failed reading cache file {path}: {exc}")
        return fallback


def _write_json_file(path: Path, payload: Any) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as exc:  # noqa: BLE001
        log(f"Failed writing cache file {path}: {exc}")


# ---- Constructors & resource assignments -----------------------------------
def parse_constructors(value: str) -> List[str]:
    """Parse comma/space separated initials into a unique, uppercase list."""
    raw = value.replace(";", ",").replace("\n", ",")
    items: List[str] = []
    for chunk in raw.split(","):
        for token in chunk.split():
            token = token.strip().upper()
            if token and token not in items:
                items.append(token)
    return items


def load_constructors() -> List[str]:
    payload = _read_json_file(CONSTRUCTORS_FILE, None)
    if isinstance(payload, dict):
        payload = payload.get("constructors")
    if isinstance(payload, list):
        parsed = []
        for item in payload:
            token = str(item).strip().upper()
            if token and token not in parsed:
                parsed.append(token)
        if parsed:
            return parsed
    return list(DEFAULT_CONSTRUCTORS)


def save_constructors(constructors: List[str]) -> None:
    _write_json_file(CONSTRUCTORS_FILE, {"constructors": constructors})


def load_required_constructors_range() -> Optional[Tuple[date, date, bool]]:
    payload = _read_json_file(REQUIRED_CONSTRUCTORS_RANGE_FILE, None)
    if not isinstance(payload, dict):
        return None
    try:
        start = date.fromisoformat(str(payload.get("start")))
        end = date.fromisoformat(str(payload.get("end")))
    except (TypeError, ValueError):
        return None
    if end < start:
        return None
    return start, end, bool(payload.get("manual", False))


def save_required_constructors_range(
    start: date, end: date, manual: bool = True
) -> None:
    _write_json_file(
        REQUIRED_CONSTRUCTORS_RANGE_FILE,
        {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "manual": bool(manual),
        },
    )


def _persist_required_range_from_state() -> None:
    st.session_state["required_range_manual"] = True
    start = st.session_state.get("required_range_start")
    end = st.session_state.get("required_range_end")
    if isinstance(start, date) and isinstance(end, date) and start <= end:
        save_required_constructors_range(start, end, True)


def _reset_required_range_start(default_start: date, default_end: date) -> None:
    st.session_state["required_range_start"] = default_start
    end = st.session_state.get("required_range_end")
    if isinstance(end, date) and default_start <= end:
        manual = end != default_end
        st.session_state["required_range_manual"] = manual
        save_required_constructors_range(default_start, end, manual)


def _reset_required_range_end(default_end: date, default_start: date) -> None:
    st.session_state["required_range_end"] = default_end
    start = st.session_state.get("required_range_start")
    if isinstance(start, date) and start <= default_end:
        manual = start != default_start
        st.session_state["required_range_manual"] = manual
        save_required_constructors_range(start, default_end, manual)


def load_constructor_assignments() -> List[Dict[str, Any]]:
    payload = _read_json_file(CONSTRUCTOR_ASSIGNMENTS_FILE, {"assignments": []})
    if isinstance(payload, dict):
        payload = payload.get("assignments")
    if not isinstance(payload, list):
        return []
    cleaned: List[Dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        days = item.get("days")
        if not isinstance(days, dict):
            days = {}
        cleaned_days: Dict[str, float] = {}
        for day_str, hours in days.items():
            try:
                day_value = date.fromisoformat(str(day_str))
                hours_value = float(hours)
            except (TypeError, ValueError):
                continue
            if hours_value > 0:
                cleaned_days[day_value.isoformat()] = round(hours_value, 2)
        if not cleaned_days:
            continue
        requested_hours = item.get("requested_hours")
        try:
            requested_hours = (
                float(requested_hours) if requested_hours is not None else None
            )
        except (TypeError, ValueError):
            requested_hours = None
        cleaned.append(
            {
                "id": str(item.get("id") or uuid.uuid4().hex),
                "kind": "external" if item.get("kind") == "external" else "project",
                "project_id": item.get("project_id"),
                "project": str(item.get("project") or ""),
                "label": str(item.get("label") or item.get("project") or ""),
                "constructor": str(item.get("constructor") or "").upper(),
                "start_date": str(item.get("start_date") or min(cleaned_days)),
                "end_date": str(item.get("end_date") or max(cleaned_days)),
                "mode": str(item.get("mode") or "custom"),
                "hours_per_day": item.get("hours_per_day"),
                "requested_hours": requested_hours,
                "days": cleaned_days,
                "created_at": item.get("created_at"),
            }
        )
    return cleaned


def save_constructor_assignments(assignments: List[Dict[str, Any]]) -> None:
    _write_json_file(CONSTRUCTOR_ASSIGNMENTS_FILE, {"assignments": assignments})


def _init_resource_state() -> None:
    if "constructors" not in st.session_state:
        st.session_state["constructors"] = load_constructors()
    if "constructor_assignments" not in st.session_state:
        st.session_state["constructor_assignments"] = load_constructor_assignments()
    if "assignments_dirty" not in st.session_state:
        st.session_state["assignments_dirty"] = False
    if "assignment_selection" not in st.session_state:
        st.session_state["assignment_selection"] = set()
    if "assign_editor_nonce" not in st.session_state:
        st.session_state["assign_editor_nonce"] = 0
    if "effective_daily_hours" not in st.session_state:
        try:
            st.session_state["effective_daily_hours"] = float(
                _read_json_file(DAILY_HOURS_FILE, 6.0)
            )
        except (TypeError, ValueError):
            st.session_state["effective_daily_hours"] = 6.0
    if (
        "required_range_start" not in st.session_state
        or "required_range_end" not in st.session_state
    ):
        persisted_required_range = load_required_constructors_range()
        if persisted_required_range is not None:
            persisted_start, persisted_end, persisted_manual = (
                persisted_required_range
            )
            st.session_state.setdefault("required_range_start", persisted_start)
            st.session_state.setdefault("required_range_end", persisted_end)
            st.session_state.setdefault(
                "required_range_manual", persisted_manual
            )
    if "required_range_manual" not in st.session_state:
        st.session_state["required_range_manual"] = False



def _init_perf_metrics() -> None:
    if "perf_metrics" not in st.session_state:
        st.session_state["perf_metrics"] = {
            "wrike_comments_cache_hit": 0,
            "wrike_comments_cache_miss": 0,
            "kpi_aggregate_cache_hit": 0,
            "kpi_aggregate_cache_miss": 0,
            "stage_times": {},
        }


def reset_perf_metrics() -> None:
    st.session_state["perf_metrics"] = {
        "wrike_comments_cache_hit": 0,
        "wrike_comments_cache_miss": 0,
        "kpi_aggregate_cache_hit": 0,
        "kpi_aggregate_cache_miss": 0,
        "stage_times": {},
    }


def _inc_perf_metric(name: str, delta: int = 1) -> None:
    _init_perf_metrics()
    st.session_state["perf_metrics"][name] = st.session_state["perf_metrics"].get(name, 0) + delta


def _set_stage_time(name: str, seconds: float) -> None:
    _init_perf_metrics()
    st.session_state["perf_metrics"]["stage_times"][name] = round(seconds, 3)


def _init_wrike_caches() -> None:
    if st.session_state.get("wrike_cache_loaded"):
        return
    comments_payload = _read_json_file(WRIKE_COMMENTS_CACHE_FILE, {})
    data_payload = _read_json_file(WRIKE_DATA_CACHE_FILE, {})
    st.session_state["wrike_comments_cache"] = comments_payload if isinstance(comments_payload, dict) else {}
    st.session_state["wrike_data_cache"] = data_payload if isinstance(data_payload, dict) else {}
    st.session_state["wrike_cache_loaded"] = True


def _persist_wrike_comments_cache() -> None:
    _init_wrike_caches()
    _write_json_file(WRIKE_COMMENTS_CACHE_FILE, st.session_state.get("wrike_comments_cache", {}))


def _persist_wrike_data_cache() -> None:
    _init_wrike_caches()
    _write_json_file(WRIKE_DATA_CACHE_FILE, st.session_state.get("wrike_data_cache", {}))


def _wrike_cache_get(bucket: str, key: str, ttl_sec: int) -> Optional[Any]:
    _init_wrike_caches()
    cache_name = "wrike_comments_cache" if bucket == "comments" else "wrike_data_cache"
    cache_dict = st.session_state.get(cache_name, {})
    entry = cache_dict.get(key)
    if not isinstance(entry, dict):
        return None
    cached_at = entry.get("cached_at")
    if not isinstance(cached_at, (int, float)):
        return None
    if _now_ts() - int(cached_at) > ttl_sec:
        return None
    return entry.get("data")


def _wrike_cache_set(bucket: str, key: str, value: Any) -> None:
    _init_wrike_caches()
    cache_name = "wrike_comments_cache" if bucket == "comments" else "wrike_data_cache"
    cache_dict = st.session_state.get(cache_name, {})
    cache_dict[key] = {"cached_at": _now_ts(), "data": value}
    st.session_state[cache_name] = cache_dict
    if bucket == "comments":
        _persist_wrike_comments_cache()
    else:
        _persist_wrike_data_cache()


def _init_kpi_aggregate_cache() -> None:
    if "kpi_aggregate_cache" not in st.session_state:
        st.session_state["kpi_aggregate_cache"] = {}


def _kpi_aggregate_get(cache_key: str) -> Optional[Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]]:
    _init_kpi_aggregate_cache()
    entry = st.session_state["kpi_aggregate_cache"].get(cache_key)
    if not isinstance(entry, dict):
        return None
    cached_at = entry.get("cached_at")
    if not isinstance(cached_at, (int, float)):
        return None
    if _now_ts() - int(cached_at) > KPI_AGG_CACHE_TTL_SEC:
        return None
    task_df = entry.get("task_df")
    bom_df = entry.get("bom_df")
    summary = entry.get("summary")
    if not isinstance(task_df, pd.DataFrame) or not isinstance(bom_df, pd.DataFrame) or not isinstance(summary, dict):
        return None
    return task_df.copy(), bom_df.copy(), dict(summary)


def _kpi_aggregate_set(
    cache_key: str, task_df: pd.DataFrame, bom_df: pd.DataFrame, summary: Dict[str, Any]
) -> None:
    _init_kpi_aggregate_cache()
    st.session_state["kpi_aggregate_cache"][cache_key] = {
        "cached_at": _now_ts(),
        "task_df": task_df.copy(),
        "bom_df": bom_df.copy(),
        "summary": dict(summary),
    }


def _init_solidworks_caches() -> None:
    if not st.session_state.get("solidworks_cache_loaded"):
        loaded_items: Dict[str, Dict[str, Any]] = {}
        loaded_orders: Dict[str, Optional[str]] = {}
        try:
            if SOLIDWORKS_CACHE_FILE.exists():
                payload = json.loads(SOLIDWORKS_CACHE_FILE.read_text(encoding="utf-8"))
                loaded_items = payload.get("items", {}) or {}
                loaded_orders = payload.get("orders", {}) or {}
                log(
                    f"Loaded Solidworks cache from disk: "
                    f"items={len(loaded_items)}, orders={len(loaded_orders)}"
                )
        except Exception as exc:  # noqa: BLE001
            log(f"Failed to load Solidworks cache from disk: {exc}")
        st.session_state["solidworks_item_cache"] = loaded_items
        st.session_state["solidworks_order_pdf_cache"] = loaded_orders
        st.session_state["solidworks_cache_loaded"] = True
        return
    if "solidworks_item_cache" not in st.session_state:
        st.session_state["solidworks_item_cache"] = {}
    if "solidworks_order_pdf_cache" not in st.session_state:
        st.session_state["solidworks_order_pdf_cache"] = {}


def _solidworks_item_cache() -> Dict[str, Dict[str, Any]]:
    _init_solidworks_caches()
    return st.session_state["solidworks_item_cache"]


def _solidworks_order_pdf_cache() -> Dict[str, Optional[str]]:
    _init_solidworks_caches()
    return st.session_state["solidworks_order_pdf_cache"]


def comments_signature(comments: List[Dict[str, Any]]) -> str:
    relevant = []
    for comment in comments:
        relevant.append(
            {
                "id": comment.get("id"),
                "updatedDate": comment.get("updatedDate"),
                "text": comment.get("text"),
            }
        )
    payload = json.dumps(relevant, ensure_ascii=False, sort_keys=True)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def persist_solidworks_cache() -> None:
    _init_solidworks_caches()
    try:
        SOLIDWORKS_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "items": st.session_state.get("solidworks_item_cache", {}),
            "orders": st.session_state.get("solidworks_order_pdf_cache", {}),
        }
        SOLIDWORKS_CACHE_FILE.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as exc:  # noqa: BLE001
        log(f"Failed to persist Solidworks cache: {exc}")


# regex used later to find hyphenated assembly numbers in Wrike comments
ASSEMBLY_NUMBER_RE = re.compile(r"\d{2,}-\d+")


@dataclass
class SolidworksContext:
    graph_token: Optional[str]
    part_number_map: Dict[str, str]
    header_map: Dict[str, str]

    def resolve_order_id(self, part_number: str) -> Optional[str]:
        normalized = normalize_part_number(part_number)
        if not normalized:
            return None
        item_id = self.part_number_map.get(normalized)
        if not item_id:
            return None
        return self.header_map.get(item_id)


def normalize_part_number(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = str(value).strip().upper()
    return normalized if normalized else None
# ---- Helpers ---------------------------------------------------------------
def iso_to_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        # Wrike returns without timezone sometimes
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def iso_to_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def extract_planned_hours(task: Dict[str, Any], planned_field_id: str) -> Optional[float]:
    for field in task.get("customFields", []):
        if field.get("id") == planned_field_id:
            try:
                raw = field.get("value")
                if raw is None:
                    return None
                if isinstance(raw, (int, float)):
                    return float(raw)
                if isinstance(raw, str):
                    # Handle "HH:MM" format returned by Wrike
                    if ":" in raw:
                        h, m = raw.split(":", 1)
                        return float(h) + float(m) / 60.0
                    return float(raw.replace(",", "."))
                return None
            except (TypeError, ValueError):
                return None
    return None


def effort_minutes_until(task: Dict[str, Any], cutoff: date) -> int:
    effort = task.get("effortAllocation") or {}
    total = 0
    for responsible in effort.get("responsibleAllocation", []) or []:
        for daily in responsible.get("dailyAllocation", []) or []:
            date_str = daily.get("date")
            if not date_str:
                continue
            try:
                alloc_date = date.fromisoformat(date_str)
            except ValueError:
                continue
            if alloc_date <= cutoff:
                minutes = daily.get("effortMinutes")
                if isinstance(minutes, (int, float)):
                    total += int(minutes)
    return total


def allocated_minutes(task: Dict[str, Any]) -> int:
    effort = task.get("effortAllocation", {}) or {}
    # Prefer allocatedEffort (minutes); fall back to totalEffort
    val = effort.get("allocatedEffort")
    if val is None:
        val = effort.get("totalEffort")
    return int(val or 0)


def _custom_status_id(task: Dict[str, Any]) -> Optional[str]:
    custom_status = task.get("customStatusId")
    if custom_status:
        return custom_status
    project_info = task.get("project")
    if isinstance(project_info, dict):
        return project_info.get("customStatusId")
    return None


def _completed_datetime(task: Dict[str, Any]) -> Optional[datetime]:
    completed = task.get("completedDate")
    if not completed:
        project_info = task.get("project")
        if isinstance(project_info, dict):
            completed = project_info.get("completedDate")
    return iso_to_datetime(completed)


def is_completed(task: Dict[str, Any], completed_status_id: str) -> bool:
    if not completed_status_id:
        return False
    return _custom_status_id(task) == completed_status_id


def completion_block(completed: int, planned: int, label: str):
    """Display completion ratio with colored large font."""
    if planned == 0:
        st.markdown(f"**{label}:** brak planowanych pozycji")
        return
    pct = int(round(completed / planned * 100)) if planned else 0
    if pct >= 100:
        color = "#2e7d32"
    elif pct >= 50:
        color = "#f9a825"
    else:
        color = "#c62828"
    st.markdown(
        f"<div style='font-size:32px;font-weight:700;color:{color};'>"
        f"{completed} / {planned} ({pct}%)</div>"
        f"<div style='color:#666;margin-bottom:12px;'>{label}</div>",
        unsafe_allow_html=True,
    )


def color_ratio(val):
    if pd.isna(val):
        return ""
    if val <= 80:
        return "background-color:#2e7d32; color:#ffffff"
    if val <= 110:
        return "background-color:#f9a825; color:#000000"
    return "background-color:#c62828; color:#ffffff"


def color_time(val):
    if pd.isna(val):
        return ""
    if val <= 60:
        return "background-color:#2e7d32; color:#ffffff"
    if val <= 100:
        return "background-color:#f9a825; color:#000000"
    return "background-color:#c62828; color:#ffffff"


def bool_symbol(val: Any) -> str:
    if val is True:
        return "✅"
    if val is False:
        return "🔴"
    return ""


def _to_date_only(value: Any) -> Optional[date]:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


def compute_kpi1_score(
    start_date: Any,
    due_date: Any,
    completed: bool,
    completed_date: Any,
    tolerance_pct: float,
) -> Optional[float]:
    """Score a BOM item's delivery time from 0 to 1; None means insufficient dates."""
    if not completed:
        return None

    start_day = _to_date_only(start_date)
    due_day = _to_date_only(due_date)
    if start_day is None or due_day is None:
        return None

    planned_days = (due_day - start_day).days
    if planned_days <= 0:
        return None

    effective_day = _to_date_only(completed_date)
    if effective_day is None:
        return None

    elapsed_days = max(0, (effective_day - start_day).days)
    if elapsed_days <= planned_days:
        return 1.0

    tolerance = max(0.0, float(tolerance_pct)) / 100.0
    if tolerance == 0:
        return 0.0

    zero_score_days = planned_days * (1.0 + tolerance)
    if elapsed_days >= zero_score_days:
        return 0.0

    return (zero_score_days - elapsed_days) / (zero_score_days - planned_days)


def weighted_hours_ratio(used_hours: pd.Series, planned_hours: pd.Series) -> float:
    """Return total used / total planned hours, ignoring missing or non-positive plans."""
    used = pd.to_numeric(used_hours, errors="coerce")
    planned = pd.to_numeric(planned_hours, errors="coerce")
    valid = used.notna() & planned.gt(0)
    planned_total = planned[valid].sum()
    if planned_total <= 0:
        return 0.0
    return round(float(used[valid].sum() / planned_total), 2)


def due_symbol(due_flag: Any, completed_flag: Any, completed_date: Any, due_date: Any) -> str:
    if due_flag is True and completed_flag is False:
        return "⚠️"
    if due_flag is True and completed_flag is True:
        completed_day = _to_date_only(completed_date)
        due_day = _to_date_only(due_date)
        if completed_day and due_day and completed_day <= due_day:
            return "✅"
        return "⚠️"
    return bool_symbol(due_flag)


def compute_daily_allocated_effort(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    for _, row in df.iterrows():
        start = row.get("start_date")
        due = row.get("due_date")
        planned = row.get("planned_hours")
        if pd.isna(start) or pd.isna(due) or pd.isna(planned) or planned <= 0:
            continue
        if not isinstance(start, date) or not isinstance(due, date):
            continue
        if due < start:
            continue
        wdays = [
            start + timedelta(days=i)
            for i in range((due - start).days + 1)
            if (start + timedelta(days=i)).weekday() < 5
        ]
        if not wdays:
            continue
        daily = planned / len(wdays)
        for d in wdays:
            rows.append({
                "date": d,
                "date_str": d.strftime("%d.%m.%Y"),
                "project": row.get("project", ""),
                "title": row.get("title", ""),
                "task_id": row.get("id", ""),
                "planned_hours": planned,
                "start_date": start,
                "due_date": due,
                "daily_hours": round(daily, 2),
            })
    return pd.DataFrame(rows)


# ---- Resource planning helpers ---------------------------------------------
WEEKDAY_LABELS = ["Pon", "Wt", "Śr", "Czw", "Pt", "Sob", "Nd"]


def business_days_between(start: date, end: date) -> List[date]:
    if end < start:
        return []
    return [
        start + timedelta(days=i)
        for i in range((end - start).days + 1)
        if (start + timedelta(days=i)).weekday() < 5
    ]


def expand_assignment_days(
    assignments: List[Dict[str, Any]],
    start: Optional[date] = None,
    end: Optional[date] = None,
) -> pd.DataFrame:
    columns = [
        "assignment_id",
        "kind",
        "project_id",
        "project",
        "label",
        "constructor",
        "date",
        "hours",
    ]
    rows: List[Dict[str, Any]] = []
    for assignment in assignments or []:
        days = assignment.get("days") or {}
        if not isinstance(days, dict):
            continue
        for day_str, hours in days.items():
            try:
                day_value = date.fromisoformat(str(day_str))
                hours_value = float(hours)
            except (TypeError, ValueError):
                continue
            if start is not None and day_value < start:
                continue
            if end is not None and day_value > end:
                continue
            if hours_value <= 0:
                continue
            rows.append(
                {
                    "assignment_id": assignment.get("id"),
                    "kind": "external" if assignment.get("kind") == "external" else "project",
                    "project_id": assignment.get("project_id"),
                    "project": assignment.get("project") or "",
                    "label": assignment.get("label") or assignment.get("project") or "",
                    "constructor": str(assignment.get("constructor") or "").upper(),
                    "date": day_value,
                    "hours": hours_value,
                }
            )
    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(rows)


def project_date_ranges(core_df: pd.DataFrame) -> Dict[str, Tuple[date, date]]:
    if core_df is None or core_df.empty or "project_id" not in core_df.columns:
        return {}
    work = core_df.copy()
    work["_start"] = work["start_date"].map(_to_date_only) if "start_date" in work else None
    work["_due"] = work["due_date"].map(_to_date_only) if "due_date" in work else None
    work = work.dropna(subset=["_start", "_due"])
    ranges: Dict[str, Tuple[date, date]] = {}
    for project_id, group in work.groupby("project_id"):
        if not project_id:
            continue
        start = min(group["_start"])
        end = max(group["_due"])
        if isinstance(start, date) and isinstance(end, date) and end >= start:
            ranges[str(project_id)] = (start, end)
    return ranges


def _assignment_day_hours(
    assignments: List[Dict[str, Any]],
    constructor: str,
    day: date,
    exclude_id: Optional[str] = None,
) -> float:
    day_key = day.isoformat()
    total = 0.0
    for assignment in assignments or []:
        if exclude_id and assignment.get("id") == exclude_id:
            continue
        if str(assignment.get("constructor") or "").upper() != constructor:
            continue
        days = assignment.get("days") or {}
        try:
            total += float(days.get(day_key, 0.0))
        except (TypeError, ValueError):
            continue
    return total


def _assignment_total_hours(assignment: Dict[str, Any]) -> float:
    days = assignment.get("days") or {}
    return round(sum(float(value) for value in days.values()), 2)


def _assignment_mode_label(mode: Any) -> str:
    return {
        "full": "Pełne",
        "half": "Połowa",
        "custom": "Własne",
        "budget": "Na godziny",
        "manual": "Ręczne",
    }.get(str(mode), str(mode))


def _without_assignment(
    assignments: List[Dict[str, Any]], assignment_id: Optional[str]
) -> List[Dict[str, Any]]:
    return [
        assignment
        for assignment in assignments
        if not assignment_id or assignment.get("id") != assignment_id
    ]


def find_assignment(
    assignments: List[Dict[str, Any]], assignment_id: Optional[str]
) -> Optional[Dict[str, Any]]:
    if not assignment_id:
        return None
    for assignment in assignments:
        if assignment.get("id") == assignment_id:
            return assignment
    return None


def allocate_hours_forward(
    assignments: List[Dict[str, Any]],
    constructor: str,
    project_id: str,
    project_name: str,
    start: date,
    project_end: date,
    requested_hours: float,
    effective_daily_hours: float,
) -> Tuple[Optional[Dict[str, Any]], float]:
    """Allocate a total hour budget forward through project business days."""
    requested = max(0.0, float(requested_hours))
    if requested <= 0 or project_end < start:
        return None, requested

    remaining = requested
    days: Dict[str, float] = {}
    for day in business_days_between(start, project_end):
        available = max(
            0.0,
            float(effective_daily_hours)
            - _assignment_day_hours(assignments, constructor, day),
        )
        if available <= 0.001:
            continue
        hours = min(remaining, available)
        if hours <= 0.001:
            continue
        days[day.isoformat()] = round(hours, 2)
        remaining = round(remaining - hours, 2)
        if remaining <= 0.001:
            remaining = 0.0
            break

    if not days:
        return None, remaining

    return (
        {
            "id": uuid.uuid4().hex,
            "kind": "project",
            "project_id": project_id,
            "project": project_name,
            "label": project_name,
            "constructor": constructor,
            "start_date": min(days),
            "end_date": max(days),
            "mode": "budget",
            "hours_per_day": None,
            "requested_hours": round(requested, 2),
            "days": days,
            "created_at": datetime.utcnow().isoformat(timespec="seconds"),
        },
        remaining,
    )


def rebuild_assignment_block(
    existing: Dict[str, Any],
    assignments: List[Dict[str, Any]],
    constructor: str,
    start: date,
    end: date,
    mode: str,
    target_hours: float,
    requested_hours: float,
    effective_daily_hours: float,
    label: Optional[str] = None,
) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]], float]:
    """Rebuild an existing assignment, ignoring its old hours in availability."""
    base = _without_assignment(assignments, existing.get("id"))
    kind = "external" if existing.get("kind") == "external" else "project"
    remaining = 0.0

    if kind == "project" and mode == "budget":
        block, remaining = allocate_hours_forward(
            assignments=base,
            constructor=constructor,
            project_id=str(existing.get("project_id") or ""),
            project_name=str(existing.get("project") or existing.get("label") or ""),
            start=start,
            project_end=end,
            requested_hours=requested_hours,
            effective_daily_hours=effective_daily_hours,
        )
    else:
        block = _build_assignment_block(
            kind=kind,
            project_id=existing.get("project_id"),
            project=str(existing.get("project") or ""),
            label=str(
                label
                if label is not None
                else existing.get("label") or existing.get("project") or ""
            ),
            constructor=constructor,
            start=start,
            end=end,
            mode=mode,
            target_hours=target_hours,
            effective_daily_hours=effective_daily_hours,
            assignments=base,
        )

    if block is None:
        return None, base, remaining

    block["id"] = existing.get("id") or block["id"]
    block["created_at"] = existing.get("created_at") or block["created_at"]
    return block, base + [block], remaining


def update_assignment_days(
    existing: Dict[str, Any],
    new_hours: Dict[str, float],
    constructor: str,
) -> Optional[Dict[str, Any]]:
    cleaned: Dict[str, float] = {}
    for day_str, hours in new_hours.items():
        try:
            day_value = date.fromisoformat(str(day_str))
            hours_value = float(hours)
        except (TypeError, ValueError):
            continue
        if hours_value > 0.001:
            cleaned[day_value.isoformat()] = round(hours_value, 2)

    if not cleaned:
        return None

    updated = dict(existing)
    updated["days"] = cleaned
    updated["constructor"] = constructor
    updated["start_date"] = min(cleaned)
    updated["end_date"] = max(cleaned)
    total = round(sum(cleaned.values()), 2)
    if updated.get("mode") == "budget":
        updated["requested_hours"] = total
    else:
        updated["mode"] = "manual"
        updated["hours_per_day"] = None
    return updated


def replace_assignment(
    assignments: List[Dict[str, Any]], updated: Dict[str, Any]
) -> List[Dict[str, Any]]:
    return [
        updated if assignment.get("id") == updated.get("id") else assignment
        for assignment in assignments
    ]


def _build_assignment_block(
    kind: str,
    project_id: Optional[str],
    project: str,
    label: str,
    constructor: str,
    start: date,
    end: date,
    mode: str,
    target_hours: float,
    effective_daily_hours: float,
    assignments: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    days: Dict[str, float] = {}
    for day in business_days_between(start, end):
        available = max(0.0, float(effective_daily_hours) - _assignment_day_hours(assignments, constructor, day))
        if available <= 0.001:
            continue
        hours = min(available, float(target_hours))
        if hours > 0.001:
            days[day.isoformat()] = round(hours, 2)
    if not days:
        return None
    return {
        "id": uuid.uuid4().hex,
        "kind": "external" if kind == "external" else "project",
        "project_id": project_id,
        "project": project,
        "label": label,
        "constructor": constructor,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "mode": mode,
        "hours_per_day": round(float(target_hours), 2),
        "days": days,
        "created_at": datetime.utcnow().isoformat(timespec="seconds"),
    }


def assign_constructor_to_ranges(
    assignments: List[Dict[str, Any]],
    constructor: str,
    ranges_with_names: List[Tuple[str, str, date, date]],
    mode: str,
    target_hours: float,
    effective_daily_hours: float,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    working = list(assignments)
    created: List[Dict[str, Any]] = []
    for project_id, project_name, start, end in ranges_with_names:
        block = _build_assignment_block(
            "project",
            project_id,
            project_name,
            project_name,
            constructor,
            start,
            end,
            mode,
            target_hours,
            effective_daily_hours,
            working,
        )
        if block is not None:
            working.append(block)
            created.append(block)
    return created, working


def add_external_assignment(
    assignments: List[Dict[str, Any]],
    constructor: str,
    label: str,
    start: date,
    end: date,
    mode: str,
    target_hours: float,
    effective_daily_hours: float,
) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
    block = _build_assignment_block(
        "external",
        None,
        "Inne zadanie",
        label,
        constructor,
        start,
        end,
        mode,
        target_hours,
        effective_daily_hours,
        assignments,
    )
    if block is None:
        return None, list(assignments)
    return block, list(assignments) + [block]


def build_project_day_table(
    core_df: pd.DataFrame,
    selected_projects: List[str],
    project_map: Dict[str, str],
    daily_df: pd.DataFrame,
    assignments: List[Dict[str, Any]],
    effective_daily_hours: float,
    constructor: str,
    selection: Set[str],
) -> pd.DataFrame:
    ranges = project_date_ranges(core_df)
    rows: List[Dict[str, Any]] = []
    for project_id in selected_projects:
        if project_id not in ranges:
            continue
        start, end = ranges[project_id]
        project_name = project_map.get(project_id, project_id)
        for day in business_days_between(start, end):
            rows.append(
                {
                    "project_id": project_id,
                    "project": project_name,
                    "date": day,
                    "date_str": day.strftime("%d.%m.%Y"),
                    "weekday": WEEKDAY_LABELS[day.weekday()],
                }
            )
    if not rows:
        return pd.DataFrame()

    table = pd.DataFrame(rows)
    if daily_df is not None and not daily_df.empty:
        plan = daily_df.copy()
        plan["date"] = pd.to_datetime(plan["date"]).dt.date
        plan = (
            plan.groupby(["project", "date"], as_index=False)["daily_hours"]
            .sum()
            .rename(columns={"daily_hours": "plan_hours"})
        )
        table = table.merge(plan, on=["project", "date"], how="left")
    else:
        table["plan_hours"] = 0.0
    table["plan_hours"] = pd.to_numeric(table["plan_hours"], errors="coerce").fillna(0.0)

    expanded = expand_assignment_days(assignments)
    if expanded.empty:
        table["assigned_constructor_h"] = 0.0
        table["assigned_project_h"] = 0.0
    else:
        constructor_totals = (
            expanded[expanded["constructor"] == constructor]
            .groupby("date")["hours"]
            .sum()
        )
        table["assigned_constructor_h"] = table["date"].map(constructor_totals).fillna(0.0)
        project_totals = (
            expanded[expanded["kind"] == "project"]
            .groupby(["project_id", "date"])["hours"]
            .sum()
        )
        table["assigned_project_h"] = [
            float(project_totals.get((project_id, day), 0.0))
            for project_id, day in zip(table["project_id"], table["date"])
        ]

    table["available_h"] = (
        float(effective_daily_hours) - table["assigned_constructor_h"]
    ).clip(lower=0.0)
    table["_row_key"] = [
        f"{project_id}::{day.isoformat()}"
        for project_id, day in zip(table["project_id"], table["date"])
    ]
    table["selected"] = table["_row_key"].isin(selection)
    return table.set_index("_row_key")


def expand_selection_to_ranges(
    selected_keys: Set[str], all_keys: List[str]
) -> Set[str]:
    """Expand checked project/day rows to the full range between extremes per project."""
    if not selected_keys:
        return set()
    grouped: Dict[str, List[Tuple[date, str]]] = defaultdict(list)
    for key in all_keys:
        try:
            project_id, date_str = str(key).split("::", 1)
            day_value = date.fromisoformat(date_str)
        except (ValueError, TypeError):
            continue
        grouped[project_id].append((day_value, key))

    expanded: Set[str] = set()
    for items in grouped.values():
        selected_dates = [day for day, key in items if key in selected_keys]
        if not selected_dates:
            continue
        start, end = min(selected_dates), max(selected_dates)
        for day_value, key in items:
            if start <= day_value <= end:
                expanded.add(key)
    return expanded


def _polish_person_label(count: int) -> str:
    if count == 1:
        return "1 osoba"
    if 2 <= count <= 4:
        return f"{count} osoby"
    return f"{count} osób"


def _polish_hours_label(hours: float) -> str:
    rounded = int(round(hours))
    if rounded == 1:
        return "1 godzina"
    if 2 <= rounded <= 4:
        return f"{rounded} godziny"
    return f"{rounded} godzin"


def format_required_constructors(full_people: int, remainder_hours: float) -> str:
    base = f"{_polish_person_label(int(full_people))} full"
    if remainder_hours >= 0.5:
        return f"{base} i {_polish_hours_label(remainder_hours)}"
    return base


def short_label(value: Any, max_len: int = 24) -> str:
    text = str(value)
    return text if len(text) <= max_len else text[: max_len - 1] + "…"



def render_df(
    df: pd.DataFrame,
    cols: list[str | tuple[str, str]],
    column_config: Optional[Dict[str, Any]] = None,
) -> None:
    """Render dataframe with safe fallback if Styler blows up."""
    df_local = df.copy()
    if "completed" in df_local:
        completed_raw = df_local["completed"].copy()
        df_local["completed"] = df_local["completed"].map(bool_symbol)
    else:
        completed_raw = pd.Series([None] * len(df_local))
    if "due_today_or_past" in df_local:
        completed_dates = (
            df_local["completed_date"].copy()
            if "completed_date" in df_local
            else pd.Series([None] * len(df_local))
        )
        due_dates = (
            df_local["due_date"].copy()
            if "due_date" in df_local
            else pd.Series([None] * len(df_local))
        )
        df_local["due_today_or_past"] = [
            due_symbol(due_flag, completed_flag, completed_date, due_date)
            for due_flag, completed_flag, completed_date, due_date in zip(
                df_local["due_today_or_past"],
                completed_raw,
                completed_dates,
                due_dates,
            )
        ]
    for c in ["allocated_hours", "planned_hours", "alloc_vs_plan_pct", "time_progress_pct"]:
        if c in df_local:
            df_local[c] = pd.to_numeric(df_local[c], errors="coerce").round(0)
    for numeric_col in ["used_hours_until_yesterday", "allocated_hours", "planned_hours"]:
        if numeric_col in df_local:
            df_local[numeric_col] = pd.to_numeric(df_local[numeric_col], errors="coerce").round(0)
    ordered_cols: list[str] = []
    label_config: Dict[str, Any] = {}
    final_config: Dict[str, Any] = dict(column_config or {})

    try:
        fmt = {
            c: "{:.0f}"
            for c in ["allocated_hours", "planned_hours", "alloc_vs_plan_pct", "time_progress_pct"]
            if c in df_local
        }
        for item in cols:
            if isinstance(item, tuple):
                column, label = item
            else:
                column, label = item, None
            if column not in df_local.columns:
                raise KeyError(f"{column} is not in DataFrame columns")
            ordered_cols.append(column)
            if label and column not in final_config:
                first_val = df_local[column].dropna().iloc[0] if not df_local[column].dropna().empty else None
                if isinstance(first_val, (date, datetime)):
                    label_config[column] = st.column_config.DateColumn(label, format="DD.MM.YYYY")
                else:
                    label_config[column] = st.column_config.TextColumn(label)
        final_config = {**final_config, **label_config}
        st.dataframe(
            df_local[ordered_cols]
            .style.map(color_ratio, subset=["alloc_vs_plan_pct"])
            .map(color_time, subset=["time_progress_pct"])
            .format(fmt),
            width='stretch',
            column_config=final_config,
        )
    except Exception as exc:  # noqa: BLE001
        log(f"Styler failed, showing plain dataframe. Error: {exc}")
        st.dataframe(
            df_local[ordered_cols],
            width='stretch',
            column_config=final_config,
        )


# ---- API Layer -------------------------------------------------------------
def api_get(
    base_url: str, api_key: str, path: str, params: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    if not api_key:
        raise RuntimeError("Brak WRIKE_API_KEY – uzupełnij .env lub sidebar.")
    headers = {"Authorization": f"bearer {api_key}"}
    url = f"{base_url.rstrip('/')}/{path.lstrip('/')}"
    resp = requests.get(url, headers=headers, params=params or {})
    log(f"GET {url} params={json.dumps(params, ensure_ascii=False)} status={resp.status_code}")
    if resp.status_code == 403:
        raise RuntimeError("API zwróciło 403 (sprawdź token i uprawnienia).")
    if resp.status_code == 400:
        raise RuntimeError(f"API zwróciło 400 (parametry?): {resp.text}")
    resp.raise_for_status()
    return resp.json()


@st.cache_data(ttl=WRIKE_DATA_CACHE_TTL_SEC, show_spinner=False)
def fetch_client_projects(
    base_url: str, api_key: str, client_folder_id: str
) -> List[Dict[str, Any]]:
    cache_key = f"client_projects|{base_url.rstrip('/')}|{client_folder_id}"
    cached = _wrike_cache_get("data", cache_key, WRIKE_DATA_CACHE_TTL_SEC)
    if isinstance(cached, list):
        return cached
    params = {"project": "true", "descendants": "false"}
    data = api_get(base_url, api_key, f"folders/{client_folder_id}/folders", params=params)
    out = data.get("data", [])
    _wrike_cache_set("data", cache_key, out)
    return out


@st.cache_data(ttl=WRIKE_DATA_CACHE_TTL_SEC, show_spinner=False)
def fetch_tasks_for_project(
    base_url: str, api_key: str, project_id: str
) -> List[Dict[str, Any]]:
    cache_key = f"tasks_for_project|{base_url.rstrip('/')}|{project_id}"
    cached = _wrike_cache_get("data", cache_key, WRIKE_DATA_CACHE_TTL_SEC)
    if isinstance(cached, list):
        return cached
    # Primary (validated) set from Postman hint
    primary_fields = [
        "superTaskIds",
        "effortAllocation",
        "customItemTypeId",
        "superParentIds",
        "customFields",
    ]
    # Fallback: no explicit fields (Wrike defaults)
    field_options: List[Optional[List[str]]] = [primary_fields, None]
    base_params = {
        "descendants": "true",
        "subTasks": "true",
        "pageSize": 1000,
    }
    tasks: List[Dict[str, Any]] = []
    next_token: Optional[str] = None
    chosen_fields: Optional[List[str]] = None

    for fields in field_options:
        params = dict(base_params)
        if fields is not None:
            params["fields"] = json.dumps(fields)
        try:
            next_token = None
            tasks.clear()
            while True:
                page_params = dict(params)
                if next_token:
                    page_params["nextPageToken"] = next_token

                try:
                    data = api_get(base_url, api_key, f"folders/{project_id}/tasks", params=page_params)
                except RuntimeError as exc:
                    if "nextPageToken" in str(exc):
                        log(f"Invalid nextPageToken received; stopping pagination. Error: {exc}")
                        break
                    raise

                tasks.extend(data.get("data", []))
                nt = data.get("nextPageToken")
                next_token = nt if nt else None  # guard against empty string
                log(
                    f"Fetched page size={len(data.get('data', []))}, total={len(tasks)}, "
                    f"nextPageToken={next_token}, fields={fields}"
                )
                # Continue only if page full AND token present
                if not next_token or len(data.get("data", [])) < base_params["pageSize"]:
                    break
            chosen_fields = fields
            break
        except RuntimeError as exc:
            # If token invalid, stop paging this variant
            if "nextPageToken" in str(exc):
                log(f"Stopping pagination due to invalid nextPageToken: {exc}")
                break
            log(f"Field set {fields} rejected: {exc}")
            continue

    if chosen_fields is None:
        raise RuntimeError("Nie udało się pobrać tasków żadnym zestawem pól.")

    _wrike_cache_set("data", cache_key, tasks)
    return tasks



def fetch_wrike_comments(base_url: str, api_key: str, path: str) -> List[Dict[str, Any]]:
    cache_key = f"{base_url.rstrip('/')}/{path.lstrip('/')}"
    cached = _wrike_cache_get("comments", cache_key, WRIKE_COMMENTS_CACHE_TTL_SEC)
    if isinstance(cached, list):
        _inc_perf_metric("wrike_comments_cache_hit")
        return cached
    _inc_perf_metric("wrike_comments_cache_miss")
    params: Dict[str, Any] = {}
    comments: List[Dict[str, Any]] = []
    next_token: Optional[str] = None
    while True:
        page_params = dict(params)
        if next_token:
            page_params["nextPageToken"] = next_token
        data = api_get(base_url, api_key, path, params=page_params)
        page_items = data.get("data", [])
        comments.extend(page_items)
        next_token = data.get("nextPageToken")
        if not next_token:
            break
    _wrike_cache_set("comments", cache_key, comments)
    return comments


def extract_solidworks_numbers(comments: List[Dict[str, Any]]) -> List[str]:
    numbers: List[str] = []
    for comment in comments:
        text = comment.get("text") or ""
        if "KX7XETJ3" not in text:
            continue
        for match in ASSEMBLY_NUMBER_RE.findall(text):
            normalized = normalize_part_number(match)
            if not normalized:
                continue
            if normalized not in numbers:
                numbers.append(normalized)
    return numbers


@st.cache_data(ttl=300, show_spinner=False)
def fetch_graph_token(tenant_id: str, client_id: str, client_secret: str) -> Optional[str]:
    if not tenant_id or not client_id or not client_secret:
        return None
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
        "scope": "https://graph.microsoft.com/.default",
    }
    try:
        resp = requests.post(token_url, data=payload, headers={"Content-Type": "application/x-www-form-urlencoded"})
        resp.raise_for_status()
        return resp.json().get("access_token")
    except requests.RequestException as exc:  # noqa: BLE001
        log(f"Graph token request failed: {exc}")
    return None


@st.cache_data(ttl=300, show_spinner=False)
def fetch_sharepoint_site_id(token: str, hostname: str, site_path: str) -> Optional[str]:
    if not token or not hostname or not site_path:
        return None
    normalized_path = site_path.strip("/")
    if not normalized_path:
        return None
    url = f"{MS_GRAPH_BASE_URL}/sites/{hostname}:/{normalized_path}"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        return resp.json().get("id")
    except requests.RequestException as exc:  # noqa: BLE001
        log(f"SharePoint site lookup failed: {exc}")
    return None


@st.cache_data(ttl=300, show_spinner=False)
def fetch_list_item_fields(
    token: str,
    site_id: str,
    list_id: str,
    select_fields: Tuple[str, ...],
) -> List[Dict[str, Any]]:
    if not token or not site_id or not list_id or not select_fields:
        return []
    headers = {"Authorization": f"Bearer {token}"}
    next_link = f"{MS_GRAPH_BASE_URL}/sites/{site_id}/lists/{list_id}/items"
    params: Optional[Dict[str, Any]] = {
        "$top": 200,
        "$expand": "fields($select=" + ",".join(select_fields) + ")",
    }
    expanded: List[Dict[str, Any]] = []
    while next_link:
        try:
            resp = requests.get(next_link, headers=headers, params=params)
            resp.raise_for_status()
        except requests.RequestException as exc:  # noqa: BLE001
            log(f"List items fetch failed for {list_id}: {exc}")
            return []
        data = resp.json()
        for entry in data.get("value", []):
            fields = entry.get("fields")
            if fields:
                expanded.append(fields)
        next_link = data.get("@odata.nextLink")
        params = None
    return expanded


def build_part_number_lookup(token: str, site_id: str, list_id: str) -> Dict[str, str]:
    select_fields = (POZYCJE_PART_FIELD, POZYCJE_HEADER_REF_FIELD)
    items = fetch_list_item_fields(token, site_id, list_id, select_fields)
    mapping: Dict[str, str] = {}
    for item in items:
        fields = item or {}
        part_value = normalize_part_number(fields.get(POZYCJE_PART_FIELD))
        header_id = fields.get(POZYCJE_HEADER_REF_FIELD)
        if header_id is None:
            header_id = fields.get("id")
        if part_value and header_id is not None:
            mapping[part_value] = str(header_id)
    log(f"Loaded {len(mapping)} part numbers from list {list_id}")
    return mapping


def build_header_lookup(token: str, site_id: str, list_id: str) -> Dict[str, str]:
    select_fields = (HEADER_ID_FIELD, HEADER_ORDER_FIELD)
    items = fetch_list_item_fields(token, site_id, list_id, select_fields)
    mapping: Dict[str, str] = {}
    for item in items:
        fields = item or {}
        header_id = fields.get(HEADER_ID_FIELD)
        if header_id is None:
            header_id = fields.get("id")
        order_value = fields.get(HEADER_ORDER_FIELD)
        if header_id is not None and order_value:
            mapping[str(header_id)] = str(order_value).strip()
    log(f"Loaded {len(mapping)} order references from list {list_id}")
    return mapping


@st.cache_data(ttl=300, show_spinner=False)
def graph_search_pdf(token: str, order_id: str) -> Optional[str]:
    if not token or not order_id or not MC_SITE_ID:
        return None
    escaped_order = order_id.replace("'", "''")
    query = quote(f"'{escaped_order}'", safe="")
    url = f"{MS_GRAPH_BASE_URL}/drives/{MC_SITE_ID}/root/search(q={query})"
    headers = {
        "Authorization": f"Bearer {token}",
    }
    try:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
    except requests.RequestException as exc:  # noqa: BLE001
        log(f"Drive search for Order_ID {order_id} failed: {exc}")
        return None
    data = resp.json()
    normalized_order = order_id.strip().lower()
    fallback_url: Optional[str] = None
    for item in data.get("value", []):
        name = str(item.get("name") or "")
        web_url = item.get("webUrl")
        if not web_url or not name.lower().endswith(".pdf"):
            continue
        if normalized_order and normalized_order in name.lower():
            return web_url
        if fallback_url is None:
            fallback_url = web_url
    if fallback_url:
        return fallback_url
    log(f"No PDF found for Order_ID {order_id}")
    return None


def prepare_solidworks_context() -> Optional[SolidworksContext]:
    if not (
        MS_TENANT_ID
        and MS_CLIENT_ID
        and MS_CLIENT_SECRET
        and POZYCJE_LIST_ID
        and NAGLOWEK_LIST_ID
        and MC_SITE_ID
    ):
        return None
    token = fetch_graph_token(MS_TENANT_ID, MS_CLIENT_ID, MS_CLIENT_SECRET)
    if not token:
        return None
    site_id = fetch_sharepoint_site_id(token, MS_SHAREPOINT_HOST, MS_SHAREPOINT_SITE_PATH)
    if not site_id:
        return None
    part_map = build_part_number_lookup(token, site_id, POZYCJE_LIST_ID)
    header_map = build_header_lookup(token, site_id, NAGLOWEK_LIST_ID)
    if not part_map or not header_map:
        log("Solidworks lookup data incomplete; skipping PDF linking.")
    return SolidworksContext(token, part_map, header_map)


def clear_wrike_cache() -> None:
    fetch_client_projects.clear()
    fetch_tasks_for_project.clear()
    st.session_state.pop("wrike_comments_cache", None)
    st.session_state.pop("wrike_data_cache", None)
    st.session_state.pop("wrike_cache_loaded", None)
    st.session_state.pop("kpi_aggregate_cache", None)
    try:
        if WRIKE_COMMENTS_CACHE_FILE.exists():
            WRIKE_COMMENTS_CACHE_FILE.unlink()
        if WRIKE_DATA_CACHE_FILE.exists():
            WRIKE_DATA_CACHE_FILE.unlink()
    except OSError as exc:
        log(f"Failed to clear Wrike disk cache: {exc}")


# ---- Aggregation -----------------------------------------------------------
def build_indexes(tasks: List[Dict[str, Any]]) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, List[str]]]:
    by_id: Dict[str, Dict[str, Any]] = {}
    children: Dict[str, List[str]] = defaultdict(list)

    for task in tasks:
        tid = task["id"]
        by_id[tid] = task
        for parent in task.get("superTaskIds", []) or []:
            children[parent].append(tid)

    return by_id, children


def make_nearest_core_resolvers(
    tasks_by_id: Dict[str, Dict[str, Any]],
    core_task_type: str,
    bom_task_type: str,
) -> Tuple[
    Callable[[str], Optional[str]],
    Callable[[str], Optional[str]],
]:

    def parent_ids(item: Dict[str, Any]) -> Iterable[str]:
        parents: List[str] = []
        parents.extend(item.get("superTaskIds") or [])
        parents.extend(item.get("projectIds") or [])
        parents.extend(item.get("parentIds") or [])
        return [pid for pid in parents if pid]

    @lru_cache(None)
    def nearest_core_task(task_id: str) -> Optional[str]:
        task = tasks_by_id.get(task_id)
        if not task:
            return None
        if task.get("customItemTypeId") == core_task_type:
            return task_id
        for parent_id in parent_ids(task):
            found = nearest_core_task(parent_id)
            if found:
                return found
        return None

    @lru_cache(None)
    def nearest_bom_task(task_id: str) -> Optional[str]:
        task = tasks_by_id.get(task_id)
        if not task:
            return None
        if task.get("customItemTypeId") == bom_task_type:
            return task_id
        for parent_id in parent_ids(task):
            found = nearest_bom_task(parent_id)
            if found:
                return found
        return None

    return nearest_core_task, nearest_bom_task


def aggregate_core_items(
    tasks: List[Dict[str, Any]],
    core_task_type: str,
    bom_task_type: str,
    planned_field_id: str,
    completed_status_id: str,
    project_lookup: Optional[Dict[str, str]] = None,
    allowed_project_ids: Optional[Set[str]] = None,
    wrike_base_url: Optional[str] = None,
    wrike_api_key: Optional[str] = None,
    solidworks_context: Optional[SolidworksContext] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    tasks_by_id, children = build_indexes(tasks)
    nearest_core_task, nearest_bom_task = make_nearest_core_resolvers(
        tasks_by_id,
        core_task_type,
        bom_task_type,
    )

    today = date.today()
    core_task_rows = []
    bom_task_rows = []
    completed_overrides = load_completed_overrides()

    # Pre-compute allocated minutes per task
    alloc_by_task = {tid: allocated_minutes(task) for tid, task in tasks_by_id.items()}
    cutoff_date = date.today() - timedelta(days=1)
    used_minutes_by_task = {tid: effort_minutes_until(task, cutoff_date) for tid, task in tasks_by_id.items()}
    planned_seen = 0
    customfields_seen = 0

    # Pre-compute raw planned hours (before netting) for all tasks
    raw_planned_by_task: Dict[str, Optional[float]] = {}
    for tid, task in tasks_by_id.items():
        raw_planned_by_task[tid] = extract_planned_hours(task, planned_field_id)

    # Cascading netting: net = max(0, raw - sum(net of children))
    @lru_cache(maxsize=None)
    def compute_netted_planned(tid: str) -> Optional[float]:
        raw = raw_planned_by_task.get(tid)
        if raw is None:
            return None
        child_sum = 0.0
        for child_id in children.get(tid, []):
            child_net = compute_netted_planned(child_id)
            if child_net is not None:
                child_sum += child_net
        return max(0.0, raw - child_sum)

    netted_planned_by_task: Dict[str, Optional[float]] = {}
    for tid in tasks_by_id:
        netted_planned_by_task[tid] = compute_netted_planned(tid)

    lookup = project_lookup or {}
    solidworks_cache_dirty = False

    def annotate_solidworks(item_id: str, is_task_item: bool) -> Tuple[Optional[str], List[str]]:
        nonlocal solidworks_cache_dirty
        if not wrike_base_url or not wrike_api_key:
            return None, []
        item_cache = _solidworks_item_cache()
        order_pdf_cache = _solidworks_order_pdf_cache()
        cache_key = f"{'task' if is_task_item else 'folder'}:{item_id}"
        path = f"tasks/{item_id}/comments" if is_task_item else f"folders/{item_id}/comments"
        try:
            comments = fetch_wrike_comments(wrike_base_url, wrike_api_key, path)
        except Exception as exc:  # noqa: BLE001
            log(f"Failed to load comments for {item_id}: {exc}")
            return None, []
        signature = comments_signature(comments)
        cached = item_cache.get(cache_key)
        if cached and cached.get("signature") == signature:
            log(f"Solidworks cache hit for {cache_key}")
            return cached.get("text"), list(cached.get("pdf_urls", []))
        numbers = extract_solidworks_numbers(comments)
        if not numbers:
            item_cache[cache_key] = {"signature": signature, "text": None, "pdf_urls": []}
            solidworks_cache_dirty = True
            return None, []
        entries: List[str] = []
        unique_pdf_urls: List[str] = []
        for number in numbers:
            label = number
            if solidworks_context:
                order_id = solidworks_context.resolve_order_id(number)
                if order_id and solidworks_context.graph_token:
                    if order_id not in order_pdf_cache:
                        order_pdf_cache[order_id] = graph_search_pdf(solidworks_context.graph_token, order_id)
                        solidworks_cache_dirty = True
                    pdf_url = order_pdf_cache[order_id]
                    if pdf_url:
                        label = f"{number} (SPEC)"
                        if pdf_url not in unique_pdf_urls:
                            unique_pdf_urls.append(pdf_url)
                else:
                    log(f"No header mapping for part number {number}")
            entries.append(label)
        text_value = ", ".join(entries)
        item_cache[cache_key] = {
            "signature": signature,
            "text": text_value,
            "pdf_urls": unique_pdf_urls,
        }
        solidworks_cache_dirty = True
        return text_value, unique_pdf_urls

    def parent_ids(task: Dict[str, Any]) -> Iterable[str]:
        parents: List[str] = []
        parents.extend(task.get("superTaskIds") or [])
        parents.extend(task.get("projectIds") or [])
        parents.extend(task.get("parentIds") or [])
        return [pid for pid in parents if pid]

    def is_descendant(task_id: str, ancestor_id: str) -> bool:
        visited: Set[str] = set()
        stack: List[str] = [task_id]
        while stack:
            current = stack.pop()
            if current == ancestor_id:
                return True
            for pid in parent_ids(tasks_by_id.get(current, {})):
                if pid not in visited:
                    visited.add(pid)
                    stack.append(pid)
        return False

    def resolve_client_project(task: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
        candidate_ids = []
        root = task.get("_selected_project_id")
        if root:
            candidate_ids.append(root)
        candidate_ids.extend(task.get("superParentIds") or [])
        candidate_ids.extend(task.get("projectIds") or [])
        candidate_ids.extend(task.get("parentIds") or [])

        visited: Set[str] = set()
        while candidate_ids:
            pid = candidate_ids.pop()
            if not pid or pid in visited:
                continue
            visited.add(pid)
            if pid in lookup:
                return pid, lookup.get(pid)
            parent = tasks_by_id.get(pid)
            if not parent:
                continue
            candidate_ids.extend(parent.get("superParentIds") or [])
            candidate_ids.extend(parent.get("projectIds") or [])
            candidate_ids.extend(parent.get("parentIds") or [])
        return None, None

    # Helper: sum allocated minutes of all tasks whose nearest core matches target
    def sum_alloc(nearest_fn, target_id: str, include_self: bool) -> int:
        total = 0
        for tid, task in tasks_by_id.items():
            if not include_self and tid == target_id:
                continue
            nearest = nearest_fn(tid)
            if nearest == target_id or (nearest is None and is_descendant(tid, target_id)):
                total += alloc_by_task.get(tid, 0)
        return total

    def sum_used(nearest_fn, target_id: str, include_self: bool) -> int:
        total = 0
        for tid, task in tasks_by_id.items():
            if not include_self and tid == target_id:
                continue
            nearest = nearest_fn(tid)
            if nearest == target_id or (nearest is None and is_descendant(tid, target_id)):
                total += used_minutes_by_task.get(tid, 0)
        return total

    def minutes_to_hours(minutes: int) -> int:
        return int(round(minutes / 60))

    for tid, task in tasks_by_id.items():
        ctype = task.get("customItemTypeId") or task.get("entityTypeId")
        raw_planned = raw_planned_by_task.get(tid)
        if task.get("customFields"):
            customfields_seen += 1
        if raw_planned is not None:
            planned_seen += 1
        planned_hours = netted_planned_by_task.get(tid)
        if planned_hours is not None:
            planned_hours = int(round(planned_hours))
        project_id, project_title = resolve_client_project(task)
        if allowed_project_ids is not None and (not project_id or project_id not in allowed_project_ids):
            continue
        if ctype not in {core_task_type, bom_task_type}:
            continue
        start = iso_to_date(
            task.get("start")
            or task.get("dates", {}).get("start")
            or task.get("project", {}).get("startDate")
        )
        due = iso_to_date(
            task.get("due")
            or task.get("dates", {}).get("due")
            or task.get("project", {}).get("endDate")
        )
        completed = is_completed(task, completed_status_id)
        due_flag = bool(due and due <= today)
        completed_datetime = _completed_datetime(task)
        override = completed_overrides.get(tid)
        if override:
            completed_datetime = datetime.combine(override, datetime.min.time())
        time_progress = None
        if start and due and due > start:
            span = (due - start).days
            if span > 0:
                if completed_datetime:
                    elapsed = (completed_datetime.date() - start).days
                else:
                    elapsed = max(0, (today - start).days)
                time_progress = int(round(elapsed / span * 100))

        base_row = {
            "id": tid,
            "title": task.get("title", "(brak tytułu)"),
            "project": project_title,
            "project_id": project_id,
            "permalink": task.get("permalink"),
            "planned_hours": planned_hours,
            "due_date": due,
            "start_date": start,
            "time_progress_pct": time_progress,
            "due_today_or_past": due_flag,
            "completed": completed,
            "completed_date": completed_datetime,
            "warnings": [],
            "solidworks": None,
            "solidworks_specs": [],
        }
        if planned_hours is None:
            base_row["warnings"].append("Brak Planned effort")
        if not base_row["warnings"]:
            base_row["warnings"] = None

        is_task_item = ctype in {core_task_type, bom_task_type}
        solidworks_text, solidworks_pdf_urls = annotate_solidworks(tid, is_task_item)
        base_row["solidworks"] = solidworks_text
        base_row["solidworks_specs"] = solidworks_pdf_urls

        if ctype == core_task_type:
            alloc_minutes = sum_alloc(nearest_core_task, tid, include_self=True)
            base_row["allocated_hours"] = round(alloc_minutes / 60)
            used_minutes = sum_used(nearest_core_task, tid, include_self=True)
            base_row["used_hours_until_yesterday"] = minutes_to_hours(used_minutes)
            core_task_rows.append(base_row)
        elif ctype == bom_task_type:
            base_row["title"] = "\u2622 " + base_row["title"]
            alloc_minutes = sum_alloc(nearest_bom_task, tid, include_self=True)
            base_row["allocated_hours"] = round(alloc_minutes / 60)
            used_minutes = sum_used(nearest_bom_task, tid, include_self=True)
            base_row["used_hours_until_yesterday"] = minutes_to_hours(used_minutes)
            bom_task_rows.append(base_row)

    task_df = pd.DataFrame(core_task_rows)
    bom_df = pd.DataFrame(bom_task_rows)

    def add_ratio(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        def ratio(row):
            ph = row.get("planned_hours")
            ah = row.get("allocated_hours")
            if ph is None or ph == 0 or pd.isna(ph):
                return None
            if ah is None or pd.isna(ah):
                return None
            return int(round(ah / ph * 100))
        df["alloc_vs_plan_pct"] = df.apply(ratio, axis=1)
        return df

    task_df = add_ratio(task_df)
    bom_df = add_ratio(bom_df)

    if not task_df.empty and "title" in task_df:
        task_df = task_df.sort_values("title")
    if not bom_df.empty and "title" in bom_df:
        bom_df = bom_df.sort_values("title")

    # Unified core items view
    core_items_df = pd.concat([task_df, bom_df], ignore_index=True) if (not task_df.empty or not bom_df.empty) else pd.DataFrame()

    # KPI summary
    def completion_stats(df: pd.DataFrame) -> Tuple[int, int]:
        if df.empty:
            return 0, 0
        due_df = df[df["due_today_or_past"] == True]
        return int(due_df["completed"].sum()), int(len(due_df))

    completed, due_total = completion_stats(bom_df)
    planned_total = pd.concat(
        [task_df.get("planned_hours", pd.Series(dtype=float)), bom_df.get("planned_hours", pd.Series(dtype=float))],
        ignore_index=True,
    )
    alloc_total = pd.concat(
        [task_df.get("allocated_hours", pd.Series(dtype=float)), bom_df.get("allocated_hours", pd.Series(dtype=float))],
        ignore_index=True,
    )
    used_series = pd.concat(
        [
            task_df.get("used_hours_until_yesterday", pd.Series(dtype=float)),
            bom_df.get("used_hours_until_yesterday", pd.Series(dtype=float)),
        ],
        ignore_index=True,
    )
    used_clean = used_series.dropna()
    used_sum = int(used_clean.sum()) if not used_clean.empty else 0

    summary = {
        "allocated_hours": int(round(alloc_total.sum())),
        "planned_hours": int(round(planned_total.sum())),
        "planned_missing": int(planned_total.isna().sum()),
        "completed_due": completed,
        "due_total": due_total,
        "planned_seen": planned_seen,
        "customfields_seen": customfields_seen,
        "time_progress_avg": int(round(
            pd.concat(
                [
                    task_df.get("time_progress_pct", pd.Series(dtype=float)),
                    bom_df.get("time_progress_pct", pd.Series(dtype=float)),
                ],
                ignore_index=True,
            ).dropna().mean()
            if not pd.concat(
                [
                    task_df.get("time_progress_pct", pd.Series(dtype=float)),
                    bom_df.get("time_progress_pct", pd.Series(dtype=float)),
                ],
                ignore_index=True,
            ).dropna().empty
            else 0,
        )),
        "used_until_yesterday": used_sum,
    }

    if solidworks_cache_dirty:
        persist_solidworks_cache()

    return task_df, bom_df, summary


def expand_dynamic_spec_columns(
    task_df: pd.DataFrame, bom_df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame, int]:
    max_specs = 0
    for df in [task_df, bom_df]:
        if df.empty or "solidworks_specs" not in df.columns:
            continue
        for value in df["solidworks_specs"]:
            if isinstance(value, list):
                max_specs = max(max_specs, len(value))

    def apply(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        out = df.copy()
        specs_series = out["solidworks_specs"] if "solidworks_specs" in out.columns else pd.Series([[]] * len(out), index=out.index)
        for idx in range(max_specs):
            col = f"solidworks_pdf_{idx + 1}"
            out[col] = specs_series.apply(
                lambda urls: urls[idx] if isinstance(urls, list) and len(urls) > idx else None
            )
        if "solidworks_specs" in out.columns:
            out = out.drop(columns=["solidworks_specs"])
        return out

    return apply(task_df), apply(bom_df), max_specs


# ---- Auth --------------------------------------------------------------------
APP_USER = os.getenv("KPI_USERNAME", "admin")
APP_PASS = os.getenv("KPI_PASSWORD", "kpi2024")


def check_password() -> None:
    if st.session_state.get("authenticated"):
        return

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.title("KPI DASHBOARD")
        with st.form("login_form"):
            st.text_input("Login", key="login_user")
            st.text_input("Hasło", type="password", key="login_pass")
            if st.form_submit_button("Zaloguj"):
                if (
                    st.session_state.login_user == APP_USER
                    and st.session_state.login_pass == APP_PASS
                ):
                    st.session_state.authenticated = True
                    st.rerun()
                else:
                    st.error("Nieprawidłowy login lub hasło")
    st.stop()


# ---- UI --------------------------------------------------------------------
def _set_assignment_flash(message: str, level: str = "success") -> None:
    st.session_state["assignment_flash"] = message
    st.session_state["assignment_flash_level"] = level


def render_budget_assignment_form(
    assignments: List[Dict[str, Any]],
    constructors: List[str],
    effective_daily_hours: float,
    project_map: Dict[str, str],
    selected_projects: List[str],
    project_ranges: Dict[str, Tuple[date, date]],
    selection: Set[str],
) -> None:
    with st.expander("Przypisz na liczbę godzin", expanded=True):
        st.caption(
            "Wybierz projekt, datę startu i łączną liczbę godzin. System będzie "
            "rozkładał dostępne godziny konstruktora na kolejne dni robocze do końca "
            "projektu, aż wykorzysta zadaną pulę."
        )
        project_options = [
            project_id
            for project_id in selected_projects
            if project_id in project_ranges
        ]
        if not project_options:
            st.info(
                "Brak projektów z kompletnymi datami start/koniec — nie można "
                "rozłożyć godzin."
            )
            return

        selected_dates_by_project: Dict[str, List[date]] = defaultdict(list)
        for key in selection:
            try:
                project_id, day_str = str(key).split("::", 1)
                selected_dates_by_project[project_id].append(
                    date.fromisoformat(day_str)
                )
            except (ValueError, TypeError):
                continue

        if st.session_state.get("budget_project") not in project_options:
            st.session_state["budget_project"] = project_options[0]
        budget_project = st.selectbox(
            "Projekt",
            options=project_options,
            format_func=lambda project_id: project_map.get(project_id, project_id),
            key="budget_project",
        )
        project_start, project_end = project_ranges[budget_project]
        selected_dates = selected_dates_by_project.get(budget_project, [])
        default_start = min(selected_dates) if selected_dates else project_start
        start_sig = (
            f"{budget_project}:{default_start.isoformat()}:{project_end.isoformat()}"
        )
        if st.session_state.get("budget_start_sig") != start_sig:
            st.session_state["budget_start_sig"] = start_sig
            st.session_state["budget_start_date"] = default_start

        budget_start = st.date_input(
            "Data startu",
            min_value=project_start,
            max_value=project_end,
            key="budget_start_date",
        )
        if not isinstance(budget_start, date):
            budget_start = default_start
        if budget_start < project_start:
            budget_start = project_start
        if budget_start > project_end:
            budget_start = project_end

        if st.session_state.get("budget_constructor") not in constructors:
            st.session_state["budget_constructor"] = constructors[0]
        budget_constructor = st.selectbox(
            "Konstruktor",
            options=constructors,
            key="budget_constructor",
        )
        budget_hours = st.number_input(
            "Łączna liczba godzin",
            min_value=0.5,
            max_value=10000.0,
            value=8.0,
            step=0.5,
            format="%.1f",
            key="budget_hours",
        )

        available_preview = sum(
            max(
                0.0,
                float(effective_daily_hours)
                - _assignment_day_hours(
                    assignments, budget_constructor, business_day
                ),
            )
            for business_day in business_days_between(budget_start, project_end)
        )
        st.caption(
            f"Dostępne godziny od {budget_start.strftime('%d.%m.%Y')} do "
            f"{project_end.strftime('%d.%m.%Y')}: **{available_preview:.1f} h**."
        )

        if st.button(
            "Rozłóż godziny",
            key="budget_allocate",
            type="primary",
        ):
            if budget_hours <= 0:
                st.error("Liczba godzin musi być większa od zera.")
            elif budget_start > project_end:
                st.error("Data startu jest po końcu projektu.")
            else:
                block, remaining = allocate_hours_forward(
                    assignments=assignments,
                    constructor=budget_constructor,
                    project_id=budget_project,
                    project_name=project_map.get(budget_project, budget_project),
                    start=budget_start,
                    project_end=project_end,
                    requested_hours=budget_hours,
                    effective_daily_hours=effective_daily_hours,
                )
                if block is None:
                    st.warning(
                        "Brak dostępnych godzin w tym zakresie — konstruktor jest "
                        "już w pełni obciążony."
                    )
                else:
                    st.session_state["constructor_assignments"] = (
                        list(assignments) + [block]
                    )
                    st.session_state["assignments_dirty"] = True
                    allocated = _assignment_total_hours(block)
                    message = (
                        f"Przypisano {allocated:.1f} h konstruktorowi "
                        f"{budget_constructor} od {block['start_date']} "
                        f"do {block['end_date']}."
                    )
                    if remaining > 0.001:
                        message += (
                            f" Nie udało się przypisać {remaining:.1f} h do końca "
                            "projektu."
                        )
                        _set_assignment_flash(message, "warning")
                    else:
                        _set_assignment_flash(message)
                    st.rerun()


def render_assignment_editor(
    assignments: List[Dict[str, Any]],
    constructors: List[str],
    effective_daily_hours: float,
    project_ranges: Dict[str, Tuple[date, date]],
) -> None:
    assignment_id = st.session_state.get("editing_assignment_id")
    if not assignment_id:
        return

    existing = find_assignment(assignments, str(assignment_id))
    if existing is None:
        st.session_state.pop("editing_assignment_id", None)
        st.info("Wybrane przypisanie zostało usunięte.")
        return

    nonce = int(st.session_state.get("assignment_edit_nonce", 0))
    st.markdown("### Edycja przypisania")
    header_col, cancel_col = st.columns([5, 1])
    assignment_name = existing.get("label") or existing.get("project") or ""
    kind_label = (
        "Inne zadanie" if existing.get("kind") == "external" else "Projekt"
    )
    header_col.caption(
        f"{kind_label}: **{assignment_name}**  |  "
        f"tryb: **{_assignment_mode_label(existing.get('mode'))}**  |  "
        f"aktualnie: **{_assignment_total_hours(existing):.1f} h**"
    )
    if cancel_col.button("Anuluj edycję", key=f"cancel_edit_{nonce}"):
        st.session_state.pop("editing_assignment_id", None)
        st.session_state["assignment_edit_nonce"] = nonce + 1
        st.rerun()

    is_external = existing.get("kind") == "external"
    project_id = str(existing.get("project_id") or "")
    project_range = project_ranges.get(project_id) if project_id else None
    try:
        current_start = date.fromisoformat(str(existing.get("start_date")))
    except (TypeError, ValueError):
        current_start = date.today()
    try:
        current_end = date.fromisoformat(str(existing.get("end_date")))
    except (TypeError, ValueError):
        current_end = current_start
    if current_end < current_start:
        current_end = current_start

    constructor_options = list(constructors)
    existing_constructor = str(existing.get("constructor") or "").upper()
    if existing_constructor and existing_constructor not in constructor_options:
        constructor_options.append(existing_constructor)
    constructor_key = f"edit_constructor_{nonce}"
    if st.session_state.get(constructor_key) not in constructor_options:
        st.session_state[constructor_key] = (
            existing_constructor or constructor_options[0]
        )
    edit_constructor = st.selectbox(
        "Konstruktor",
        options=constructor_options,
        key=constructor_key,
    )

    block_tab, days_tab = st.tabs(["Blok", "Dni"])

    with block_tab:
        st.caption(
            "Edycja bloku przelicza przypisanie od nowa. Ręczne zmiany w zakładce "
            "„Dni” zostaną nadpisane."
        )
        if is_external:
            edit_label = st.text_input(
                "Nazwa zadania",
                value=str(existing.get("label") or ""),
                key=f"edit_label_{nonce}",
            )
        else:
            edit_label = str(
                existing.get("label") or existing.get("project") or ""
            )

        min_bound = None
        max_bound = None
        if project_range:
            min_bound = min(project_range[0], current_start, current_end)
            max_bound = max(project_range[1], current_start, current_end)

        edit_start = st.date_input(
            "Data startu",
            value=current_start,
            min_value=min_bound,
            max_value=max_bound,
            key=f"edit_start_{nonce}",
        )
        if not isinstance(edit_start, date):
            edit_start = current_start

        is_budget = existing.get("mode") == "budget" and not is_external
        edit_end = current_end
        edit_requested_hours = 0.0
        edit_target_hours = effective_daily_hours
        edit_mode = "custom"
        if is_budget:
            if project_range:
                edit_end = project_range[1]
                st.caption(
                    f"Koniec projektu: **{edit_end.strftime('%d.%m.%Y')}**. "
                    "Godziny zostaną rozłożone do tej daty."
                )
            else:
                st.warning(
                    "Brak zakresu dat projektu — nie można przeliczyć przypisania "
                    "na liczbę godzin."
                )
            edit_requested_hours = st.number_input(
                "Łączna liczba godzin",
                min_value=0.5,
                max_value=10000.0,
                value=float(
                    existing.get("requested_hours")
                    or _assignment_total_hours(existing)
                    or 0.5
                ),
                step=0.5,
                format="%.1f",
                key=f"edit_requested_{nonce}",
            )
        else:
            edit_end = st.date_input(
                "Data końca",
                value=current_end,
                min_value=min_bound,
                max_value=max_bound,
                key=f"edit_end_{nonce}",
            )
            if not isinstance(edit_end, date):
                edit_end = current_end
            mode_options = ["Pełne", "Połowa", "Własne"]
            existing_mode = str(existing.get("mode") or "custom")
            mode_index = 0 if existing_mode == "full" else 1 if existing_mode == "half" else 2
            edit_mode_label = st.radio(
                "Tryb godzin",
                mode_options,
                index=mode_index,
                horizontal=True,
                key=f"edit_mode_{nonce}",
            )
            if edit_mode_label == "Pełne":
                edit_mode = "full"
                edit_target_hours = effective_daily_hours
            elif edit_mode_label == "Połowa":
                edit_mode = "half"
                edit_target_hours = effective_daily_hours / 2
            else:
                edit_mode = "custom"
                existing_days = existing.get("days") or {}
                default_target = float(existing.get("hours_per_day") or 0.0)
                if default_target <= 0 and existing_days:
                    default_target = max(
                        float(value) for value in existing_days.values()
                    )
                if default_target <= 0:
                    default_target = effective_daily_hours
                edit_target_hours = st.number_input(
                    "Godziny dziennie",
                    min_value=0.1,
                    max_value=24.0,
                    value=default_target,
                    step=0.5,
                    format="%.1f",
                    key=f"edit_target_{nonce}",
                )

        if st.button(
            "Przelicz i zapisz",
            key=f"edit_save_block_{nonce}",
            type="primary",
        ):
            if is_external and not edit_label.strip():
                st.error("Podaj nazwę zadania.")
            elif edit_end < edit_start:
                st.error("Data końca nie może być wcześniejsza od daty startu.")
            elif is_budget and not project_range:
                st.error("Brak zakresu dat projektu do przeliczenia godzin.")
            else:
                block, working, remaining = rebuild_assignment_block(
                    existing=existing,
                    assignments=assignments,
                    constructor=edit_constructor,
                    start=edit_start,
                    end=edit_end,
                    mode="budget" if is_budget else edit_mode,
                    target_hours=edit_target_hours,
                    requested_hours=edit_requested_hours,
                    effective_daily_hours=effective_daily_hours,
                    label=edit_label.strip() if is_external else edit_label,
                )
                if block is None:
                    st.warning(
                        "Nie udało się przeliczyć przypisania — brak dostępnych "
                        "godzin w wybranym zakresie."
                    )
                else:
                    st.session_state["constructor_assignments"] = working
                    st.session_state["assignments_dirty"] = True
                    allocated = _assignment_total_hours(block)
                    message = (
                        f"Zapisano edycję: {allocated:.1f} h konstruktorowi "
                        f"{edit_constructor}."
                    )
                    if is_budget and remaining > 0.001:
                        message += (
                            f" Pozostało nieprzypisane: {remaining:.1f} h."
                        )
                        _set_assignment_flash(message, "warning")
                    else:
                        _set_assignment_flash(message)
                    st.session_state.pop("editing_assignment_id", None)
                    st.session_state["assignment_edit_nonce"] = nonce + 1
                    st.rerun()

    with days_tab:
        st.caption(
            "Możesz skorygować godziny w istniejących dniach. Limit to dostępne "
            "godziny konstruktora po wykluczeniu tego przypisania."
        )
        edit_rows: List[Dict[str, Any]] = []
        for day_str, hours in (existing.get("days") or {}).items():
            try:
                day_value = date.fromisoformat(str(day_str))
                hours_value = float(hours)
            except (TypeError, ValueError):
                continue
            available = max(
                0.0,
                float(effective_daily_hours)
                - _assignment_day_hours(
                    assignments,
                    edit_constructor,
                    day_value,
                    exclude_id=existing.get("id"),
                ),
            )
            edit_rows.append(
                {
                    "_day": day_value.isoformat(),
                    "Dzień": day_value.strftime("%d.%m.%Y"),
                    "Dzień tyg.": WEEKDAY_LABELS[day_value.weekday()],
                    "Dostępne": round(available, 2),
                    "Godziny": round(hours_value, 2),
                }
            )

        if not edit_rows:
            st.info("To przypisanie nie ma dni do edycji.")
        else:
            edit_df = (
                pd.DataFrame(edit_rows)
                .sort_values("_day")
                .set_index("_day")
            )
            available_by_day = {
                str(index): float(row["Dostępne"])
                for index, row in edit_df.iterrows()
            }
            edited_days = st.data_editor(
                edit_df[["Dzień", "Dzień tyg.", "Dostępne", "Godziny"]],
                column_config={
                    "Dostępne": st.column_config.NumberColumn(
                        "Dostępne (bez tego bloku)", format="%.2f"
                    ),
                    "Godziny": st.column_config.NumberColumn(
                        "Godziny", format="%.2f", min_value=0.0, max_value=24.0
                    ),
                },
                disabled=["Dzień", "Dzień tyg.", "Dostępne"],
                hide_index=True,
                num_rows="fixed",
                key=f"edit_days_{nonce}_{assignment_id}",
                width="stretch",
            )
            if st.button(
                "Zapisz dni",
                key=f"edit_save_days_{nonce}",
                type="primary",
            ):
                validation_errors: List[str] = []
                new_hours: Dict[str, float] = {}
                for day_str, row in edited_days.iterrows():
                    try:
                        hours_value = float(row["Godziny"])
                    except (TypeError, ValueError):
                        hours_value = 0.0
                    if hours_value < 0:
                        validation_errors.append(
                            f"{day_str}: godziny nie mogą być ujemne."
                        )
                        continue
                    available = available_by_day.get(str(day_str), 0.0)
                    if hours_value > available + 0.001:
                        validation_errors.append(
                            f"{day_str}: maksymalnie {available:.2f} h."
                        )
                    new_hours[str(day_str)] = hours_value

                if validation_errors:
                    st.error("Popraw godziny: " + " ".join(validation_errors))
                else:
                    updated = update_assignment_days(
                        existing=existing,
                        new_hours=new_hours,
                        constructor=edit_constructor,
                    )
                    if updated is None:
                        st.error(
                            "Przypisanie musi mieć co najmniej jedną godzinę."
                        )
                    else:
                        st.session_state["constructor_assignments"] = (
                            replace_assignment(assignments, updated)
                        )
                        st.session_state["assignments_dirty"] = True
                        _set_assignment_flash(
                            f"Zapisano ręczną edycję "
                            f"{_assignment_total_hours(updated):.1f} h."
                        )
                        st.session_state.pop("editing_assignment_id", None)
                        st.session_state["assignment_edit_nonce"] = nonce + 1
                        st.rerun()


def render_resource_planning_section(
    core_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    project_map: Dict[str, str],
    selected_projects: List[str],
    constructors: List[str],
    effective_daily_hours: float,
) -> None:
    st.subheader("Planowanie konstruktorów")
    st.caption("Tabela obejmuje dni robocze (pon.–pt.) w zakresie dat każdego projektu.")
    flash = st.session_state.pop("assignment_flash", None)
    flash_level = st.session_state.pop("assignment_flash_level", "success")
    if flash:
        if flash_level == "warning":
            st.warning(flash)
        else:
            st.success(flash)

    if not constructors:
        st.info(
            "Dodaj konstruktorów w panelu bocznym (sekcja „Konstruktorzy”), "
            "aby przypisywać godziny."
        )
        return

    assignments = st.session_state["constructor_assignments"]
    dirty = bool(st.session_state.get("assignments_dirty", False))
    if dirty:
        st.warning("Masz niezapisane zmiany. Kliknij „Zapisz”, aby utrwalić je na dysku.")

    def render_save_controls(suffix: str) -> None:
        current_dirty = bool(st.session_state.get("assignments_dirty", False))
        save_col, revert_col, hint_col = st.columns([1, 1, 3])
        if save_col.button(
            "Zapisz",
            type="primary",
            key=f"save_assignments_{suffix}",
        ):
            save_constructor_assignments(
                st.session_state["constructor_assignments"]
            )
            st.session_state["assignments_dirty"] = False
            _set_assignment_flash("Zapisano przypisania konstruktorów.")
            st.rerun()
        if revert_col.button(
            "Cofnij zmiany",
            key=f"revert_assignments_{suffix}",
            disabled=not current_dirty,
        ):
            st.session_state["constructor_assignments"] = (
                load_constructor_assignments()
            )
            st.session_state["assignments_dirty"] = False
            st.session_state["assignment_selection"] = set()
            st.session_state["assign_editor_nonce"] = (
                int(st.session_state.get("assign_editor_nonce", 0)) + 1
            )
            st.session_state.pop("editing_assignment_id", None)
            st.session_state["assignment_edit_nonce"] = (
                int(st.session_state.get("assignment_edit_nonce", 0)) + 1
            )
            _set_assignment_flash("Przywrócono ostatni zapis.")
            st.rerun()
        hint_col.caption("Zmiany są trwałe dopiero po kliknięciu „Zapisz”.")

    render_save_controls("top")

    if st.session_state.get("assign_constructor") not in constructors:
        st.session_state["assign_constructor"] = constructors[0]
    # Read the selected constructor before building the table.  The selectbox
    # itself is rendered below, directly above the assignment buttons.
    selected_constructor = st.session_state["assign_constructor"]

    project_ranges = project_date_ranges(core_df)
    selection = set(st.session_state.get("assignment_selection", set()))
    table = build_project_day_table(
        core_df=core_df,
        selected_projects=selected_projects,
        project_map=project_map,
        daily_df=daily_df,
        assignments=assignments,
        effective_daily_hours=effective_daily_hours,
        constructor=selected_constructor,
        selection=set(st.session_state.get("assignment_selection", set())),
    )

    if table.empty:
        st.info(
            "Brak dni do zaplanowania — wybrane projekty nie mają kompletnych dat "
            "start/koniec w core itemach."
        )
    else:
        valid_keys = set(table.index)
        selection = set(st.session_state.get("assignment_selection", set())) & valid_keys
        st.session_state["assignment_selection"] = selection
        table["selected"] = table.index.isin(selection)
        nonce = int(st.session_state.get("assign_editor_nonce", 0))
        table_sig = hashlib.md5(
            "|".join(table.index.tolist()).encode("utf-8")
        ).hexdigest()[:8]
        display_columns = [
            "selected",
            "project",
            "date_str",
            "weekday",
            "plan_hours",
            "available_h",
            "assigned_project_h",
        ]
        edited = st.data_editor(
            table[display_columns],
            column_config={
                "selected": st.column_config.CheckboxColumn(
                    "Zaznacz",
                    help="Zaznacz dwa dni w obrębie projektu, aby wybrać cały zakres.",
                    default=False,
                ),
                "project": st.column_config.TextColumn("Projekt"),
                "date_str": st.column_config.TextColumn("Dzień"),
                "weekday": st.column_config.TextColumn("Dzień tyg."),
                "plan_hours": st.column_config.NumberColumn("Plan (h)", format="%.2f"),
                "available_h": st.column_config.NumberColumn(
                    f"Dostępne h – {selected_constructor}",
                    format="%.2f",
                    help=(
                        "Dostępność globalna konstruktora w danym dniu, po odjęciu "
                        "wszystkich przypisań, w tym innych zadań spoza projektów."
                    ),
                ),
                "assigned_project_h": st.column_config.NumberColumn(
                    "Przypisane do projektu (h)", format="%.2f"
                ),
            },
            disabled=[
                "project",
                "date_str",
                "weekday",
                "plan_hours",
                "available_h",
                "assigned_project_h",
            ],
            hide_index=True,
            num_rows="fixed",
            key=f"assign_editor_{nonce}_{table_sig}",
            width="stretch",
        )

        selection_before = set(st.session_state.get("assignment_selection", set()))
        current_selection = set(
            edited.index[edited["selected"].fillna(False)].tolist()
        )
        all_keys = table.index.tolist()
        if current_selection != selection_before:
            expanded = expand_selection_to_ranges(current_selection, all_keys)
            st.session_state["assignment_selection"] = expanded
            if expanded != current_selection:
                st.session_state["assign_editor_nonce"] = nonce + 1
                st.rerun()

        selection = set(st.session_state.get("assignment_selection", set()))
        st.caption(
            f"Zaznaczono {len(selection)} dni. "
            "Zaznaczenie dwóch skrajnych dni w projekcie automatycznie wypełnia zakres."
        )

        from_start_col, to_end_col, clear_col = st.columns([1, 1, 2])
        from_start = from_start_col.checkbox(
            "Od początku",
            key="assign_from_start",
            help="Rozszerza zakres do najwcześniejszego startu projektu.",
        )
        to_end = to_end_col.checkbox(
            "Do końca",
            key="assign_to_end",
            help="Rozszerza zakres do najpóźniejszego terminu projektu.",
        )
        if clear_col.button(
            "Wyczyść zaznaczenie",
            key="clear_assignment_selection",
            disabled=not selection,
        ):
            st.session_state["assignment_selection"] = set()
            st.session_state["assign_editor_nonce"] = nonce + 1
            st.rerun()

        selected_constructor = st.selectbox(
            "Konstruktor do przypisania",
            options=constructors,
            key="assign_constructor",
            help=(
                "Dostępne godziny konstruktora to efektywne godziny dziennie "
                "pomniejszone o jego wcześniejsze przypisania w danym dniu."
            ),
        )

        full_col, half_col = st.columns(2)
        assign_full = full_col.button(
            "Przypisz pełne godziny",
            key="assign_full_hours",
            disabled=not selection,
            help=(
                f"Przypisuje min(dostępne, {effective_daily_hours:.1f} h) "
                "na każdy zaznaczony dzień."
            ),
        )
        assign_half = half_col.button(
            "Przypisz połowę godzin",
            key="assign_half_hours",
            disabled=not selection,
            help=(
                f"Przypisuje min(dostępne, {effective_daily_hours / 2:.1f} h) "
                "na każdy zaznaczony dzień."
            ),
        )

        if assign_full or assign_half:
            grouped_days: Dict[str, List[date]] = defaultdict(list)
            for key in selection:
                try:
                    project_id, day_str = str(key).split("::", 1)
                    grouped_days[project_id].append(date.fromisoformat(day_str))
                except (ValueError, TypeError):
                    continue
            ranges_with_names: List[Tuple[str, str, date, date]] = []
            for project_id, days in grouped_days.items():
                start, end = min(days), max(days)
                if from_start and project_id in project_ranges:
                    start = min(start, project_ranges[project_id][0])
                if to_end and project_id in project_ranges:
                    end = max(end, project_ranges[project_id][1])
                ranges_with_names.append(
                    (project_id, project_map.get(project_id, project_id), start, end)
                )

            mode = "full" if assign_full else "half"
            target_hours = (
                effective_daily_hours if mode == "full" else effective_daily_hours / 2
            )
            created, working = assign_constructor_to_ranges(
                assignments=assignments,
                constructor=selected_constructor,
                ranges_with_names=ranges_with_names,
                mode=mode,
                target_hours=target_hours,
                effective_daily_hours=effective_daily_hours,
            )
            if not created:
                st.warning(
                    "Brak dostępnych godzin w zaznaczonym zakresie — "
                    "konstruktor jest już w pełni obciążony."
                )
            else:
                st.session_state["constructor_assignments"] = working
                st.session_state["assignments_dirty"] = True
                assigned_hours = sum(
                    sum(float(value) for value in block["days"].values())
                    for block in created
                )
                _set_assignment_flash(
                    f"Przypisano {assigned_hours:.1f} h konstruktorowi "
                    f"{selected_constructor} w {len(created)} "
                    f"{'bloku' if len(created) == 1 else 'blokach'}."
                )
                st.rerun()

    render_budget_assignment_form(
        assignments=st.session_state["constructor_assignments"],
        constructors=constructors,
        effective_daily_hours=effective_daily_hours,
        project_map=project_map,
        selected_projects=selected_projects,
        project_ranges=project_ranges,
        selection=set(st.session_state.get("assignment_selection", set())),
    )

    with st.expander("Obciążenia inne niż projekty", expanded=False):
        st.caption(
            "Zablokuj konstruktora na zadanie spoza listy projektów (np. szkolenie, "
            "urlop, projekt wewnętrzny). Godziny zmniejszą jego dostępność we wszystkich "
            "projektach."
        )
        if st.session_state.get("ext_constructor") not in constructors:
            st.session_state["ext_constructor"] = constructors[0]
        ext_constructor = st.selectbox(
            "Konstruktor", options=constructors, key="ext_constructor"
        )
        ext_label = st.text_input(
            "Nazwa zadania",
            key="ext_label",
            placeholder="np. Szkolenie, urlop, projekt wewnętrzny",
        )
        ext_range = st.date_input(
            "Zakres dat",
            value=(date.today(), date.today()),
            key="ext_range",
        )
        if isinstance(ext_range, (tuple, list)) and len(ext_range) == 2:
            ext_start, ext_end = ext_range
        elif isinstance(ext_range, date):
            ext_start = ext_end = ext_range
        else:
            ext_start = ext_end = date.today()
        ext_mode = st.radio(
            "Liczba godzin",
            ["Pełne", "Połowa", "Własne"],
            horizontal=True,
            key="ext_mode",
        )
        custom_default = float(st.session_state.get("effective_daily_hours", 6.0))
        ext_custom = st.number_input(
            "Godziny dziennie",
            min_value=0.0,
            max_value=24.0,
            value=custom_default,
            step=0.5,
            format="%.1f",
            key="ext_custom",
            disabled=(ext_mode != "Własne"),
        )
        if st.button("Dodaj obciążenie", key="ext_add"):
            if not ext_label.strip():
                st.error("Podaj nazwę zadania.")
            elif ext_end < ext_start:
                st.error("Data końcowa nie może być wcześniejsza od początkowej.")
            else:
                target_hours = (
                    effective_daily_hours
                    if ext_mode == "Pełne"
                    else effective_daily_hours / 2
                    if ext_mode == "Połowa"
                    else ext_custom
                )
                block, working = add_external_assignment(
                    assignments=assignments,
                    constructor=ext_constructor,
                    label=ext_label.strip(),
                    start=ext_start,
                    end=ext_end,
                    mode="full" if ext_mode == "Pełne" else "half" if ext_mode == "Połowa" else "custom",
                    target_hours=target_hours,
                    effective_daily_hours=effective_daily_hours,
                )
                if block is None:
                    st.warning(
                        "Brak dostępnych godzin w wybranym zakresie — "
                        "konstruktor jest już w pełni obciążony."
                    )
                else:
                    st.session_state["constructor_assignments"] = working
                    st.session_state["assignments_dirty"] = True
                    assigned_hours = sum(float(value) for value in block["days"].values())
                    _set_assignment_flash(
                        f"Dodano obciążenie „{ext_label.strip()}”: "
                        f"{assigned_hours:.1f} h."
                    )
                    st.rerun()

    assignments = st.session_state["constructor_assignments"]
    with st.expander("Zarządzaj przypisaniami", expanded=False):
        if not assignments:
            st.caption("Brak zapisanych przypisań.")
        else:
            summary_rows = []
            for assignment in assignments:
                total_hours = _assignment_total_hours(assignment)
                requested_hours = assignment.get("requested_hours")
                summary_rows.append(
                    {
                        "Typ": "Inne zadanie"
                        if assignment.get("kind") == "external"
                        else "Projekt",
                        "Projekt / zadanie": assignment.get("label")
                        or assignment.get("project")
                        or "",
                        "Konstruktor": assignment.get("constructor", ""),
                        "Od": assignment.get("start_date", ""),
                        "Do": assignment.get("end_date", ""),
                        "Żądane h": round(float(requested_hours), 2)
                        if requested_hours is not None
                        else None,
                        "Przypisane h": total_hours,
                        "Tryb": _assignment_mode_label(assignment.get("mode")),
                    }
                )
            st.dataframe(
                pd.DataFrame(summary_rows),
                hide_index=True,
                width="stretch",
                column_config={
                    "Żądane h": st.column_config.NumberColumn(
                        "Żądane h", format="%.2f"
                    ),
                    "Przypisane h": st.column_config.NumberColumn(
                        "Przypisane h", format="%.2f"
                    ),
                },
            )
            options = {
                assignment["id"]: (
                    f"{assignment.get('constructor', '')} | "
                    f"{assignment.get('label') or assignment.get('project') or ''} | "
                    f"{assignment.get('start_date', '')}–{assignment.get('end_date', '')} | "
                    f"{_assignment_total_hours(assignment):.1f} h | "
                    f"{_assignment_mode_label(assignment.get('mode'))}"
                )
                for assignment in assignments
            }
            if st.session_state.get("manage_assignment_id") not in options:
                st.session_state["manage_assignment_id"] = next(iter(options))
            selected_assignment = st.selectbox(
                "Wybierz przypisanie",
                options=list(options.keys()),
                format_func=lambda key: options[key],
                key="manage_assignment_id",
            )
            edit_col, delete_col = st.columns(2)
            if edit_col.button(
                "Edytuj wybrane przypisanie",
                key="edit_assignment",
            ):
                st.session_state["editing_assignment_id"] = selected_assignment
                st.session_state["assignment_edit_nonce"] = (
                    int(st.session_state.get("assignment_edit_nonce", 0)) + 1
                )
                st.rerun()
            if delete_col.button(
                "Usuń wybrane przypisanie",
                key="delete_assignment",
            ):
                st.session_state["constructor_assignments"] = [
                    assignment
                    for assignment in assignments
                    if assignment["id"] != selected_assignment
                ]
                if (
                    st.session_state.get("editing_assignment_id")
                    == selected_assignment
                ):
                    st.session_state.pop("editing_assignment_id", None)
                st.session_state["assignments_dirty"] = True
                _set_assignment_flash("Usunięto przypisanie.")
                st.rerun()

    assignments = st.session_state["constructor_assignments"]
    render_assignment_editor(
        assignments=assignments,
        constructors=constructors,
        effective_daily_hours=effective_daily_hours,
        project_ranges=project_ranges,
    )

    st.divider()
    render_save_controls("bottom")


# ---- UI --------------------------------------------------------------------
def render_resource_usage(
    assignments: List[Dict[str, Any]],
    constructors: List[str],
    effective_daily_hours: float,
    selected_projects: List[str],
) -> None:
    st.subheader("ZUŻYCIE ZASOBÓW")
    st.caption(
        "Obciążenie konstruktorów w projektach oraz w zadaniach spoza listy projektów. "
        "Przy wybranych projektach widok pokazuje tylko przypisania do tych projektów; "
        "inne zadania są widoczne po wyczyszczeniu wyboru. Wykresy pokazują dzienne użycie "
        "godzin, a etykiety nad słupkami — projekty/zadania obciążające dany dzień. "
        "Oś czasu obejmuje pełny wybrany zakres dat, również dni bez obciążenia. "
        "Wszyscy dostępni konstruktorzy są pokazani w zakładkach."
    )
    if st.session_state.get("assignments_dirty"):
        st.warning(
            "Widok obejmuje niezapisane zmiany. Przejdź do DAILY PLANNED EFFORT "
            "i kliknij „Zapisz”, aby utrwalić je na dysku."
        )

    expanded = expand_assignment_days(assignments)
    if expanded.empty:
        st.info(
            "Brak przypisań konstruktorów. Dodaj je w zakładce DAILY PLANNED EFFORT."
        )
        return

    if selected_projects:
        # External tasks are global, so they should not masquerade as loads of
        # a selected project.  Show them only when no project filter is active.
        expanded = expanded[
            (expanded["kind"] == "project")
            & (expanded["project_id"].isin(selected_projects))
        ]
        if expanded.empty:
            st.info("Brak przypisań do wybranych projektów.")
            if any(item.get("kind") == "external" for item in assignments):
                st.caption(
                    "Inne zadania są ukryte przy filtrze projektów — "
                    "wyczyść wybór, aby je zobaczyć."
                )
            return
        if any(item.get("kind") == "external" for item in assignments):
            st.caption(
                "Wybrano projekty — pokazuję tylko przypisania do tych projektów. "
                "Inne zadania zobaczysz po wyczyszczeniu wyboru projektów."
            )
        else:
            st.caption("Wybrano projekty — pokazuję tylko przypisania do tych projektów.")
    else:
        st.caption(
            "Nie wybrano projektów — pokazuję wszystkie zapisane przypisania, "
            "w tym inne zadania."
        )

    # The usage view should span whole assignment ranges, not only days that
    # actually have hours.  Include block start/end dates in the default range.
    range_dates: List[date] = []
    for assignment in assignments:
        if selected_projects and (
            assignment.get("kind") == "external"
            or assignment.get("project_id") not in selected_projects
        ):
            continue
        for key in ("start_date", "end_date"):
            try:
                range_dates.append(date.fromisoformat(str(assignment.get(key))))
            except (TypeError, ValueError):
                continue

    expanded_min = _to_date_only(expanded["date"].min())
    expanded_max = _to_date_only(expanded["date"].max())
    candidate_dates = [
        value
        for value in [expanded_min, expanded_max, *range_dates]
        if value is not None
    ]
    if not candidate_dates:
        st.info("Brak poprawnych dat w przypisaniach.")
        return
    min_date = min(candidate_dates)
    max_date = max(candidate_dates)
    range_sig = f"{min_date.isoformat()}::{max_date.isoformat()}"
    if st.session_state.get("resource_usage_range_sig") != range_sig:
        st.session_state["resource_usage_range_sig"] = range_sig
        st.session_state["resource_usage_range"] = (min_date, max_date)
    date_range = st.date_input(
        "Zakres dat",
        min_value=min_date,
        max_value=max_date,
        key="resource_usage_range",
    )
    if isinstance(date_range, (tuple, list)) and len(date_range) == 2:
        start_filter, end_filter = date_range
    else:
        start_filter, end_filter = min_date, max_date

    if end_filter < start_filter:
        start_filter, end_filter = end_filter, start_filter
    expanded = expanded[
        (expanded["date"] >= start_filter) & (expanded["date"] <= end_filter)
    ]
    if expanded.empty:
        st.info("Brak przypisań w wybranym zakresie dat.")
        return

    if start_filter == end_filter:
        domain_start = pd.Timestamp(start_filter) - pd.Timedelta(days=0.5)
        domain_end = pd.Timestamp(end_filter) + pd.Timedelta(days=0.5)
    else:
        domain_start = pd.Timestamp(start_filter)
        domain_end = pd.Timestamp(end_filter)
    x_scale = alt.Scale(domain=[domain_start, domain_end], padding=10)

    present_constructors = sorted(
        {
            str(value).upper()
            for value in expanded["constructor"].dropna().tolist()
            if str(value).strip()
        }
    )
    available_constructors = list(constructors)
    for constructor in present_constructors:
        if constructor not in available_constructors:
            available_constructors.append(constructor)
    if not available_constructors:
        st.info("Brak konstruktorów w przypisaniach.")
        return

    expanded = expanded.copy()
    expanded["category"] = [
        f"Inne: {row['label']}" if row["kind"] == "external" else row["project"]
        for _, row in expanded.iterrows()
    ]
    expanded["category_short"] = expanded["category"].map(short_label)

    label_df = (
        expanded.groupby(["date", "constructor"], as_index=False)
        .agg(
            total_hours=("hours", "sum"),
            projects=(
                "category",
                lambda values: ", ".join(sorted({short_label(value, 18) for value in values})),
            ),
        )
    )

    workdays = int(pd.bdate_range(start_filter, end_filter).size)
    capacity = float(effective_daily_hours) * workdays

    tabs = st.tabs(available_constructors)
    for tab, constructor in zip(tabs, available_constructors):
        with tab:
            con_df = expanded[expanded["constructor"] == constructor]
            con_labels = label_df[label_df["constructor"] == constructor]
            total_h = float(con_df["hours"].sum()) if not con_df.empty else 0.0
            utilization = (total_h / capacity * 100) if capacity > 0 else 0.0

            if con_df.empty:
                st.info("Brak obciążenia w wybranym zakresie dat.")
            else:
                bars = (
                    alt.Chart(con_df)
                    .mark_bar()
                    .encode(
                        x=alt.X(
                            "date:T",
                            title="Dzień",
                            scale=x_scale,
                            axis=alt.Axis(format="%d.%m", labelAngle=-45),
                        ),
                        y=alt.Y("hours:Q", title="Godziny", stack="zero"),
                        color=alt.Color("category_short:N", title="Projekt / zadanie"),
                        tooltip=[
                            alt.Tooltip("date:T", title="Dzień", format="%d.%m.%Y"),
                            alt.Tooltip("category:N", title="Projekt / zadanie"),
                            alt.Tooltip("hours:Q", title="Godziny", format=".2f"),
                        ],
                    )
                )
                labels = (
                    alt.Chart(con_labels)
                    .mark_text(angle=270, dx=-4, dy=0, fontSize=8, color="#444")
                    .encode(
                        x=alt.X(
                            "date:T",
                            title="Dzień",
                            scale=x_scale,
                            axis=alt.Axis(format="%d.%m", labelAngle=-45),
                        ),
                        y=alt.Y("total_hours:Q", title=None),
                        text=alt.Text("projects:N"),
                    )
                )
                st.altair_chart(
                    (bars + labels).properties(height=360, width="container"),
                    use_container_width=True,
                )

            c1, c2, c3 = st.columns(3)
            c1.metric("Suma godzin", f"{total_h:.1f} h")
            c2.metric(
                "Dni z obciążeniem",
                int(con_df["date"].nunique()) if not con_df.empty else 0,
            )
            c3.metric(
                "Wykorzystanie",
                f"{utilization:.0f}%",
                help=(
                    f"{total_h:.1f} h / ({effective_daily_hours:.1f} h × "
                    f"{workdays} dni roboczych) = {utilization:.1f}%"
                ),
            )

            if not con_df.empty:
                detail = (
                    con_df.groupby(["date", "category"], as_index=False)["hours"]
                    .sum()
                    .sort_values(["date", "category"])
                )
                detail["Dzień"] = detail["date"].apply(
                    lambda value: value.strftime("%d.%m.%Y")
                )
                detail = detail.rename(
                    columns={"category": "Projekt / zadanie", "hours": "Godziny"}
                )[["Dzień", "Projekt / zadanie", "Godziny"]]
                st.dataframe(
                    detail,
                    hide_index=True,
                    width="stretch",
                    column_config={
                        "Godziny": st.column_config.NumberColumn(
                            "Godziny", format="%.2f"
                        )
                    },
                )


def render_required_constructors_section(
    core_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    selected_projects: List[str],
    effective_daily_hours: float,
    fallback_start: date,
    fallback_end: date,
) -> None:
    required_project_ranges = project_date_ranges(core_df)
    selected_project_ranges = [
        required_project_ranges[project_id]
        for project_id in selected_projects
        if project_id in required_project_ranges
    ]
    if selected_project_ranges:
        required_default_start = min(
            project_start for project_start, _ in selected_project_ranges
        )
        required_default_end = max(
            project_end for _, project_end in selected_project_ranges
        )
    else:
        required_default_start = fallback_start
        required_default_end = fallback_end

    if "required_range_start" not in st.session_state:
        st.session_state["required_range_start"] = required_default_start
    if "required_range_end" not in st.session_state:
        st.session_state["required_range_end"] = required_default_end
    if not st.session_state.get("required_range_manual", False):
        st.session_state["required_range_start"] = required_default_start
        st.session_state["required_range_end"] = required_default_end
    required_range_start = st.session_state["required_range_start"]
    required_range_end = st.session_state["required_range_end"]
    if not isinstance(required_range_start, date):
        st.session_state["required_range_start"] = required_default_start
        required_range_start = required_default_start
    if not isinstance(required_range_end, date):
        st.session_state["required_range_end"] = required_default_end
        required_range_end = required_default_end

    required_range_valid = required_range_start <= required_range_end
    if required_range_valid:
        required_daily_df = daily_df[
            (daily_df["date"] >= required_range_start)
            & (daily_df["date"] <= required_range_end)
        ]
        required_total_h = float(required_daily_df["daily_hours"].sum())
        required_workdays = int(
            pd.bdate_range(required_range_start, required_range_end).size
        )
        required_capacity_per_person = effective_daily_hours * required_workdays
        if required_capacity_per_person > 0:
            required_full_people = int(
                math.floor(required_total_h / required_capacity_per_person + 1e-9)
            )
            required_remainder_hours = max(
                0.0,
                required_total_h
                - required_full_people * required_capacity_per_person,
            )
            required_label = format_required_constructors(
                required_full_people, required_remainder_hours
            )
        else:
            required_full_people = 0
            required_remainder_hours = 0.0
            required_label = "—"
    else:
        required_total_h = 0.0
        required_workdays = 0
        required_capacity_per_person = 0.0
        required_full_people = 0
        required_remainder_hours = 0.0
        required_label = "—"

    required_remainder_text = (
        f" i {_polish_hours_label(required_remainder_hours)}"
        if required_remainder_hours >= 0.5
        else ""
    )
    if required_range_valid:
        required_help = (
            f"Zakres {required_range_start:%d.%m.%Y}–"
            f"{required_range_end:%d.%m.%Y}: "
            f"pojemność jednej osoby {effective_daily_hours:.1f} h × "
            f"{required_workdays} dni roboczych = "
            f"{required_capacity_per_person:.1f} h. "
            f"{required_total_h:.1f} h planu = "
            f"{_polish_person_label(required_full_people)} full"
            f"{required_remainder_text}."
        )
    else:
        required_help = (
            "Wybierz poprawny zakres dat: początek nie może być "
            "późniejszy niż koniec."
        )

    st.markdown("**Wymagani konstruktorzy — zakres obliczenia**")
    req_metric_col, req_start_col, req_end_col = st.columns([1.2, 1, 1])
    with req_metric_col:
        st.metric(
            "Wymagani konstruktorzy",
            required_label,
            help=required_help,
        )
    with req_start_col:
        st.date_input(
            "Początek zakresu",
            key="required_range_start",
            on_change=_persist_required_range_from_state,
        )
        st.button(
            "Resetuj początek",
            key="reset_required_range_start",
            on_click=_reset_required_range_start,
            args=(required_default_start, required_default_end),
        )
    with req_end_col:
        st.date_input(
            "Koniec zakresu",
            key="required_range_end",
            on_change=_persist_required_range_from_state,
        )
        st.button(
            "Resetuj koniec",
            key="reset_required_range_end",
            on_click=_reset_required_range_end,
            args=(required_default_end, required_default_start),
        )
    st.caption(
        f"Domyślny zakres projektu: "
        f"{required_default_start:%d.%m.%Y}–"
        f"{required_default_end:%d.%m.%Y}."
    )
    if not required_range_valid:
        st.warning(
            "Data początkowa zakresu wymaganych konstruktorów jest "
            "późniejsza niż data końcowa."
        )


def main() -> None:
    st.set_page_config(page_title="Wrike KPI – Core Items", layout="wide")
    reset_logs()
    reset_perf_metrics()
    check_password()
    _init_resource_state()
    st.title("KPI DASHBOARD")

    pages = ["KPI DASHBOARD", "DAILY PLANNED EFFORT", "ZUŻYCIE ZASOBÓW"]
    st.session_state.setdefault("page", pages[0])

    with st.sidebar:
        current_page = st.session_state.get("page", pages[0])
        page_index = pages.index(current_page) if current_page in pages else 0
        page = st.radio("Widok", pages, index=page_index)
        st.session_state["page"] = page
        st.divider()
        st.radio("Filtruj zadania", ["ALL", "\u2622 BOM"], horizontal=True, key="task_filter")
        st.radio("Status", ["ALL", "COMPLETED"], horizontal=True, key="status_filter")
        kpi_date_from = st.date_input("KPI due od", value=None, key="kpi_date_from")
        kpi_date_to = st.date_input("KPI due do", value=None, key="kpi_date_to")
        kpi1_tolerance_pct = st.number_input(
            "Tolerancja opóźnienia KPI 1 (%)",
            min_value=0,
            max_value=100,
            value=20,
            step=1,
            key="kpi1_tolerance_pct",
            help=(
                "Po przekroczeniu zaplanowanego czasu ocena maleje liniowo. "
                "Przy tolerancji 20% wynik spada do zera po przekroczeniu planu o 20%."
            ),
        )
        st.caption(
            "Legenda: 1 = wykonanie w planowanym czasie; między planem a końcem tolerancji "
            "ocena liniowo spada; po przekroczeniu tego progu = 0. "
            "Otwarte zadania nie są oceniane ani wliczane do średniej."
        )
        st.header("Konfiguracja")
        base_url = DEFAULT_BASE_URL
        api_key = DEFAULT_API_KEY
        client_folder = DEFAULT_CLIENT_FOLDER
        core_task_type = DEFAULT_CORE_TASK_TYPE
        bom_task_type = DEFAULT_BOM_TASK_TYPE
        planned_field_id = DEFAULT_PLANNED_FIELD_ID
        completed_status_id = DEFAULT_COMPLETED_STATUS_ID
        st.caption("Parametry ładowane z .env.")
        st.header("Konstruktorzy")
        with st.form("constructors_form"):
            constructors_text = st.text_input(
                "Inicjały (po przecinku)",
                value=", ".join(st.session_state.get("constructors", [])),
                help=(
                    "Każdy konstruktor ma dziennie tyle godzin, ile wynosi pole "
                    "„Efektywne godziny dziennie”."
                ),
            )
            if st.form_submit_button("Zapisz konstruktorów"):
                parsed_constructors = parse_constructors(constructors_text)
                if not parsed_constructors:
                    st.error("Podaj co najmniej jednego konstruktora.")
                else:
                    st.session_state["constructors"] = parsed_constructors
                    save_constructors(parsed_constructors)
                    st.success("Zapisano listę konstruktorów.")
        if st.session_state.get("constructors"):
            st.caption(
                "Pula: " + ", ".join(st.session_state["constructors"])
            )
        if "refresh_timestamp" not in st.session_state:
            st.session_state["refresh_timestamp"] = None
        if "refresh_nonce" not in st.session_state:
            st.session_state["refresh_nonce"] = 0
        if st.button("Odśwież dane z Wrike", key="refresh"):
            clear_wrike_cache()
            st.session_state["refresh_nonce"] += 1
            st.session_state["refresh_timestamp"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        if st.session_state["refresh_timestamp"]:
            st.caption(f"Ostatnie odświeżenie: {st.session_state['refresh_timestamp']}")

    if not api_key or not client_folder:
        st.warning("Uzupełnij API key i ID folderu z projektami, aby pobrać dane.")
        return

    graph_t0 = time.perf_counter()
    solidworks_context = prepare_solidworks_context()
    _set_stage_time("graph", time.perf_counter() - graph_t0)

    # Step 1: projekty klienckie
    wrike_t0 = time.perf_counter()
    try:
        projects = fetch_client_projects(base_url, api_key, client_folder)
    except Exception as exc:  # noqa: BLE001
        st.error(str(exc))
        return

    project_map = {p["id"]: p.get("title", p["id"]) for p in projects}
    if not project_map:
        st.info("Brak dostępnych projektów w zdefiniowanym folderze.")
        return
    sorted_project_ids = sorted(project_map.keys(), key=lambda pid: project_map[pid].casefold())
    default_selection: List[str] = []
    selected_projects = st.multiselect(
        "Wybierz projekt(y):",
        options=sorted_project_ids,
        default=default_selection,
        format_func=lambda pid: project_map[pid],
    )

    if st.session_state["page"] == "ZUŻYCIE ZASOBÓW":
        render_resource_usage(
            assignments=st.session_state["constructor_assignments"],
            constructors=st.session_state["constructors"],
            effective_daily_hours=float(st.session_state["effective_daily_hours"]),
            selected_projects=selected_projects,
        )
        return

    if not selected_projects:
        st.info("Zaznacz przynajmniej jeden projekt, aby zobaczyć KPI.")
        return

    # Step 2: taski projektu
    with st.spinner("Pobieram taski projektu z Wrike..."):
        try:
            tasks: List[Dict[str, Any]] = []
            existing_ids: set[str] = set()
            for pid in selected_projects:
                project_tasks = fetch_tasks_for_project(base_url, api_key, pid)
                for task in project_tasks:
                    tid = task["id"]
                    if tid in existing_ids:
                        continue
                    task["_selected_project_id"] = pid
                    tasks.append(task)
                    existing_ids.add(tid)
        except Exception as exc:  # noqa: BLE001
            st.error(str(exc))
            return
    _set_stage_time("wrike_fetch", time.perf_counter() - wrike_t0)

    if not tasks:
        st.info("Brak tasków w wybranych projektach.")
        return

    # Step 3-5: KPI + drzewo
    agg_cache_key = json.dumps(
        {
            "selected_projects": sorted(selected_projects),
            "refresh_nonce": st.session_state.get("refresh_nonce", 0),
            "core_task_type": core_task_type,
            "bom_task_type": bom_task_type,
            "planned_field_id": planned_field_id,
            "completed_status_id": completed_status_id,
        },
        sort_keys=True,
    )
    aggregate_t0 = time.perf_counter()
    cached_agg = _kpi_aggregate_get(agg_cache_key)
    if cached_agg is not None:
        _inc_perf_metric("kpi_aggregate_cache_hit")
        task_df, bom_df, summary = cached_agg
    else:
        _inc_perf_metric("kpi_aggregate_cache_miss")
        task_df, bom_df, summary = aggregate_core_items(
            tasks,
            core_task_type=core_task_type,
            bom_task_type=bom_task_type,
            planned_field_id=planned_field_id,
            completed_status_id=completed_status_id,
            project_lookup=project_map,
            allowed_project_ids=set(selected_projects),
            wrike_base_url=base_url,
            wrike_api_key=api_key,
            solidworks_context=solidworks_context,
        )
        _kpi_aggregate_set(agg_cache_key, task_df, bom_df, summary)
    _set_stage_time("aggregate", time.perf_counter() - aggregate_t0)
    task_df, bom_df, spec_col_count = expand_dynamic_spec_columns(task_df, bom_df)
    if st.session_state["page"] == "DAILY PLANNED EFFORT":
        st.subheader("DAILY PLANNED EFFORT")
        st.caption(
            "Obciążenie dzienne na podstawie planned_hours "
            "rozłożonych równo na dni robocze (pon-pt)"
        )

        core_df = pd.concat([task_df, bom_df], ignore_index=True)
        daily_df = compute_daily_allocated_effort(core_df)

        prev_daily = float(st.session_state.get("effective_daily_hours", 6.0))
        effective_daily_h = st.number_input(
            "Efektywne godziny dziennie",
            min_value=0.0,
            max_value=24.0,
            value=prev_daily,
            step=0.5,
            format="%.1f",
            help=(
                "Efektywna liczba godzin pracy dziennie. Każdy konstruktor ma "
                "dziennie tyle godzin dostępnych, ile wynosi ta wartość."
            ),
        )
        if effective_daily_h != prev_daily:
            st.session_state["effective_daily_hours"] = effective_daily_h
            _write_json_file(DAILY_HOURS_FILE, effective_daily_h)

        if daily_df.empty:
            st.info(
                "Brak danych – żaden core item nie ma jednocześnie "
                "planned_hours > 0 oraz obu dat (start, due) w dniach roboczych."
            )
        else:
            min_date = daily_df["date"].min()
            max_date = daily_df["date"].max()

            projects_sig = hashlib.md5(
                json.dumps(sorted(selected_projects), sort_keys=True).encode("utf-8")
            ).hexdigest()[:8]

            # Reset zakresu dat przy każdej zmianie zestawu projektów.
            if st.session_state.get("daily_effort_projects_sig") != projects_sig:
                st.session_state["daily_effort_projects_sig"] = projects_sig
                st.session_state["daily_effort_range"] = (min_date, max_date)

            date_range = st.date_input(
                "Zakres dat",
                min_value=min_date,
                max_value=max_date,
                key="daily_effort_range",
            )
            if isinstance(date_range, (tuple, list)) and len(date_range) == 2:
                start_filter, end_filter = date_range
            else:
                start_filter, end_filter = min_date, max_date

            filtered_df = daily_df[
                (daily_df["date"] >= start_filter) & (daily_df["date"] <= end_filter)
            ]

            if filtered_df.empty:
                st.info("Brak danych w wybranym zakresie dat.")
            else:
                total_h = float(filtered_df["daily_hours"].sum())
                n_dates = filtered_df["date"].nunique()
                n_items = filtered_df["title"].nunique()
                workdays_in_range = int(pd.bdate_range(start_filter, end_filter).size)

                assignments = st.session_state["constructor_assignments"]
                expanded_assignments = expand_assignment_days(
                    assignments, start_filter, end_filter
                )
                if expanded_assignments.empty:
                    relevant_assignments = expanded_assignments
                else:
                    relevant_assignments = expanded_assignments[
                        (expanded_assignments["kind"] == "external")
                        | (expanded_assignments["project_id"].isin(selected_projects))
                    ]
                assigned_project_h = (
                    float(
                        relevant_assignments[
                            relevant_assignments["kind"] == "project"
                        ]["hours"].sum()
                    )
                    if not relevant_assignments.empty
                    else 0.0
                )
                assigned_external_h = (
                    float(
                        relevant_assignments[
                            relevant_assignments["kind"] == "external"
                        ]["hours"].sum()
                    )
                    if not relevant_assignments.empty
                    else 0.0
                )
                project_assignment_rows = (
                    relevant_assignments[
                        relevant_assignments["kind"] == "project"
                    ]
                    if not relevant_assignments.empty
                    else relevant_assignments
                )

                average_daily_h = total_h / workdays_in_range if workdays_in_range else 0.0
                plan_display_h = int(round(total_h))
                assigned_display_h = int(round(assigned_project_h))
                remaining_display_h = plan_display_h - assigned_display_h
                c1, c2, c3 = st.columns(3)
                c1.metric(
                    "Plan - przypisane = pozostało",
                    f"{plan_display_h}-{assigned_display_h}={remaining_display_h}",
                    help=(
                        "Planowane godziny minus godziny przypisane do wybranych "
                        "projektów w wybranym zakresie dat. Wynik ujemny oznacza, "
                        "że przypisano więcej godzin niż plan. Inne zadania nie są "
                        "wliczane do tej wartości."
                    ),
                )
                c2.metric("Dni roboczych", n_dates)
                c3.metric("Core itemów", n_items)
                if remaining_display_h < 0:
                    st.caption(
                        f"Przypisano więcej niż plan o "
                        f"{abs(remaining_display_h)} h."
                    )
                st.caption(
                    f"Przypisania w zakresie: projekty {assigned_project_h:.1f} h, "
                    f"inne zadania {assigned_external_h:.1f} h."
                )
                if assigned_project_h <= 0 and assigned_external_h <= 0:
                    st.caption(
                        "Brak przypisań konstruktorów w tym zakresie."
                    )
                elif assigned_project_h <= 0:
                    st.caption(
                        "Brak przypisań do wybranych projektów — "
                        "w zakresie są tylko inne zadania."
                    )

                st.subheader("Planned effort visualisation")
                pivot = filtered_df.pivot_table(
                    index="date",
                    columns="project",
                    values="daily_hours",
                    aggfunc="sum",
                    fill_value=0,
                )
                long_chart = pivot.reset_index().melt(
                    id_vars=["date"], var_name="project", value_name="daily_hours"
                )
                bars = alt.Chart(long_chart).mark_bar().encode(
                    x=alt.X("date:T", title="Date"),
                    y=alt.Y("daily_hours:Q", title="Godziny"),
                    color=alt.Color("project:N", title="Project"),
                )
                chart = bars
                if not project_assignment_rows.empty:
                    line_df = (
                        project_assignment_rows.groupby("date", as_index=False)[
                            "hours"
                        ].sum()
                    )
                    line = alt.Chart(line_df).mark_line(
                        point=True, color="#d32f2f", strokeWidth=2
                    ).encode(
                        x=alt.X("date:T", title="Date"),
                        y=alt.Y("hours:Q", title="Godziny"),
                        tooltip=[
                            alt.Tooltip("date:T", title="Dzień", format="%d.%m.%Y"),
                            alt.Tooltip("hours:Q", title="Przypisane godziny", format=".2f"),
                        ],
                    )
                    chart = bars + line

                chart_col, average_col = st.columns([4, 1])
                chart_col.altair_chart(
                    chart.properties(width="container"), use_container_width=True
                )
                if not project_assignment_rows.empty:
                    st.caption(
                        "Czerwona linia = suma godzin przypisanych do wybranych "
                        "projektów na dany dzień."
                    )
                    if assigned_external_h > 0:
                        st.caption(
                            f"Inne zadania poza projektami: "
                            f"{assigned_external_h:.1f} h "
                            "(nie są rysowane na wykresie)."
                        )
                elif assigned_external_h > 0:
                    st.caption(
                        f"Inne zadania poza projektami: "
                        f"{assigned_external_h:.1f} h."
                    )
                with average_col:
                    st.metric(
                        "Średnie obciążenie",
                        f"{average_daily_h:.1f}".replace(".", ",") + " h/dzień",
                        help=(
                            f"{total_h:.2f} h łącznie podzielone przez "
                            f"{workdays_in_range} dni roboczych w wybranym zakresie. "
                            "Uwzględnia także dni robocze bez zaplanowanego obciążenia."
                        ),
                    )

        render_required_constructors_section(
            core_df=core_df,
            daily_df=daily_df,
            selected_projects=selected_projects,
            effective_daily_hours=effective_daily_h,
            fallback_start=min_date,
            fallback_end=max_date,
        )

        render_resource_planning_section(
            core_df=core_df,
            daily_df=daily_df,
            project_map=project_map,
            selected_projects=selected_projects,
            constructors=st.session_state.get("constructors", []),
            effective_daily_hours=float(
                st.session_state.get("effective_daily_hours", 6.0)
            ),
        )

        with st.expander("Log (debug)"):
            _init_logs()
            _init_perf_metrics()
            st.write("Cache metrics:")
            st.json(
                {
                    "wrike_comments_cache_hit": st.session_state["perf_metrics"].get(
                        "wrike_comments_cache_hit", 0
                    ),
                    "wrike_comments_cache_miss": st.session_state["perf_metrics"].get(
                        "wrike_comments_cache_miss", 0
                    ),
                    "kpi_aggregate_cache_hit": st.session_state["perf_metrics"].get(
                        "kpi_aggregate_cache_hit", 0
                    ),
                    "kpi_aggregate_cache_miss": st.session_state["perf_metrics"].get(
                        "kpi_aggregate_cache_miss", 0
                    ),
                    "stage_times_sec": st.session_state["perf_metrics"].get(
                        "stage_times", {}
                    ),
                }
            )
            st.write("\n".join(st.session_state["logs"]))
        return

    core_cols = [
        ("project", "Project"),
        ("title", "Title"),
        ("solidworks", "SOLIDWORKS"),
        ("allocated_hours", "Alloc [h]"),
        ("used_hours_until_yesterday", "Used until yesterday (h)"),
        ("planned_hours", "Plan [h]"),
        ("alloc_vs_plan_pct", "Alloc vs plan %"),
        ("time_progress_pct", "Time progress %"),
        ("start_date", "Start date"),
        ("due_date", "Due date"),
        ("completed_date", "Completed date"),
        ("completed", "Completed"),
        ("due_today_or_past", "Due today/past"),
        ("permalink", "Permalink"),
        ("warnings", "Warnings"),
    ]
    for idx in range(spec_col_count):
        core_cols.insert(4 + idx, (f"solidworks_pdf_{idx + 1}", f"SPEC {idx + 1}"))

    # ---- Date filter logic ----
    date_filter_active = kpi_date_from is not None and kpi_date_to is not None

    def _filter_by_due(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or "due_date" not in df.columns:
            return df
        return df[(df["due_date"] >= kpi_date_from) & (df["due_date"] <= kpi_date_to)]

    if date_filter_active:
        task_df_kpi = _filter_by_due(task_df)
        bom_df_kpi = _filter_by_due(bom_df)
    else:
        task_df_kpi = task_df
        bom_df_kpi = bom_df

    # ---- KPI recalc ----
    if date_filter_active:
        planned_h = int(round(pd.concat(
            [task_df_kpi.get("planned_hours", pd.Series(dtype=float)),
             bom_df_kpi.get("planned_hours", pd.Series(dtype=float))], ignore_index=True).sum()))
        alloc_h = int(round(pd.concat(
            [task_df_kpi.get("allocated_hours", pd.Series(dtype=float)),
             bom_df_kpi.get("allocated_hours", pd.Series(dtype=float))], ignore_index=True).sum()))
        used_series = pd.concat(
            [task_df_kpi.get("used_hours_until_yesterday", pd.Series(dtype=float)),
             bom_df_kpi.get("used_hours_until_yesterday", pd.Series(dtype=float))], ignore_index=True).dropna()
        used_h = int(used_series.sum()) if not used_series.empty else 0
        planned_missing = int(pd.concat(
            [task_df_kpi.get("planned_hours", pd.Series(dtype=float)),
             bom_df_kpi.get("planned_hours", pd.Series(dtype=float))], ignore_index=True).isna().sum())
        ratio_total = round(alloc_h / planned_h * 100) if planned_h > 0 else None
    else:
        planned_h = summary["planned_hours"]
        alloc_h = summary["allocated_hours"]
        used_h = summary.get("used_until_yesterday", 0)
        planned_missing = summary["planned_missing"]
        ratio_total = round(alloc_h / planned_h * 100) if planned_h > 0 else None

    # KPI 1: average delivery-time score for completed BOM items due today or earlier.
    if not bom_df_kpi.empty:
        due_past = (
            bom_df_kpi[bom_df_kpi["due_today_or_past"] == True]
            if "due_today_or_past" in bom_df_kpi
            else pd.DataFrame()
        )
    else:
        due_past = pd.DataFrame()
    kpi1_scores = []
    for _, row in due_past.iterrows():
        score = compute_kpi1_score(
            start_date=row.get("start_date"),
            due_date=row.get("due_date"),
            completed=bool(row.get("completed", False)),
            completed_date=row.get("completed_date"),
            tolerance_pct=kpi1_tolerance_pct,
        )
        if score is not None:
            kpi1_scores.append(score)
    kpi1_val = round(sum(kpi1_scores) / len(kpi1_scores), 2) if kpi1_scores else 0.0

    # KPI 2: weighted used/planned ratio for completed BOM items.
    completed_bom = bom_df_kpi[bom_df_kpi["completed"] == True] if not bom_df_kpi.empty and "completed" in bom_df_kpi else pd.DataFrame()
    if not completed_bom.empty:
        kpi2 = weighted_hours_ratio(
            completed_bom.get("used_hours_until_yesterday", pd.Series(dtype=float)),
            completed_bom.get("planned_hours", pd.Series(dtype=float)),
        )
    else:
        kpi2 = 0.0

    # ---- Render KPI block ----
    def _kpi1_color(val: float) -> str:
        if val < 0.50:
            return "#d32f2f"   # red
        elif val <= 0.95:
            return "#f9a825"   # yellow
        else:
            return "#2e7d32"   # green

    def _kpi2_color(val: float) -> str:
        if val <= 1.00:
            return "#2e7d32"   # green
        elif val <= 1.25:
            return "#f9a825"   # yellow
        else:
            return "#d32f2f"   # red

    def _colored_metric(label: str, value: str, color: str) -> None:
        st.markdown(f"""
        <div style="margin:0.3rem 0">
          <p style="color:#999;font-size:0.8rem;margin:0 0 2px 0">{label}</p>
          <p style="color:{color};font-size:2.8rem;font-weight:600;margin:0">{value}</p>
        </div>
        """, unsafe_allow_html=True)

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Planned (h)", planned_h, delta=f"braki: {planned_missing}")
        st.metric("Allocated (h)", alloc_h)
        st.metric("Used until yesterday (h)", used_h)
        if ratio_total is not None:
            st.metric("Alloc / Planned", f"{ratio_total}%")
        if not date_filter_active and summary["time_progress_avg"]:
            st.metric("Ścieżka czasu (avg)", f"{summary['time_progress_avg']}%")
    with col2:
        _colored_metric(
            "KPI 1: Terminowość realizacji (BOM)",
            f"{kpi1_val} &nbsp;<span style='font-size:0.5em;color:#888'>"
            f"({len(kpi1_scores)}/{len(due_past)} ocenionych)</span>",
            _kpi1_color(kpi1_val),
        )
    with col3:
        _colored_metric("KPI 2: Used / Plan (compl. BOM)", str(kpi2), _kpi2_color(kpi2))

    st.caption(
        f"Debug planned effort: customFields zaczytane dla {summary['customfields_seen']} core items; "
        f"planned effort znaleziono w {summary['planned_seen']}."
    )
    st.subheader("Core Items (global)")
    if st.session_state.get("task_filter") == "\u2622 BOM":
        display_df = bom_df.copy()
    else:
        display_df = pd.concat([task_df, bom_df], ignore_index=True) if (not task_df.empty or not bom_df.empty) else pd.DataFrame()
    if st.session_state.get("status_filter") == "COMPLETED" and not display_df.empty and "completed" in display_df:
        display_df = display_df[display_df["completed"] == True]
    if date_filter_active and not display_df.empty:
        display_df = _filter_by_due(display_df)
    render_df(
        display_df,
        core_cols,
        column_config={
            "permalink": st.column_config.LinkColumn("Link", display_text="otwórz"),
            **{
                f"solidworks_pdf_{idx + 1}": st.column_config.LinkColumn(
                    f"SPEC {idx + 1}", display_text=f"otwórz SPEC {idx + 1}"
                )
                for idx in range(spec_col_count)
            },
        },
    )

    with st.expander("Nadpisz daty completed"):
        overrides = load_completed_overrides()
        if overrides:
            st.caption(f"Aktywne nadpisania: {len(overrides)}")
        all_items = pd.concat([task_df, bom_df], ignore_index=True) if (not task_df.empty or not bom_df.empty) else pd.DataFrame()
        if not all_items.empty:
            item_options = {row["id"]: f"[{row.get('project', '')}] {row['title']}" for _, row in all_items.iterrows()}
            selected_id = st.selectbox("Wybierz taska", options=list(item_options.keys()), format_func=lambda tid: item_options[tid])
            current_override = overrides.get(selected_id)
            if current_override:
                st.info(f"Obecne nadpisanie: {current_override.isoformat()}")
            new_date = st.date_input("Nowa data completed", value=current_override or date.today(), key="override_date")
            col_a, col_b = st.columns(2)
            with col_a:
                if st.button("Zapisz nadpisanie"):
                    overrides[selected_id] = new_date
                    save_completed_overrides(overrides)
                    st.rerun()
            with col_b:
                if current_override and st.button("Usuń nadpisanie"):
                    del overrides[selected_id]
                    save_completed_overrides(overrides)
                    st.rerun()

    with st.expander("Log (debug)"):
        _init_logs()
        _init_perf_metrics()
        st.write("Cache metrics:")
        st.json(
            {
                "wrike_comments_cache_hit": st.session_state["perf_metrics"].get("wrike_comments_cache_hit", 0),
                "wrike_comments_cache_miss": st.session_state["perf_metrics"].get("wrike_comments_cache_miss", 0),
                "kpi_aggregate_cache_hit": st.session_state["perf_metrics"].get("kpi_aggregate_cache_hit", 0),
                "kpi_aggregate_cache_miss": st.session_state["perf_metrics"].get("kpi_aggregate_cache_miss", 0),
                "stage_times_sec": st.session_state["perf_metrics"].get("stage_times", {}),
            }
        )
        st.write("\n".join(st.session_state["logs"]))

    st.subheader("Legenda ikon — Core Items")
    st.markdown(
        "- ✅ **Completed:** zadanie jest ukończone. **Due today/past:** ukończone "
        "w terminie lub przed terminem.\n"
        "- 🔴 **Completed:** zadanie nie jest ukończone. **Due today/past:** termin "
        "jeszcze nie minął lub brakuje daty terminu.\n"
        "- ⚠️ **Due today/past:** termin minął, a zadanie jest nadal otwarte, albo "
        "zostało ukończone po terminie."
    )

    st.subheader("Jak liczone są KPI i wskaźniki")
    st.markdown(
        "- **KPI 1 — terminowość BOM:** średnia ocen ukończonych pozycji BOM, których "
        "termin już minął. Ocena wynosi 1 do końca planowanego czasu (od startu do terminu), "
        "potem liniowo spada zgodnie z tolerancją X ustawioną w panelu i osiąga 0 po jej "
        "przekroczeniu. Otwarte pozycje oraz pozycje bez wymaganych dat są pomijane.\n"
        "- **KPI 2 — Used / Plan:** suma „Used until yesterday” podzielona przez sumę planu "
        "dla ukończonych pozycji BOM. To ważony wskaźnik: większy plan ma proporcjonalnie "
        "większy wpływ. „Used” pochodzi z dziennych przydziałów Wrike do wczoraj, nie z "
        "potwierdzonych wpisów czasu pracy.\n"
        "- **Alloc / Planned:** suma przydzielonych godzin (Allocated) podzielona przez sumę "
        "znanych godzin planu dla Core Items i BOM. Obejmuje pozycje niezależnie od statusu; "
        "przy aktywnym filtrze dat uwzględnia wybrany zakres terminów.\n"
        "- **Ścieżka czasu (avg):** średnia liczona jako upływ czasu od startu do ukończenia "
        "(albo do dziś, jeśli brak daty ukończenia) podzielony przez kalendarzowy czas od "
        "startu do terminu. Może przekroczyć 100%, jeśli minął termin; pokazuje się tylko "
        "bez aktywnego filtra zakresu dat."
    )

if __name__ == "__main__":
    main()
