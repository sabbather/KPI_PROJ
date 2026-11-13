#!/usr/bin/env python3
"""
Streamlit dashboard for monitoring Wrike KPI's for Core Projects and Core Tasks.

Environment (.env):
  WRIKE_API_KEY=...
  WRIKE_BASE_URL=https://app-eu.wrike.com/api/v4         # optional
  WRIKE_CLIENT_PROJECTS_FOLDER_ID=<folder with client projects>
  WRIKE_CORE_TASK_TYPE_ID=IEAGWGLXPIAHEHEZ               # optional override
  WRIKE_BOM_TASK_TYPE_ID=IEAGWGLXPIAIOPLM               # optional override
  WRIKE_REV_TASK_TYPE_ID=IEAGWGLXPIAJO4T2               # optional override
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
from html import escape
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple
import re
from pathlib import Path
from urllib.parse import quote

import altair as alt
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st
from dotenv import load_dotenv

load_dotenv()


# ---- Version ---------------------------------------------------------------
VERSION_FILE = Path(__file__).resolve().parent / "VERSION"
DEFAULT_APP_VERSION = "1.0.0"


def get_app_version() -> str:
    """Return the app version from APP_VERSION, VERSION file, or fallback."""
    env_version = os.getenv("APP_VERSION", "").strip()
    if env_version:
        return env_version.removeprefix("v")
    try:
        file_version = VERSION_FILE.read_text(encoding="utf-8").strip()
    except OSError:
        file_version = ""
    return (file_version or DEFAULT_APP_VERSION).removeprefix("v")


def render_version_badge() -> None:
    """Render a small version pill, aligned to the right."""
    version = escape(get_app_version())
    st.markdown(
        f"""
        <div style="display:flex; justify-content:flex-end; padding-top:0.9rem;">
            <span title="Wersja aplikacji" style="
                display:inline-block;
                padding:0.15rem 0.6rem;
                border:1px solid rgba(128,128,128,0.45);
                border-radius:999px;
                background:rgba(128,128,128,0.12);
                font-size:0.78rem;
                font-weight:600;
                letter-spacing:0.02em;
                white-space:nowrap;
            ">v{version}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ---- Config (overridable in sidebar) ---------------------------------------
DEFAULT_BASE_URL = os.getenv("WRIKE_BASE_URL", "https://app-eu.wrike.com/api/v4")
DEFAULT_API_KEY = os.getenv("WRIKE_API_KEY")
DEFAULT_CLIENT_FOLDER = os.getenv("WRIKE_CLIENT_PROJECTS_FOLDER_ID", "")
DEFAULT_CORE_TASK_TYPE = os.getenv("WRIKE_CORE_TASK_TYPE_ID", "IEAGWGLXPIAHEHEZ")
DEFAULT_BOM_TASK_TYPE = os.getenv("WRIKE_BOM_TASK_TYPE_ID", "")
DEFAULT_REV_TASK_TYPE = os.getenv("WRIKE_REV_TASK_TYPE_ID", "IEAGWGLXPIAJO4T2")
DEFAULT_PLANNED_FIELD_ID = os.getenv("WRIKE_PLANNED_EFFORT_FIELD_ID", "IEAGWGLXJUALG3VY")
DEFAULT_COMPLETED_STATUS_ID = os.getenv("WRIKE_COMPLETED_STATUS_ID", "IEAGWGLXJMGYX4ND")
DEFAULT_INTERNAL_FOLDER = os.getenv("PROJEKTY_WEWNETRZNE", "")
DEFAULT_CONSTRUCTORS_FOLDER = os.getenv("WRIKE_CONSTRUCTORS_FOLDER_ID", "")
DEFAULT_IN_PROGRESS_STATUS_ID = os.getenv("WRIKE_PINNED_STATUS_ID", "IEAGWGLXJMGS5BX4")
DEFAULT_TODO_STATUS_ID = os.getenv("WRIKE_TODO_STATUS_ID", "IEAGWGLXJMAAAAAA")
DEFAULT_CONSTRUCTOR_COMPLETED_STATUS_ID = os.getenv(
    "WRIKE_CONSTRUCTOR_COMPLETED_STATUS_ID", "IEAGWGLXJMAAAAAB"
)

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
WRIKE_DATE_COMMENTS_FILE = Path(".cache") / "wrike_date_comments.json"
WRIKE_MILESTONE_COMMENTS_FILE = Path(".cache") / "wrike_milestone_comments.json"
HOLIDAYS_FILE = Path(".cache") / "holidays.json"
MANUAL_OFF_DAYS_FILE = Path(".cache") / "manual_off_days.json"
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
        hours_per_day = item.get("hours_per_day")
        if hours_per_day is None and cleaned_days:
            values = [round(float(value), 2) for value in cleaned_days.values()]
            hours_per_day = max(set(values), key=values.count)
        title = str(item.get("project") or item.get("label") or "")
        cleaned.append(
            {
                "id": str(item.get("id") or uuid.uuid4().hex),
                "kind": "external" if item.get("kind") == "external" else "project",
                "project_id": item.get("project_id"),
                "project": str(item.get("project") or ""),
                "label": str(item.get("label") or item.get("project") or ""),
                "project_number": (title.strip().split() or [""])[0],
                "constructor": str(item.get("constructor") or "").upper(),
                "start_date": str(item.get("start_date") or min(cleaned_days)),
                "end_date": str(item.get("end_date") or max(cleaned_days)),
                "mode": str(item.get("mode") or "custom"),
                "hours_per_day": hours_per_day,
                "requested_hours": requested_hours,
                "days": cleaned_days,
                "working_days": (
                    int(item["working_days"])
                    if item.get("working_days") is not None
                    else len(cleaned_days)
                ),
                "created_at": item.get("created_at"),
                "wrike_task_id": item.get("wrike_task_id"),
                "wrike_folder_id": item.get("wrike_folder_id"),
                "wrike_synced_start": item.get("wrike_synced_start"),
                "wrike_synced_end": item.get("wrike_synced_end"),
            }
        )
    return cleaned


def save_constructor_assignments(assignments: List[Dict[str, Any]]) -> None:
    _write_json_file(CONSTRUCTOR_ASSIGNMENTS_FILE, {"assignments": assignments})


def fetch_polish_holidays(years: Iterable[int]) -> Dict[str, str]:
    """Fetch Polish public holidays from Nager.Date. Returns {date_iso: name}."""
    out: Dict[str, str] = {}
    for year in years:
        url = f"https://date.nager.at/api/v3/PublicHolidays/{year}/PL"
        try:
            resp = requests.get(url, timeout=20)
            resp.raise_for_status()
            for item in resp.json():
                out[item["date"]] = str(
                    item.get("localName") or item.get("name") or ""
                )
        except Exception as exc:  # noqa: BLE001
            log(f"Nie udało się pobrać świąt dla {year}: {exc}")
    return out


def load_holidays() -> Dict[str, str]:
    payload = _read_json_file(HOLIDAYS_FILE, {})
    if not isinstance(payload, dict):
        return {}
    holidays = payload.get("holidays")
    return holidays if isinstance(holidays, dict) else {}


def ensure_holidays() -> Dict[str, str]:
    """Return cached Polish holidays, fetching this + next year when stale/empty.
    Append-only: previously stored dates are never removed."""
    payload = _read_json_file(HOLIDAYS_FILE, {})
    if not isinstance(payload, dict):
        payload = {}
    existing = payload.get("holidays")
    existing = existing if isinstance(existing, dict) else {}
    fetched_at = str(payload.get("fetched_at") or "")
    stale = True
    if fetched_at and existing:
        try:
            last = date.fromisoformat(fetched_at)
            stale = (date.today() - last).days > 30
        except (TypeError, ValueError):
            stale = True
    if stale:
        this_year = date.today().year
        fetched = fetch_polish_holidays([this_year, this_year + 1])
        merged = dict(existing)
        merged.update(fetched)  # append-only: never remove existing dates
        _write_json_file(
            HOLIDAYS_FILE,
            {"fetched_at": date.today().isoformat(), "holidays": merged},
        )
        return merged
    return existing


def build_off_days(
    holidays: Set[date],
    vacations: Dict[str, Set[date]],
    constructors: Iterable[str],
) -> Dict[str, Set[date]]:
    return {
        str(constructor).upper(): set(holidays)
        | set(vacations.get(str(constructor).upper(), set()))
        for constructor in constructors
    }


def load_manual_off_days() -> Set[date]:
    payload = _read_json_file(MANUAL_OFF_DAYS_FILE, [])
    if not isinstance(payload, list):
        return set()
    result: Set[date] = set()
    for item in payload:
        try:
            result.add(date.fromisoformat(str(item)))
        except (TypeError, ValueError):
            continue
    return result


def save_manual_off_days(days: Set[date]) -> None:
    _write_json_file(
        MANUAL_OFF_DAYS_FILE, sorted(d.isoformat() for d in days)
    )


def _refresh_off_days() -> None:
    """Recompute the merged global off-days (Nager holidays + manual company days)."""
    nager_names = st.session_state.get("nager_holiday_names", {})
    manual = st.session_state.get("manual_off_days", set())
    holidays = set(nager_names.keys()) | set(manual)
    names = dict(nager_names)
    for day_value in manual:
        names.setdefault(day_value, "Dzień wolny")
    st.session_state["holidays"] = holidays
    st.session_state["holiday_names"] = names


def _init_resource_state() -> None:
    if "constructors" not in st.session_state:
        st.session_state["constructors"] = load_constructors()
    if "constructor_assignments" not in st.session_state:
        st.session_state["constructor_assignments"] = load_constructor_assignments()
    if "holidays" not in st.session_state:
        holiday_dict = ensure_holidays()
        nager_names: Dict[date, str] = {}
        for day_str, name in holiday_dict.items():
            try:
                day_value = date.fromisoformat(str(day_str))
            except (TypeError, ValueError):
                continue
            nager_names[day_value] = str(name)
        st.session_state["nager_holiday_names"] = nager_names
        st.session_state["manual_off_days"] = load_manual_off_days()
        _refresh_off_days()
    if "vacations" not in st.session_state:
        st.session_state["vacations"] = load_vacations(
            DEFAULT_BASE_URL,
            DEFAULT_API_KEY,
            st.session_state["constructors"],
        )
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


def _wrike_cache_delete(bucket: str, key: str) -> None:
    cache_name = "wrike_comments_cache" if bucket == "comments" else "wrike_data_cache"
    cache_dict = st.session_state.get(cache_name, {})
    if key not in cache_dict:
        return
    cache_dict.pop(key, None)
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


def effort_minutes_total(task: Dict[str, Any]) -> int:
    """Sum every daily allocation, including today and future dates."""
    effort = task.get("effortAllocation") or {}
    total = 0
    for responsible in effort.get("responsibleAllocation", []) or []:
        for daily in responsible.get("dailyAllocation", []) or []:
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
    as_of: Optional[date] = None,
) -> Optional[float]:
    """Score a BOM item's delivery time from 0 to 1; None means insufficient dates.

    Open items past their due date are scored as if they were closed on `as_of`
    (today by default), so their score keeps dropping as the delay grows.
    """
    start_day = _to_date_only(start_date)
    due_day = _to_date_only(due_date)
    if start_day is None or due_day is None:
        return None

    planned_days = (due_day - start_day).days
    if planned_days <= 0:
        return None

    if completed:
        effective_day = _to_date_only(completed_date)
    else:
        reference_day = _to_date_only(as_of) or date.today()
        if due_day >= reference_day:
            return None
        effective_day = reference_day

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
        planned = row.get("daily_planned_hours")
        if planned is None or pd.isna(planned):
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


def allocate_working_days(
    start: date,
    n_days: int,
    hours_per_day: float,
    off_days: Set[date],
    available_fn: Optional[Callable[[date], float]] = None,
) -> Dict[str, float]:
    """Allocate `hours_per_day` to `n_days` working days from `start`, skipping
    weekends, the given off-days (holidays/vacations) and days where the
    constructor is already busy (`available_fn(day) < hours_per_day`)."""
    days: Dict[str, float] = {}
    day = start
    remaining = max(0, int(n_days))
    hpd = float(hours_per_day)
    guard = 0
    while remaining > 0 and guard < 100000:
        guard += 1
        if day.weekday() < 5 and day not in off_days:
            if available_fn is None or available_fn(day) >= hpd - 1e-9:
                days[day.isoformat()] = round(hpd, 2)
                remaining -= 1
        day += timedelta(days=1)
    return days


def expand_assignment_days(
    assignments: List[Dict[str, Any]],
    start: Optional[date] = None,
    end: Optional[date] = None,
    off_days: Optional[Dict[str, Set[date]]] = None,
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
        constructor = str(assignment.get("constructor") or "").upper()
        constructor_off = off_days.get(constructor, set()) if off_days else set()
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
            if constructor_off and day_value in constructor_off:
                continue
            rows.append(
                {
                    "assignment_id": assignment.get("id"),
                    "kind": "external" if assignment.get("kind") == "external" else "project",
                    "project_id": assignment.get("project_id"),
                    "project": assignment.get("project") or "",
                    "label": assignment.get("label") or assignment.get("project") or "",
                    "constructor": constructor,
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


def fetch_project_core_date_range(
    base_url: str,
    api_key: str,
    project_id: str,
    core_task_type: str,
    bom_task_type: str,
) -> Optional[Tuple[date, date]]:
    """Return (min start, max due) across the project's core/BOM tasks."""
    if not project_id:
        return None
    try:
        tasks = fetch_tasks_for_project(base_url, api_key, project_id)
    except Exception:  # noqa: BLE001
        return None
    allowed_types = {core_task_type, bom_task_type} - {""}
    starts: List[date] = []
    dues: List[date] = []
    for task in tasks:
        ctype = task.get("customItemTypeId") or task.get("entityTypeId")
        if ctype not in allowed_types:
            continue
        start = iso_to_date(
            task.get("start")
            or (task.get("dates") or {}).get("start")
            or (task.get("project") or {}).get("startDate")
        )
        due = iso_to_date(
            task.get("due")
            or (task.get("dates") or {}).get("due")
            or (task.get("project") or {}).get("endDate")
        )
        if start is not None:
            starts.append(start)
        if due is not None:
            dues.append(due)
    if not starts or not dues:
        return None
    return min(starts), max(dues)


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


def fit_assignment_to_hours(
    assignment: Dict[str, Any],
    assignments: List[Dict[str, Any]],
    target_hours: float,
    effective_daily_hours: float,
    off_days: Set[date],
    project_end: Optional[date] = None,
) -> Tuple[Optional[Dict[str, Any]], float]:
    """Rebuild `assignment` so its total equals `target_hours`, spreading the
    hours forward from its start date at its daily rate. Skips weekends,
    off-days and days where the constructor is already busy. Returns
    (updated_assignment, unfilled_hours)."""
    target = max(0.0, float(target_hours))
    if target <= 0.001:
        return None, target
    try:
        start = date.fromisoformat(str(assignment.get("start_date")))
    except (TypeError, ValueError):
        return None, target
    hpd = float(assignment.get("hours_per_day") or effective_daily_hours or 0.0)
    if hpd <= 0:
        hpd = float(effective_daily_hours or 0.0)
    if hpd <= 0:
        return None, target
    constructor = str(assignment.get("constructor") or "").upper()
    exclude_id = assignment.get("id")
    days: Dict[str, float] = {}
    remaining = target
    day = start
    guard = 0
    while remaining > 0.001 and guard < 20000:
        guard += 1
        if project_end is not None and day > project_end:
            break
        if day.weekday() < 5 and day not in off_days:
            available = max(
                0.0,
                float(effective_daily_hours)
                - _assignment_day_hours(
                    assignments, constructor, day, exclude_id=exclude_id
                ),
            )
            if available > 0.001:
                hours = min(hpd, remaining, available)
                if hours > 0.001:
                    days[day.isoformat()] = round(hours, 2)
                    remaining = round(remaining - hours, 2)
        day += timedelta(days=1)
    if not days:
        return None, round(remaining, 2)
    updated = dict(assignment)
    updated["days"] = days
    updated["end_date"] = max(days)
    updated["working_days"] = len(days)
    if updated.get("hours_per_day") is None:
        updated["hours_per_day"] = hpd
    if updated.get("requested_hours") is not None:
        updated["requested_hours"] = round(target - remaining, 2)
    return updated, round(remaining, 2)


def _assignment_end_date(assignment: Dict[str, Any]) -> Optional[date]:
    try:
        return date.fromisoformat(str(assignment.get("end_date") or ""))
    except (TypeError, ValueError):
        return None


def _assignment_task_status_id(
    assignment: Dict[str, Any],
    todo_status_id: str,
    in_progress_status_id: str,
    completed_status_id: str,
) -> str:
    """Return the Wrike status for a task based on its dates: past → Completed,
    future → ToDo, current → InProgress."""
    today = date.today()
    end = _assignment_end_date(assignment)
    try:
        start = date.fromisoformat(str(assignment.get("start_date") or ""))
    except (TypeError, ValueError):
        start = None
    if end is not None and end < today:
        return completed_status_id
    if start is not None and start > today:
        return todo_status_id
    return in_progress_status_id


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
    for c in [
        "allocated_hours",
        "revisions_allocated_hours",
        "planned_hours",
        "alloc_vs_plan_pct",
        "time_progress_pct",
    ]:
        if c in df_local:
            df_local[c] = pd.to_numeric(df_local[c], errors="coerce").round(0)
    for numeric_col in [
        "used_hours_until_yesterday",
        "allocated_hours",
        "revisions_allocated_hours",
        "planned_hours",
    ]:
        if numeric_col in df_local:
            df_local[numeric_col] = pd.to_numeric(df_local[numeric_col], errors="coerce").round(0)
    ordered_cols: list[str] = []
    label_config: Dict[str, Any] = {}
    final_config: Dict[str, Any] = dict(column_config or {})

    try:
        fmt = {
            c: "{:.0f}"
            for c in [
                "allocated_hours",
                "revisions_allocated_hours",
                "planned_hours",
                "alloc_vs_plan_pct",
                "time_progress_pct",
            ]
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


def _api_write(
    method: str,
    base_url: str,
    api_key: str,
    path: str,
    payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if not api_key:
        raise RuntimeError("Brak WRIKE_API_KEY – uzupełnij .env lub sidebar.")
    headers = {"Authorization": f"bearer {api_key}", "Content-Type": "application/json"}
    url = f"{base_url.rstrip('/')}/{path.lstrip('/')}"
    resp = requests.request(method, url, headers=headers, json=payload or {})
    log(
        f"{method} {url} payload={json.dumps(payload, ensure_ascii=False)} "
        f"status={resp.status_code}"
    )
    if resp.status_code == 403:
        raise RuntimeError("API zwróciło 403 (sprawdź token i uprawnienia).")
    if resp.status_code == 400:
        raise RuntimeError(f"API zwróciło 400 (parametry?): {resp.text}")
    resp.raise_for_status()
    return resp.json()


def api_post(
    base_url: str, api_key: str, path: str, payload: Dict[str, Any]
) -> Dict[str, Any]:
    return _api_write("POST", base_url, api_key, path, payload)


def api_put(
    base_url: str, api_key: str, path: str, payload: Dict[str, Any]
) -> Dict[str, Any]:
    return _api_write("PUT", base_url, api_key, path, payload)


def api_delete(base_url: str, api_key: str, path: str) -> Dict[str, Any]:
    return _api_write("DELETE", base_url, api_key, path)


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
def fetch_internal_projects(
    base_url: str, api_key: str, internal_folder_id: str
) -> List[Dict[str, Any]]:
    if not internal_folder_id:
        return []
    cache_key = f"internal_projects|{base_url.rstrip('/')}|{internal_folder_id}"
    cached = _wrike_cache_get("data", cache_key, WRIKE_DATA_CACHE_TTL_SEC)
    if isinstance(cached, list):
        return cached
    params = {"project": "true", "descendants": "false"}
    data = api_get(
        base_url, api_key, f"folders/{internal_folder_id}/folders", params=params
    )
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


@st.cache_data(ttl=WRIKE_DATA_CACHE_TTL_SEC, show_spinner=False)
def fetch_constructor_folder_map(
    base_url: str,
    api_key: str,
    constructors_folder_id: str,
    constructors: Tuple[str, ...],
) -> Dict[str, str]:
    """Map constructor initials (uppercased) to their folder id under KONSTRUKTORZY."""
    if not constructors_folder_id:
        return {}
    cache_key = (
        f"constructor_folders|{base_url.rstrip('/')}|{constructors_folder_id}"
        f"|{','.join(sorted(constructors))}"
    )
    cached = _wrike_cache_get("data", cache_key, WRIKE_DATA_CACHE_TTL_SEC)
    if isinstance(cached, dict):
        return cached
    wanted = {str(c).strip().upper() for c in constructors if str(c).strip()}
    data = api_get(base_url, api_key, f"folders/{constructors_folder_id}/folders")
    out: Dict[str, str] = {}
    for folder in data.get("data", []):
        title = str(folder.get("title") or "").strip().upper()
        if title in wanted:
            out[title] = folder.get("id")
    _wrike_cache_set("data", cache_key, out)
    return out


@st.cache_data(ttl=WRIKE_DATA_CACHE_TTL_SEC, show_spinner=False)
def fetch_folder_tasks(
    base_url: str, api_key: str, folder_id: str
) -> List[Dict[str, Any]]:
    cache_key = f"folder_tasks|{base_url.rstrip('/')}|{folder_id}"
    cached = _wrike_cache_get("data", cache_key, WRIKE_DATA_CACHE_TTL_SEC)
    if isinstance(cached, list):
        return cached
    params: Dict[str, Any] = {"pageSize": 1000}
    tasks: List[Dict[str, Any]] = []
    next_token: Optional[str] = None
    while True:
        page_params = dict(params)
        if next_token:
            page_params["nextPageToken"] = next_token
        data = api_get(
            base_url, api_key, f"folders/{folder_id}/tasks", params=page_params
        )
        tasks.extend(data.get("data", []))
        nt = data.get("nextPageToken")
        next_token = nt if nt else None
        if not next_token or len(data.get("data", [])) < params["pageSize"]:
            break
    _wrike_cache_set("data", cache_key, tasks)
    return tasks


@st.cache_data(ttl=WRIKE_DATA_CACHE_TTL_SEC, show_spinner=False)
def fetch_folder_tasks_recursive(
    base_url: str, api_key: str, folder_id: str
) -> List[Dict[str, Any]]:
    """Fetch tasks from a folder and all its descendant folders."""
    cache_key = f"folder_tasks_recursive|{base_url.rstrip('/')}|{folder_id}"
    cached = _wrike_cache_get("data", cache_key, WRIKE_DATA_CACHE_TTL_SEC)
    if isinstance(cached, list):
        return cached
    params: Dict[str, Any] = {"pageSize": 1000, "descendants": "true"}
    tasks: List[Dict[str, Any]] = []
    next_token: Optional[str] = None
    while True:
        page_params = dict(params)
        if next_token:
            page_params["nextPageToken"] = next_token
        data = api_get(
            base_url, api_key, f"folders/{folder_id}/tasks", params=page_params
        )
        tasks.extend(data.get("data", []))
        nt = data.get("nextPageToken")
        next_token = nt if nt else None
        if not next_token or len(data.get("data", [])) < params["pageSize"]:
            break
    _wrike_cache_set("data", cache_key, tasks)
    return tasks


@st.cache_data(ttl=300, show_spinner=False)
def fetch_project_milestone(
    base_url: str, api_key: str, project_id: str
) -> Optional[Dict[str, Any]]:
    """Return the '[Milestone] Mechanical Design Ready' task in a project (or None)."""
    if not project_id:
        return None
    params: Dict[str, Any] = {"descendants": "true", "pageSize": 1000}
    next_token: Optional[str] = None
    try:
        while True:
            page_params = dict(params)
            if next_token:
                page_params["nextPageToken"] = next_token
            data = api_get(
                base_url, api_key, f"folders/{project_id}/tasks", params=page_params
            )
            for task in data.get("data", []):
                title = str(task.get("title") or "")
                if "Mechanical Design Ready" in title:
                    return task
            nt = data.get("nextPageToken")
            next_token = nt if nt else None
            if not next_token or len(data.get("data", [])) < params["pageSize"]:
                break
    except Exception:  # noqa: BLE001
        return None
    return None


def fetch_project_milestone_due(
    base_url: str, api_key: str, project_id: str
) -> Optional[date]:
    milestone = fetch_project_milestone(base_url, api_key, project_id)
    if not milestone:
        return None
    return _task_date_part((milestone.get("dates") or {}).get("due"))



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
    fetch_internal_projects.clear()
    fetch_constructor_folder_map.clear()
    fetch_folder_tasks.clear()
    fetch_folder_tasks_recursive.clear()
    fetch_task.clear()
    fetch_contacts.clear()
    fetch_user_schedule_exclusions.clear()
    fetch_project_milestone.clear()
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


def _task_item_type(task: Dict[str, Any]) -> str:
    return str(task.get("customItemTypeId") or task.get("entityTypeId") or "")


def aggregate_subtree_values(
    target_id: str,
    tasks_by_id: Dict[str, Dict[str, Any]],
    children: Dict[str, List[str]],
    raw_planned_by_task: Dict[str, Optional[float]],
    alloc_by_task: Dict[str, int],
    used_by_task: Dict[str, int],
    rev_task_type: str,
    used_total_by_task: Optional[Dict[str, int]] = None,
) -> Dict[str, Any]:
    """Sum plan/alloc/used for a task and all its descendants.

    REV Task branches are excluded from the parent totals and reported
    separately as `revisions_alloc_minutes`.
    """
    used_total_by_task = used_total_by_task or used_by_task
    total_planned = 0.0
    planned_found = False
    total_alloc = 0
    total_used = 0
    total_used_all = 0
    revisions_alloc = 0
    visited: Set[str] = set()

    def is_revision(task_id: str) -> bool:
        if not rev_task_type:
            return False
        task = tasks_by_id.get(task_id) or {}
        return _task_item_type(task) == rev_task_type

    def sum_revision_alloc(task_id: str, seen: Set[str]) -> int:
        if task_id in seen:
            return 0
        seen.add(task_id)
        total = int(alloc_by_task.get(task_id, 0))
        for child_id in children.get(task_id, []) or []:
            total += sum_revision_alloc(child_id, seen)
        return total

    def walk(task_id: str, is_root: bool = False) -> None:
        nonlocal total_planned, planned_found, total_alloc, total_used, total_used_all, revisions_alloc
        if task_id in visited:
            return
        visited.add(task_id)
        if not is_root and is_revision(task_id):
            revisions_alloc += sum_revision_alloc(task_id, set())
            return
        task = tasks_by_id.get(task_id)
        if not task:
            return
        raw_planned = raw_planned_by_task.get(task_id)
        if raw_planned is not None:
            total_planned += float(raw_planned)
            planned_found = True
        total_alloc += int(alloc_by_task.get(task_id, 0))
        total_used += int(used_by_task.get(task_id, 0))
        total_used_all += int(used_total_by_task.get(task_id, 0))
        for child_id in children.get(task_id, []) or []:
            walk(child_id)

    walk(target_id, is_root=True)
    return {
        "planned_hours": total_planned if planned_found else None,
        "allocated_minutes": total_alloc,
        "used_minutes": total_used,
        "used_total_minutes": total_used_all,
        "revisions_allocated_minutes": revisions_alloc,
    }


def compute_completion_adjustments(df: pd.DataFrame) -> Tuple[int, int]:
    """Return (A, B) for completed CORE/BOM rows.

    A = -(planned - allocated) when a completed task was allocated less than
    planned (savings), always <= 0.
    B = used - allocated when a completed task used more than allocated,
    always >= 0.
    Savings and overruns are summed separately and never netted.
    """
    if df is None or df.empty:
        return 0, 0
    completed = df
    if "completed" in completed.columns:
        completed = completed[completed["completed"] == True]
    if completed.empty:
        return 0, 0
    planned = pd.to_numeric(
        completed.get("planned_hours", pd.Series(dtype=float)),
        errors="coerce",
    ).fillna(0.0)
    alloc = pd.to_numeric(
        completed.get("allocated_hours", pd.Series(dtype=float)),
        errors="coerce",
    ).fillna(0.0)
    used = pd.to_numeric(
        completed.get("used_hours_until_yesterday", pd.Series(dtype=float)),
        errors="coerce",
    ).fillna(0.0)
    saved_hours = float((planned - alloc).clip(lower=0.0).sum())
    overrun_hours = float((used - alloc).clip(lower=0.0).sum())
    return -int(round(saved_hours)), int(round(overrun_hours))


def compute_unfinished_row_overrun(
    rows_by_id: Dict[str, Dict[str, Any]],
    children_by_parent: Dict[Optional[str], List[str]],
    parent_id: Optional[str] = None,
) -> int:
    """Overrun on unfinished CORE/BOM rows: sum(max(0, Used - Plan)).

    Rows are checked from the top down. If a parent row already overruns, its
    children are not counted again; if it does not overrun, its child rows are
    inspected separately. Rows without a positive plan are skipped.
    """
    total = 0
    for row_id in children_by_parent.get(parent_id, []):
        row = rows_by_id.get(row_id)
        if not row:
            continue
        planned = float(row.get("planned_hours") or 0.0)
        used = float(row.get("used_hours_until_yesterday") or 0.0)
        if (
            planned > 0
            and not bool(row.get("completed"))
            and used > planned
        ):
            total += int(round(used - planned))
        else:
            total += compute_unfinished_row_overrun(
                rows_by_id, children_by_parent, row_id
            )
    return total


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
    rev_task_type: str = "",
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    tasks_by_id, children = build_indexes(tasks)

    today = date.today()
    core_task_rows = []
    bom_task_rows = []
    completed_overrides = load_completed_overrides()

    # Pre-compute allocated minutes per task
    alloc_by_task = {tid: allocated_minutes(task) for tid, task in tasks_by_id.items()}
    cutoff_date = date.today() - timedelta(days=1)
    used_minutes_by_task = {tid: effort_minutes_until(task, cutoff_date) for tid, task in tasks_by_id.items()}
    used_total_minutes_by_task = {tid: effort_minutes_total(task) for tid, task in tasks_by_id.items()}
    planned_seen = 0
    customfields_seen = 0

    # Pre-compute raw planned hours for all tasks
    raw_planned_by_task: Dict[str, Optional[float]] = {}
    for tid, task in tasks_by_id.items():
        raw_planned_by_task[tid] = extract_planned_hours(task, planned_field_id)

    def is_revision_task(task_id: str) -> bool:
        if not rev_task_type:
            return False
        task = tasks_by_id.get(task_id) or {}
        return _task_item_type(task) == rev_task_type

    @lru_cache(maxsize=None)
    def has_revision_ancestor(task_id: str) -> bool:
        if not rev_task_type:
            return False
        task = tasks_by_id.get(task_id) or {}
        for parent_id in task.get("superTaskIds") or []:
            parent = tasks_by_id.get(parent_id)
            if not parent:
                continue
            if _task_item_type(parent) == rev_task_type:
                return True
            if has_revision_ancestor(parent_id):
                return True
        return False

    def sum_subtree_alloc(task_id: str, seen: Set[str]) -> int:
        if task_id in seen:
            return 0
        seen.add(task_id)
        total = int(alloc_by_task.get(task_id, 0))
        for child_id in children.get(task_id, []) or []:
            total += sum_subtree_alloc(child_id, seen)
        return total

    global_revisions_alloc_minutes = 0
    for tid in tasks_by_id:
        if is_revision_task(tid) and not has_revision_ancestor(tid):
            global_revisions_alloc_minutes += sum_subtree_alloc(tid, set())



    # Cascading netting: net = max(0, raw - sum(net of non-REV children)).
    # Used only for the daily chart, so parent/child plans are not double-counted.
    @lru_cache(maxsize=None)
    def compute_netted_planned(tid: str) -> Optional[float]:
        raw = raw_planned_by_task.get(tid)
        if raw is None:
            return None
        child_sum = 0.0
        for child_id in children.get(tid, []):
            if is_revision_task(child_id):
                continue
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

    @lru_cache(maxsize=None)
    def has_core_bom_ancestor(task_id: str) -> bool:
        task = tasks_by_id.get(task_id) or {}
        for parent_id in task.get("superTaskIds") or []:
            parent = tasks_by_id.get(parent_id)
            if not parent:
                continue
            if _task_item_type(parent) in {core_task_type, bom_task_type}:
                return True
            if has_core_bom_ancestor(parent_id):
                return True
        return False

    @lru_cache(maxsize=None)
    def nearest_core_bom_ancestor(task_id: str) -> Optional[str]:
        task = tasks_by_id.get(task_id) or {}
        for parent_id in task.get("superTaskIds") or []:
            parent = tasks_by_id.get(parent_id)
            if not parent:
                continue
            if _task_item_type(parent) in {core_task_type, bom_task_type}:
                return parent_id
            found = nearest_core_bom_ancestor(parent_id)
            if found:
                return found
        return None

    def minutes_to_hours(minutes: int) -> int:
        return int(round(minutes / 60))

    for tid, task in tasks_by_id.items():
        ctype = _task_item_type(task)
        raw_planned = raw_planned_by_task.get(tid)
        if task.get("customFields"):
            customfields_seen += 1
        if raw_planned is not None:
            planned_seen += 1
        daily_planned_hours = netted_planned_by_task.get(tid)
        if daily_planned_hours is not None:
            daily_planned_hours = int(round(daily_planned_hours))
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

        subtree = aggregate_subtree_values(
            tid,
            tasks_by_id,
            children,
            raw_planned_by_task,
            alloc_by_task,
            used_minutes_by_task,
            rev_task_type,
            used_total_minutes_by_task,
        )
        planned_total = subtree["planned_hours"]
        planned_hours = (
            int(round(planned_total)) if planned_total is not None else None
        )

        base_row = {
            "id": tid,
            "title": task.get("title", "(brak tytułu)"),
            "project": project_title,
            "project_id": project_id,
            "permalink": task.get("permalink"),
            "planned_hours": planned_hours,
            "daily_planned_hours": daily_planned_hours,
            "allocated_hours": minutes_to_hours(subtree["allocated_minutes"]),
            "used_hours_until_yesterday": minutes_to_hours(subtree["used_minutes"]),
            "used_hours_total": minutes_to_hours(subtree["used_total_minutes"]),
            "revisions_allocated_hours": minutes_to_hours(
                subtree["revisions_allocated_minutes"]
            ),
            "is_top_level": not has_core_bom_ancestor(tid),
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
            core_task_rows.append(base_row)
        elif ctype == bom_task_type:
            base_row["title"] = "\u2622 " + base_row["title"]
            bom_task_rows.append(base_row)

    rows_by_id = {
        row["id"]: row for row in core_task_rows + bom_task_rows
    }
    children_by_parent: Dict[Optional[str], List[str]] = defaultdict(list)
    for row_id in rows_by_id:
        parent_id = nearest_core_bom_ancestor(row_id)
        if parent_id in rows_by_id:
            children_by_parent[parent_id].append(row_id)
        else:
            children_by_parent[None].append(row_id)
    unfinished_overrun_hours = compute_unfinished_row_overrun(
        rows_by_id, children_by_parent
    )

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

    # Global metrics use only top-level CORE/BOM rows for alloc/used, while the
    # global planned total uses the deduplicated (netted) plan across all rows.
    # This keeps the total from double-counting parents and their children.
    all_rows = core_items_df
    top_rows = core_items_df
    if not top_rows.empty and "is_top_level" in top_rows.columns:
        top_rows = top_rows[top_rows["is_top_level"] == True]
    planned_total = pd.to_numeric(
        all_rows.get("daily_planned_hours", pd.Series(dtype=float)),
        errors="coerce",
    ).fillna(0.0)
    alloc_total = pd.to_numeric(
        top_rows.get("allocated_hours", pd.Series(dtype=float)), errors="coerce"
    ).fillna(0.0)
    used_total = pd.to_numeric(
        top_rows.get("used_hours_until_yesterday", pd.Series(dtype=float)),
        errors="coerce",
    ).fillna(0.0)
    used_total_all = pd.to_numeric(
        top_rows.get("used_hours_total", pd.Series(dtype=float)),
        errors="coerce",
    ).fillna(0.0)
    planned_missing_total = pd.to_numeric(
        all_rows.get("planned_hours", pd.Series(dtype=float)), errors="coerce"
    )

    allocated_hours = int(round(alloc_total.sum()))
    revisions_hours = minutes_to_hours(global_revisions_alloc_minutes)
    allocated_with_revisions = allocated_hours + revisions_hours
    revisions_share_pct = (
        round(revisions_hours / allocated_with_revisions * 100.0, 1)
        if allocated_with_revisions > 0
        else 0.0
    )
    completion_a, completion_b = compute_completion_adjustments(top_rows)
    time_progress_series = pd.concat(
        [
            task_df.get("time_progress_pct", pd.Series(dtype=float)),
            bom_df.get("time_progress_pct", pd.Series(dtype=float)),
        ],
        ignore_index=True,
    ).dropna()

    summary = {
        "allocated_hours": allocated_hours,
        "planned_hours": int(round(planned_total.sum())),
        "planned_missing": int(planned_missing_total.isna().sum()),
        "completed_due": completed,
        "due_total": due_total,
        "planned_seen": planned_seen,
        "customfields_seen": customfields_seen,
        "time_progress_avg": int(round(time_progress_series.mean()))
        if not time_progress_series.empty
        else 0,
        "used_until_yesterday": int(round(used_total.sum())),
        "used_total_hours": int(round(used_total_all.sum())),
        "revisions_allocated_hours": revisions_hours,
        "allocated_with_revisions_hours": allocated_with_revisions,
        "revisions_share_pct": revisions_share_pct,
        "completion_savings_hours": completion_a,
        "completion_overrun_hours": completion_b,
        "unfinished_overrun_hours": unfinished_overrun_hours,
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
        st.caption(f"Wersja aplikacji: v{get_app_version()}")
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


def _legacy_render_resource_planning_section(
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
def _assignment_hours_per_day(assignment: Dict[str, Any]) -> int:
    value = assignment.get("hours_per_day")
    try:
        return int(round(float(value)))
    except (TypeError, ValueError):
        return 0


def _assignment_task_title(assignment: Dict[str, Any]) -> str:
    number = assignment.get("project_number") or (
        str(assignment.get("project") or "").strip().split() or [""]
    )[0]
    return f"[PLAN] {number} [{_assignment_hours_per_day(assignment)}]"


def _assignment_task_dates(assignment: Dict[str, Any]) -> Dict[str, str]:
    return {
        "type": "Planned",
        "start": str(assignment.get("start_date") or ""),
        "due": str(assignment.get("end_date") or ""),
    }


def _task_date_part(value: Any) -> Optional[date]:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _combined_project_options(
    project_map: Dict[str, str], internal_projects: List[Dict[str, Any]]
) -> List[Dict[str, str]]:
    options: Dict[str, Dict[str, str]] = {}
    for project_id, title in project_map.items():
        options[project_id] = {
            "id": project_id,
            "title": title,
            "number": (str(title).strip().split() or [""])[0],
        }
    for project in internal_projects:
        title = str(project.get("title") or project.get("id") or "")
        options[project.get("id")] = {
            "id": project.get("id"),
            "title": title,
            "number": (title.strip().split() or [""])[0],
        }
    return sorted(options.values(), key=lambda o: (o["title"] or "").casefold())


@st.cache_data(ttl=300, show_spinner=False)
def fetch_task(
    base_url: str, api_key: str, task_id: str
) -> Optional[Dict[str, Any]]:
    try:
        data = api_get(base_url, api_key, f"tasks/{task_id}")
    except Exception:  # noqa: BLE001
        return None
    items = data.get("data", [])
    return items[0] if items else None


@st.cache_data(ttl=WRIKE_DATA_CACHE_TTL_SEC, show_spinner=False)
def fetch_contacts(base_url: str, api_key: str) -> List[Dict[str, Any]]:
    cache_key = f"contacts|{base_url.rstrip('/')}"
    cached = _wrike_cache_get("data", cache_key, WRIKE_DATA_CACHE_TTL_SEC)
    if isinstance(cached, list):
        return cached
    data = api_get(base_url, api_key, "contacts")
    out = [c for c in data.get("data", []) if c.get("type") == "Person"]
    _wrike_cache_set("data", cache_key, out)
    return out


def build_constructor_assignee_map(
    contacts: List[Dict[str, Any]],
) -> Dict[str, str]:
    """Map constructor initials (first letter of first+last name) to a Wrike user id."""
    mapping: Dict[str, str] = {}
    for contact in contacts:
        first = str(contact.get("firstName") or "").strip()
        last = str(contact.get("lastName") or "").strip()
        if not first or not last:
            continue
        initials = (first[0] + last[0]).upper()
        if initials and initials not in mapping:
            mapping[initials] = contact.get("id")
    return mapping


@st.cache_data(ttl=WRIKE_DATA_CACHE_TTL_SEC, show_spinner=False)
def fetch_user_schedule_exclusions(
    base_url: str, api_key: str, user_ids: Tuple[str, ...]
) -> List[Dict[str, Any]]:
    if not api_key or not user_ids:
        return []
    cache_key = (
        f"user_schedule_exclusions|{base_url.rstrip('/')}|{','.join(sorted(user_ids))}"
    )
    cached = _wrike_cache_get("data", cache_key, WRIKE_DATA_CACHE_TTL_SEC)
    if isinstance(cached, list):
        return cached
    data = api_get(
        base_url,
        api_key,
        "user_schedule_exclusions",
        params={"userIds": json.dumps(list(user_ids))},
    )
    out = data.get("data", [])
    _wrike_cache_set("data", cache_key, out)
    return out


def load_vacations(
    base_url: str, api_key: str, constructors: List[str]
) -> Dict[str, Set[date]]:
    """Map constructor initials -> set of non-working dates (vacations from Wrike)."""
    if not api_key or not constructors:
        return {}
    try:
        contacts = fetch_contacts(base_url, api_key)
    except Exception:  # noqa: BLE001
        return {}
    assignee_map = build_constructor_assignee_map(contacts)
    reverse = {user_id: initials for initials, user_id in assignee_map.items()}
    user_ids = [assignee_map[c] for c in constructors if c in assignee_map]
    if not user_ids:
        return {}
    try:
        exclusions = fetch_user_schedule_exclusions(base_url, api_key, tuple(user_ids))
    except Exception:  # noqa: BLE001
        return {}
    vacations: Dict[str, Set[date]] = {}
    for exclusion in exclusions:
        if exclusion.get("isWorkDays") is True:
            continue
        if exclusion.get("exclusionType") not in ("VacationPTO", "OtherNonWorking"):
            continue
        user_id = exclusion.get("userId")
        constructor = reverse.get(user_id)
        if not constructor:
            continue
        start = _task_date_part(exclusion.get("fromDate"))
        end = _task_date_part(exclusion.get("toDate"))
        if start is None or end is None:
            continue
        days = vacations.setdefault(constructor, set())
        for day in business_days_between(start, end):
            days.add(day)
    return vacations


def _fmt_date_pl(value: Optional[date]) -> str:
    return value.strftime("%d.%m.%Y") if value else "?"


def _add_date_change_comment(
    base_url: str,
    api_key: str,
    task_id: str,
    who: str,
    old_start: Optional[date],
    old_end: Optional[date],
    new_start: Optional[date],
    new_end: Optional[date],
) -> None:
    text = (
        f"Zmieniono daty ({who}): "
        f"{_fmt_date_pl(old_start)}–{_fmt_date_pl(old_end)} → "
        f"{_fmt_date_pl(new_start)}–{_fmt_date_pl(new_end)}"
    )
    try:
        api_post(base_url, api_key, f"tasks/{task_id}/comments", {"text": text})
    except Exception as exc:  # noqa: BLE001
        log(f"Failed to add date-change comment to task {task_id}: {exc}")


def _load_date_comment_sigs() -> Dict[str, str]:
    data = _read_json_file(WRIKE_DATE_COMMENTS_FILE, {})
    return data if isinstance(data, dict) else {}


def _save_date_comment_sigs(sigs: Dict[str, str]) -> None:
    _write_json_file(WRIKE_DATE_COMMENTS_FILE, sigs)


def _load_milestone_sigs() -> Dict[str, str]:
    data = _read_json_file(WRIKE_MILESTONE_COMMENTS_FILE, {})
    return data if isinstance(data, dict) else {}


def _save_milestone_sigs(sigs: Dict[str, str]) -> None:
    _write_json_file(WRIKE_MILESTONE_COMMENTS_FILE, sigs)


def _add_milestone_comment(
    base_url: str, api_key: str, task_id: str, milestone_due: date
) -> None:
    text = (
        "Planowany koniec projektu (Mechanical Design Ready): "
        f"{milestone_due.strftime('%d.%m.%Y')}"
    )
    try:
        api_post(base_url, api_key, f"tasks/{task_id}/comments", {"text": text})
    except Exception as exc:  # noqa: BLE001
        log(f"Failed to add milestone comment to task {task_id}: {exc}")


def find_eop_task(
    base_url: str,
    api_key: str,
    constructors_folder_id: str,
    number: str,
) -> Optional[Dict[str, Any]]:
    """Return the '[PLAN] {number} [EOP]' task from KONSTRUKTORZY (or None)."""
    if not api_key or not constructors_folder_id or not number:
        return None
    title = f"[PLAN] {number} [EOP]"
    legacy_title = f"[PLAN] [EOP] {number}"
    try:
        tasks = fetch_folder_tasks(base_url, api_key, constructors_folder_id)
    except Exception:  # noqa: BLE001
        return None
    for task in tasks:
        task_title = str(task.get("title") or "").strip()
        if task_title in {title, legacy_title}:
            return task
    return None


def comment_eop_change(
    base_url: str,
    api_key: str,
    constructors_folder_id: str,
    number: str,
    old_due: Optional[date],
    new_due: date,
    in_progress_status_id: str,
    todo_status_id: str,
) -> List[str]:
    """Add an EOP-change comment to constructor project tasks ToDo/In Progress."""
    errors: List[str] = []
    if (
        not api_key
        or not constructors_folder_id
        or not number
        or old_due == new_due
    ):
        return errors
    allowed_statuses = {
        status
        for status in (in_progress_status_id, todo_status_id)
        if status
    }
    try:
        tasks = fetch_folder_tasks_recursive(
            base_url, api_key, constructors_folder_id
        )
    except Exception as exc:  # noqa: BLE001
        return [f"Nie udało się pobrać tasków konstruktorów: {exc}"]
    prefix = f"[PLAN] {number} ["
    text = (
        f"Zmieniono EOP projektu {number}: "
        f"{_fmt_date_pl(old_due)} → {_fmt_date_pl(new_due)}"
    )
    for task in tasks:
        title = str(task.get("title") or "").strip()
        if not title.startswith(prefix) or "[EOP]" in title:
            continue
        if allowed_statuses and task.get("customStatusId") not in allowed_statuses:
            continue
        try:
            api_post(
                base_url,
                api_key,
                f"tasks/{task['id']}/comments",
                {"text": text},
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{title}: {exc}")
    return errors


def set_eop_date(
    base_url: str,
    api_key: str,
    constructors_folder_id: str,
    number: str,
    new_due: date,
    old_due: Optional[date] = None,
    in_progress_status_id: str = "",
    todo_status_id: str = "",
) -> Tuple[List[str], List[str]]:
    """Write the independent EOP date to '[PLAN] {number} [EOP]'.

    Returns (save_errors, comment_errors). The EOP task is deliberately not
    synchronised with the project's 'Mechanical Design Ready' milestone.
    """
    errors: List[str] = []
    if not api_key or not constructors_folder_id or not number:
        return ["Brak konfiguracji Wrike (folder konstruktorów)."], []
    title = f"[PLAN] {number} [EOP]"
    eop_task = find_eop_task(base_url, api_key, constructors_folder_id, number)
    try:
        if eop_task is None:
            api_post(
                base_url,
                api_key,
                f"folders/{constructors_folder_id}/tasks",
                {
                    "title": title,
                    "dates": {"type": "Milestone", "due": new_due.isoformat()},
                },
            )
        else:
            payload: Dict[str, Any] = {
                "dates": {"type": "Milestone", "due": new_due.isoformat()}
            }
            if str(eop_task.get("title") or "").strip() != title:
                payload["title"] = title
            api_put(base_url, api_key, f"tasks/{eop_task['id']}", payload)
    except Exception as exc:  # noqa: BLE001
        return [f"{title}: {exc}"], []

    fetch_folder_tasks.clear()
    fetch_folder_tasks_recursive.clear()
    _wrike_cache_delete(
        "data",
        f"folder_tasks|{base_url.rstrip('/')}|{constructors_folder_id}",
    )
    _wrike_cache_delete(
        "data",
        f"folder_tasks_recursive|{base_url.rstrip('/')}|{constructors_folder_id}",
    )
    comment_errors = comment_eop_change(
        base_url,
        api_key,
        constructors_folder_id,
        number,
        old_due,
        new_due,
        in_progress_status_id,
        todo_status_id,
    )
    return errors, comment_errors


def comment_constructor_changes(
    base_url: str, api_key: str, diffs: List[Dict[str, Any]]
) -> None:
    """Add a Wrike comment for date changes made by the constructor (detected on
    load). Idempotent: each change is commented only once (tracked on disk)."""
    sigs = _load_date_comment_sigs()
    changed = False
    for change in diffs:
        if not change.get("constructor_changed"):
            continue
        assignment = change.get("assignment") or {}
        task_id = assignment.get("wrike_task_id")
        wrike_start = change.get("wrike_start")
        wrike_end = change.get("wrike_end")
        if not task_id or wrike_start is None or wrike_end is None:
            continue
        sig = f"{wrike_start.isoformat()}::{wrike_end.isoformat()}"
        if sigs.get(task_id) == sig:
            continue
        local_start = _task_date_part(assignment.get("start_date"))
        local_end = _task_date_part(assignment.get("end_date"))
        _add_date_change_comment(
            base_url,
            api_key,
            task_id,
            "konstruktor",
            local_start,
            local_end,
            wrike_start,
            wrike_end,
        )
        sigs[task_id] = sig
        changed = True
    if changed:
        _save_date_comment_sigs(sigs)


def push_assignments_to_wrike(
    base_url: str,
    api_key: str,
    assignments: List[Dict[str, Any]],
    constructors_folder_id: str,
    in_progress_status_id: str,
    todo_status_id: str,
    completed_status_id: str,
    client_project_ids: Optional[Set[str]] = None,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Create/update a Wrike task per project assignment. Returns (assignments, errors)."""
    client_project_ids = client_project_ids or set()
    if not api_key or not constructors_folder_id or not in_progress_status_id:
        return list(assignments), ["Brak konfiguracji Wrike (folder konstruktorów / statusy)."]
    constructors = sorted(
        {
            str(a.get("constructor") or "").upper()
            for a in assignments
            if str(a.get("constructor") or "").strip()
        }
    )
    folder_map = fetch_constructor_folder_map(
        base_url, api_key, constructors_folder_id, tuple(constructors)
    )
    assignee_map = build_constructor_assignee_map(fetch_contacts(base_url, api_key))
    milestone_sigs = _load_milestone_sigs()
    errors: List[str] = []
    updated: List[Dict[str, Any]] = []
    for assignment in assignments:
        if assignment.get("kind") == "external":
            updated.append(assignment)
            continue
        initials = str(assignment.get("constructor") or "").upper()
        folder_id = folder_map.get(initials)
        if not folder_id:
            try:
                data = api_post(
                    base_url,
                    api_key,
                    f"folders/{constructors_folder_id}/folders",
                    {"title": initials},
                )
                folder_id = data.get("data", [{}])[0].get("id")
                folder_map[initials] = folder_id
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{initials}: nie udało się utworzyć folderu – {exc}")
                updated.append(assignment)
                continue
        payload = {
            "title": _assignment_task_title(assignment),
            "customStatus": _assignment_task_status_id(
                assignment, todo_status_id, in_progress_status_id, completed_status_id
            ),
            "dates": _assignment_task_dates(assignment),
        }
        assignee_id = assignee_map.get(initials)
        if not assignee_id:
            log(f"Brak użytkownika Wrike dla inicjałów {initials} – task bez assignee.")
        try:
            task_id = assignment.get("wrike_task_id")
            if task_id:
                old_start = old_end = None
                existing_responsible_ids: List[str] = []
                try:
                    existing_data = api_get(base_url, api_key, f"tasks/{task_id}")
                    existing = existing_data.get("data", [{}])[0]
                    existing_dates = existing.get("dates") or {}
                    old_start = _task_date_part(existing_dates.get("start"))
                    old_end = _task_date_part(existing_dates.get("due"))
                    existing_responsible_ids = list(
                        existing.get("responsibleIds") or []
                    )
                except Exception:  # noqa: BLE001
                    old_start = old_end = None
                if assignee_id:
                    payload["addResponsibles"] = [assignee_id]
                    to_remove = [
                        rid for rid in existing_responsible_ids if rid != assignee_id
                    ]
                    if to_remove:
                        payload["removeResponsibles"] = to_remove
                api_put(base_url, api_key, f"tasks/{task_id}", payload)
                new_task_id = task_id
                new_start = _task_date_part(payload["dates"]["start"])
                new_end = _task_date_part(payload["dates"]["due"])
                if (old_start, old_end) != (new_start, new_end):
                    _add_date_change_comment(
                        base_url,
                        api_key,
                        task_id,
                        "admin",
                        old_start,
                        old_end,
                        new_start,
                        new_end,
                    )
            else:
                if assignee_id:
                    payload["responsibles"] = [assignee_id]
                data = api_post(base_url, api_key, f"folders/{folder_id}/tasks", payload)
                new_task_id = data.get("data", [{}])[0].get("id")
            assignment["wrike_task_id"] = new_task_id
            assignment["wrike_folder_id"] = folder_id
            assignment["wrike_synced_start"] = payload["dates"]["start"]
            assignment["wrike_synced_end"] = payload["dates"]["due"]

            # Milestone end-date comment (client projects only).
            project_id = assignment.get("project_id")
            if project_id in client_project_ids:
                milestone_due = fetch_project_milestone_due(
                    base_url, api_key, str(project_id)
                )
                if milestone_due is not None:
                    sig = milestone_due.isoformat()
                    if milestone_sigs.get(new_task_id) != sig:
                        _add_milestone_comment(
                            base_url, api_key, new_task_id, milestone_due
                        )
                        milestone_sigs[new_task_id] = sig
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{_assignment_task_title(assignment)}: {exc}")
        updated.append(assignment)
    _save_milestone_sigs(milestone_sigs)
    return updated, errors


def delete_wrike_tasks(
    base_url: str, api_key: str, task_ids: Iterable[str]
) -> List[str]:
    errors: List[str] = []
    for task_id in task_ids:
        if not task_id:
            continue
        try:
            api_delete(base_url, api_key, f"tasks/{task_id}")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"usunięcie {task_id}: {exc}")
    return errors


def collect_wrike_date_changes(
    base_url: str, api_key: str, assignments: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    changes: List[Dict[str, Any]] = []
    for assignment in assignments:
        if assignment.get("kind") == "external":
            continue
        task_id = assignment.get("wrike_task_id")
        if not task_id:
            continue
        task = fetch_task(base_url, api_key, task_id)
        if task is None:
            changes.append({"assignment": assignment, "missing": True})
            continue
        dates = task.get("dates") or {}
        wrike_start = _task_date_part(dates.get("start"))
        wrike_end = _task_date_part(dates.get("due"))
        local_start = _task_date_part(assignment.get("start_date"))
        local_end = _task_date_part(assignment.get("end_date"))
        synced_start = _task_date_part(assignment.get("wrike_synced_start"))
        synced_end = _task_date_part(assignment.get("wrike_synced_end"))
        # A change is attributed to the constructor only when Wrike moved away
        # from the last pushed (synced) dates.  Otherwise it is a local edit.
        constructor_changed = (
            synced_start is not None
            and synced_end is not None
            and (wrike_start, wrike_end) != (synced_start, synced_end)
        )
        if wrike_start != local_start or wrike_end != local_end:
            changes.append(
                {
                    "assignment": assignment,
                    "wrike_start": wrike_start,
                    "wrike_end": wrike_end,
                    "constructor_changed": constructor_changed,
                }
            )
    return changes


def apply_wrike_dates(
    assignments: List[Dict[str, Any]], changes: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    by_id = {a.get("id"): a for a in assignments}
    for change in changes:
        if change.get("missing"):
            continue
        assignment = by_id.get(change["assignment"].get("id"))
        if assignment is None:
            continue
        wrike_start = change.get("wrike_start")
        wrike_end = change.get("wrike_end")
        if wrike_start is None or wrike_end is None:
            continue
        assignment["start_date"] = wrike_start.isoformat()
        assignment["end_date"] = wrike_end.isoformat()
        assignment["wrike_synced_start"] = wrike_start.isoformat()
        assignment["wrike_synced_end"] = wrike_end.isoformat()
        hours_per_day = float(assignment.get("hours_per_day") or 0.0)
        days: Dict[str, float] = {}
        for day in business_days_between(wrike_start, wrike_end):
            days[day.isoformat()] = round(hours_per_day, 2)
        assignment["days"] = days
        assignment["working_days"] = len(days)
    return list(assignments)


def _apply_full_project_range(
    key: str,
    project_id: str,
    known_range: Optional[Tuple[date, date]],
    base_url: str,
    api_key: str,
    core_task_type: str,
    bom_task_type: str,
) -> None:
    if not project_id or project_id == "__inne__":
        return
    value = known_range or fetch_project_core_date_range(
        base_url, api_key, project_id, core_task_type, bom_task_type
    )
    if value is not None:
        st.session_state[key] = value
        st.session_state.pop("form_full_project_flash", None)
    else:
        st.session_state["form_full_project_flash"] = (
            "Brak core tasków z datami dla wybranego projektu."
        )


def render_resource_planning_section(
    project_map: Dict[str, str],
    constructors: List[str],
    effective_daily_hours: float,
    base_url: str,
    api_key: str,
    internal_folder: str,
    constructors_folder: str,
    in_progress_status_id: str,
    todo_status_id: str,
    completed_status_id: str,
    core_df: Optional[pd.DataFrame] = None,
    default_project_id: Optional[str] = None,
    core_task_type: str = "",
    bom_task_type: str = "",
) -> None:
    st.subheader("Planowanie konstruktorów")
    st.caption(
        "Dodaj lub edytuj obciążenie konstruktora (konstruktor → projekt → zakres dat "
        "→ godziny dziennie). Po „Zapisz” obciążenia projektowe są synchronizowane do "
        "Wrike jako jeden task na obciążenie."
    )
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
        st.warning(
            "Masz niezapisane zmiany. Kliknij „Zapisz”, aby zapisać na dysku "
            "i zsynchronizować z Wrike."
        )

    project_demand: Dict[str, float] = {}
    project_ranges: Dict[str, Tuple[date, date]] = {}
    project_end_map: Dict[str, date] = {}
    if core_df is not None and not core_df.empty and "project_id" in core_df.columns:
        demand_source = core_df
        if "is_top_level" in core_df.columns:
            demand_source = core_df[core_df["is_top_level"] == True]
        demand_df = (
            demand_source[["project_id", "planned_hours"]]
            .dropna(subset=["project_id"])
            .copy()
        )
        demand_df["project_id"] = demand_df["project_id"].astype(str).str.strip()
        demand_df["planned_hours"] = pd.to_numeric(
            demand_df["planned_hours"], errors="coerce"
        ).fillna(0.0)
        demand_df = demand_df[demand_df["project_id"] != ""]
        project_demand = {
            str(pid): float(hours)
            for pid, hours in demand_df.groupby("project_id")["planned_hours"]
            .sum()
            .items()
        }
        project_ranges = project_date_ranges(core_df)
        project_end_map = {
            str(pid): end for pid, (_, end) in project_ranges.items()
        }
    internal_projects = (
        fetch_internal_projects(base_url, api_key, internal_folder)
        if internal_folder and api_key
        else []
    )
    project_options = _combined_project_options(project_map, internal_projects)

    def do_save() -> None:
        working = st.session_state["constructor_assignments"]
        errors: List[str] = []
        pending = set(st.session_state.get("wrike_pending_delete", set()))
        if pending:
            errors += delete_wrike_tasks(base_url, api_key, pending)
            st.session_state["wrike_pending_delete"] = set()
        updated, push_errors = push_assignments_to_wrike(
            base_url,
            api_key,
            working,
            constructors_folder,
            in_progress_status_id,
            todo_status_id,
            completed_status_id,
            client_project_ids=set(project_map.keys()),
        )
        # Task dates just changed in Wrike — drop the cached task data so the
        # next date-change check fetches fresh dates (avoids false "konstruktor
        # zmienił daty" warnings).
        fetch_task.clear()
        fetch_folder_tasks.clear()
        fetch_folder_tasks_recursive.clear()
        fetch_project_milestone.clear()
        errors += push_errors
        st.session_state["constructor_assignments"] = updated
        save_constructor_assignments(updated)
        st.session_state["assignments_dirty"] = False
        if errors:
            _set_assignment_flash(
                "Zapisano lokalnie, ale Wrike zgłosiło błędy: " + " | ".join(errors),
                "warning",
            )
        else:
            _set_assignment_flash("Zapisano i zsynchronizowano z Wrike.")
        st.rerun()

    def render_save_controls(suffix: str) -> None:
        current_dirty = bool(st.session_state.get("assignments_dirty", False))
        save_col, revert_col, hint_col = st.columns([1, 1, 3])
        if save_col.button("Zapisz", type="primary", key=f"save_assignments_{suffix}"):
            do_save()
        if revert_col.button(
            "Cofnij zmiany",
            key=f"revert_assignments_{suffix}",
            disabled=not current_dirty,
        ):
            st.session_state["constructor_assignments"] = load_constructor_assignments()
            st.session_state["assignments_dirty"] = False
            st.session_state["wrike_pending_delete"] = set()
            st.session_state.pop("form_edit_id", None)
            st.session_state["form_nonce"] = int(st.session_state.get("form_nonce", 0)) + 1
            _set_assignment_flash("Przywrócono ostatni zapis.")
            st.rerun()
        hint_col.caption("Zmiany są trwałe dopiero po kliknięciu „Zapisz”.")

    # Detect Wrike date changes and deleted tasks before saving (so removed
    # assignments are not pushed again).
    changes = (
        collect_wrike_date_changes(base_url, api_key, assignments)
        if api_key
        else []
    )
    missing = [c for c in changes if c.get("missing")]
    diffs = [c for c in changes if not c.get("missing")]
    constructor_diffs = [c for c in diffs if c.get("constructor_changed")]
    removed_titles: List[str] = []
    if missing:
        missing_ids = {c["assignment"]["id"] for c in missing}
        removed_titles = [_assignment_task_title(c["assignment"]) for c in missing]
        st.session_state["constructor_assignments"] = [
            a for a in assignments if a["id"] not in missing_ids
        ]
        st.session_state["assignments_dirty"] = True
        assignments = st.session_state["constructor_assignments"]

    render_save_controls("top")

    # Wrike date differences: user chooses the source of truth.
    if removed_titles:
        st.warning(
            "Usunięto obciążenia (brak taska w Wrike): "
            + ", ".join(f"**{t}**" for t in removed_titles)
        )
    if diffs:
        lines = [
            f"- {c['assignment'].get('constructor')} | "
            f"{c['assignment'].get('label') or c['assignment'].get('project')}: "
            f"lokalnie {c['assignment'].get('start_date')}–{c['assignment'].get('end_date')}, "
            f"Wrike {c['wrike_start']}–{c['wrike_end']}"
            for c in diffs
        ]
        st.warning(
            "Różnice dat między aplikacją a Wrike — wybierz źródło:\n\n"
            + "\n".join(lines)
            + "\n\n**Zastosuj daty z Wrike** = pobierz daty z Wrike do aplikacji. "
            "**Zapisz** = wyślij lokalne daty do Wrike."
        )
        if st.button("Zastosuj daty z Wrike", key="apply_wrike_dates"):
            comment_constructor_changes(base_url, api_key, constructor_diffs)
            st.session_state["constructor_assignments"] = apply_wrike_dates(
                assignments, diffs
            )
            st.session_state["assignments_dirty"] = True
            do_save()

    # Add / edit form
    nonce = int(st.session_state.get("form_nonce", 0))
    editing_id = st.session_state.get("form_edit_id")
    editing = find_assignment(assignments, editing_id) if editing_id else None

    if editing:
        st.markdown("### Edycja obciążenia")
        if st.button("Anuluj edycję", key="cancel_form_edit"):
            st.session_state.pop("form_edit_id", None)
            st.session_state["form_nonce"] = nonce + 1
            st.rerun()
    else:
        st.markdown("### Dodaj obciążenie")

    constructor_options = list(constructors)
    if editing and str(editing.get("constructor") or "").upper() not in constructor_options:
        constructor_options.append(str(editing.get("constructor") or "").upper())
    constructor_key = f"form_constructor_{nonce}"
    if st.session_state.get(constructor_key) not in constructor_options:
        st.session_state[constructor_key] = (
            str(editing.get("constructor") or "").upper() if editing else constructor_options[0]
        )
    selected_constructor = st.selectbox(
        "Konstruktor", options=constructor_options, key=constructor_key
    )

    preview_fig = build_resource_usage_figure(
        assignments, [selected_constructor], effective_daily_hours, strict=True, height=340
    )
    if preview_fig is not None:
        with st.expander(f"Wykres — {selected_constructor}", expanded=True):
            st.plotly_chart(preview_fig, use_container_width=True)

    label_options = {o["id"]: o["title"] for o in project_options}
    label_options["__inne__"] = "Inne zadanie (bez projektu)"
    project_ids = list(label_options.keys())
    project_key = f"form_project_{nonce}_{default_project_id or 'none'}"
    if st.session_state.get(project_key) not in project_ids:
        if editing:
            default_pid = (
                "__inne__"
                if editing.get("kind") == "external"
                else editing.get("project_id")
            )
        elif default_project_id and default_project_id in project_ids:
            default_pid = default_project_id
        else:
            default_pid = project_ids[0] if project_ids else "__inne__"
        if default_pid not in project_ids:
            default_pid = project_ids[0] if project_ids else "__inne__"
        st.session_state[project_key] = default_pid
    selected_project = st.selectbox(
        "Projekt",
        options=project_ids,
        format_func=lambda pid: label_options[pid],
        key=project_key,
    )

    is_external = selected_project == "__inne__"
    if is_external:
        label_value = st.text_input(
            "Nazwa zadania",
            value=str(editing.get("label") or "") if editing else "",
            key=f"form_label_{nonce}",
        )
    else:
        label_value = ""

    if editing:
        try:
            edit_start = date.fromisoformat(str(editing.get("start_date")))
        except (TypeError, ValueError):
            edit_start = date.today()
        try:
            edit_end = date.fromisoformat(str(editing.get("end_date")))
        except (TypeError, ValueError):
            edit_end = edit_start
    else:
        edit_start = edit_end = date.today()
    form_project_range = (
        project_ranges.get(str(selected_project))
        if selected_project != "__inne__"
        else None
    )
    full_project_flash = st.session_state.pop("form_full_project_flash", None)
    if full_project_flash:
        st.warning(full_project_flash)
    range_col, full_project_col = st.columns([3, 1])
    range_key = f"form_range_{nonce}"
    with range_col:
        if range_key in st.session_state:
            date_range = st.date_input("Zakres dat", key=range_key)
        else:
            date_range = st.date_input(
                "Zakres dat", value=(edit_start, edit_end), key=range_key
            )
    with full_project_col:
        st.write("")
        st.button(
            "Cały projekt",
            key=f"form_full_project_{nonce}",
            disabled=selected_project == "__inne__",
            on_click=_apply_full_project_range,
            args=(
                range_key,
                selected_project,
                form_project_range,
                base_url,
                api_key,
                core_task_type,
                bom_task_type,
            ),
            help=(
                "Ustawia zakres od najwcześniejszego startu do najpóźniejszego "
                "terminu core tasków wybranego projektu."
                if selected_project != "__inne__"
                else "Wybierz projekt, aby ustawić zakres."
            ),
        )
    if isinstance(date_range, (tuple, list)) and len(date_range) == 2:
        form_start, form_end = date_range
    else:
        form_start = form_end = date_range

    default_hpd = (
        int(round(float(editing.get("hours_per_day") or effective_daily_hours)))
        if editing
        else int(round(effective_daily_hours))
    )
    form_hpd = st.number_input(
        "Godziny dziennie",
        min_value=1,
        max_value=24,
        value=default_hpd,
        step=1,
        key=f"form_hpd_{nonce}",
    )

    if st.button("Zapisz obciążenie", type="primary", key="form_save"):
        if isinstance(form_end, date) and isinstance(form_start, date) and form_end < form_start:
            st.error("Data końcowa nie może być wcześniejsza od początkowej.")
        elif is_external and not str(label_value).strip():
            st.error("Podaj nazwę zadania.")
        else:
            if is_external:
                project_id = None
                project_name = "Inne zadanie"
                label = str(label_value).strip()
                number = label
                kind = "external"
            else:
                option = next((o for o in project_options if o["id"] == selected_project), None)
                project_id = selected_project
                project_name = option["title"] if option else selected_project
                label = project_name
                number = option["number"] if option else project_name
                kind = "project"
            hpd = int(form_hpd)
            n_days = len(business_days_between(form_start, form_end))
            off = set(st.session_state.get("holidays", set())) | set(
                st.session_state.get("vacations", {}).get(
                    str(selected_constructor).upper(), set()
                )
            )
            exclude_id = editing["id"] if editing else None

            def available_fn(day: date) -> float:
                return max(
                    0.0,
                    float(effective_daily_hours)
                    - _assignment_day_hours(
                        assignments,
                        str(selected_constructor).upper(),
                        day,
                        exclude_id=exclude_id,
                    ),
                )

            days = allocate_working_days(form_start, n_days, hpd, off, available_fn)
            if not days:
                st.error("Brak wolnych dni roboczych w wybranym zakresie.")
                st.stop()
            if len(days) < n_days:
                st.warning(
                    f"Konstruktor ma ograniczoną dostępność — przydzielono "
                    f"{len(days)} z {n_days} dni roboczych."
                )
            new_assignment = {
                "id": editing["id"] if editing else uuid.uuid4().hex,
                "kind": kind,
                "project_id": project_id,
                "project": project_name,
                "label": label,
                "project_number": number,
                "constructor": str(selected_constructor).upper(),
                "start_date": form_start.isoformat(),
                "end_date": max(days),
                "hours_per_day": float(hpd),
                "days": days,
                "working_days": len(days),
                "created_at": editing.get("created_at") if editing else datetime.utcnow().isoformat(timespec="seconds"),
                "wrike_task_id": editing.get("wrike_task_id") if editing else None,
                "wrike_folder_id": editing.get("wrike_folder_id") if editing else None,
                "wrike_synced_start": editing.get("wrike_synced_start") if editing else None,
                "wrike_synced_end": editing.get("wrike_synced_end") if editing else None,
            }
            if editing:
                working = replace_assignment(assignments, new_assignment)
            else:
                working = list(assignments) + [new_assignment]
            st.session_state["constructor_assignments"] = working
            st.session_state["assignments_dirty"] = True
            _set_assignment_flash("Zapisano obciążenie (zapisz, aby wysłać do Wrike).")
            st.session_state.pop("form_edit_id", None)
            st.session_state["form_nonce"] = nonce + 1
            st.rerun()

    # List of assignments
    st.divider()
    with st.expander("Zarządzaj przypisaniami", expanded=True):
        if not assignments:
            st.caption("Brak zapisanych przypisań.")
        else:
            today = date.today()
            active_assignments = [
                a for a in assignments
                if (_assignment_end_date(a) or date.max) >= today
            ]
            if not active_assignments:
                st.caption("Brak aktywnych przypisań (wszystkie już zakończone).")
            else:
                summary_rows = []
                for assignment in active_assignments:
                    summary_rows.append(
                        {
                            "Typ": "Inne zadanie" if assignment.get("kind") == "external" else "Projekt",
                            "Projekt / zadanie": assignment.get("label") or assignment.get("project") or "",
                            "Konstruktor": assignment.get("constructor", ""),
                            "Od": assignment.get("start_date", ""),
                            "Do": assignment.get("end_date", ""),
                            "h/dzień": assignment.get("hours_per_day"),
                            "Przypisane h": _assignment_total_hours(assignment),
                            "Wrike": "✓" if assignment.get("wrike_task_id") else "—",
                        }
                    )
                st.dataframe(pd.DataFrame(summary_rows), hide_index=True, width="stretch")
                options = {
                    assignment["id"]: (
                        f"{assignment.get('constructor', '')} | "
                        f"{assignment.get('label') or assignment.get('project') or ''} | "
                        f"{assignment.get('start_date', '')}–{assignment.get('end_date', '')} | "
                        f"{_assignment_total_hours(assignment):.0f} h"
                    )
                    for assignment in active_assignments
                }
                if st.session_state.get("manage_assignment_id") not in options:
                    st.session_state["manage_assignment_id"] = next(iter(options))
                selected_assignment = st.selectbox(
                    "Wybierz przypisanie",
                    options=list(options.keys()),
                    format_func=lambda key: options[key],
                    key="manage_assignment_id",
                )
                selected_obj = find_assignment(assignments, selected_assignment)
                selected_pid = (
                    str(selected_obj.get("project_id") or "") if selected_obj else ""
                )
                if (
                    selected_obj is not None
                    and selected_obj.get("kind") != "external"
                    and selected_pid
                    and selected_pid in project_demand
                ):
                    demand_h: Optional[float] = project_demand[selected_pid]
                    others_h = round(
                        sum(
                            _assignment_total_hours(a)
                            for a in assignments
                            if a.get("kind") != "external"
                            and str(a.get("project_id") or "") == selected_pid
                            and a.get("id") != selected_assignment
                        ),
                        2,
                    )
                    target_h: Optional[float] = round(demand_h - others_h, 2)
                    current_h = _assignment_total_hours(selected_obj)
                    st.caption(
                        f"Plan projektu: {demand_h:.0f} h · inne przypisania: "
                        f"{others_h:.0f} h · docelowo: {target_h:.0f} h · "
                        f"obecnie: {current_h:.0f} h"
                    )
                else:
                    demand_h = None
                    others_h = 0.0
                    target_h = None
                    current_h = (
                        _assignment_total_hours(selected_obj) if selected_obj else 0.0
                    )
                    if (
                        selected_obj is not None
                        and selected_obj.get("kind") == "external"
                    ):
                        st.caption(
                            "Dopasowanie do zapotrzebowania dotyczy tylko "
                            "przypisań projektowych."
                        )
                    else:
                        st.caption(
                            "Brak planu projektu w wybranym filtrze KPI — "
                            "nie można dopasować."
                        )
                edit_col, fit_col, delete_col = st.columns(3)
                if edit_col.button("Edytuj wybrane", key="edit_assignment_btn"):
                    st.session_state["form_edit_id"] = selected_assignment
                    st.session_state["form_nonce"] = nonce + 1
                    st.rerun()
                if fit_col.button(
                    "Dopasuj do zapotrzebowania",
                    key="fit_assignment_btn",
                    disabled=target_h is None,
                ):
                    target_obj = find_assignment(assignments, selected_assignment)
                    if target_obj is None:
                        _set_assignment_flash(
                            "Nie znaleziono wybranego przypisania.", "warning"
                        )
                        st.rerun()
                    elif target_h is None or demand_h is None:
                        _set_assignment_flash(
                            "Brak planu projektu — nie można dopasować.",
                            "warning",
                        )
                        st.rerun()
                    elif target_h <= 0.001:
                        _set_assignment_flash(
                            f"Projekt jest już pokryty innymi przypisaniami "
                            f"({others_h:.0f} h przy planie {demand_h:.0f} h) "
                            f"— bez zmian.",
                            "warning",
                        )
                        st.rerun()
                    elif abs(target_h - current_h) <= 0.01:
                        _set_assignment_flash(
                            "Przypisanie jest już dopasowane do zapotrzebowania."
                        )
                        st.rerun()
                    else:
                        constructor = str(
                            target_obj.get("constructor") or ""
                        ).upper()
                        off_days = set(
                            st.session_state.get("holidays", set())
                        ) | set(
                            st.session_state.get("vacations", {}).get(
                                constructor, set()
                            )
                        )
                        project_id = str(target_obj.get("project_id") or "")
                        project_end = project_end_map.get(project_id)
                        milestone_due = (
                            fetch_project_milestone_due(
                                base_url, api_key, project_id
                            )
                            if project_id and base_url and api_key
                            else None
                        )
                        if milestone_due is not None and (
                            project_end is None or milestone_due > project_end
                        ):
                            project_end = milestone_due
                        updated, unfilled = fit_assignment_to_hours(
                            target_obj,
                            assignments,
                            target_h,
                            float(effective_daily_hours),
                            off_days,
                            project_end=project_end,
                        )
                        if updated is None:
                            end_txt = (
                                f" (limit {project_end:%d.%m.%Y})"
                                if project_end is not None
                                else ""
                            )
                            _set_assignment_flash(
                                "Nie udało się dopasować — brak wolnych dni "
                                f"roboczych{end_txt}.",
                                "warning",
                            )
                            st.rerun()
                        else:
                            st.session_state[
                                "constructor_assignments"
                            ] = replace_assignment(assignments, updated)
                            st.session_state["assignments_dirty"] = True
                            label = (
                                updated.get("label")
                                or updated.get("project")
                                or ""
                            )
                            message = (
                                f"Dopasowano {constructor} | {label}: "
                                f"{current_h:.0f} h → "
                                f"{_assignment_total_hours(updated):.0f} h "
                                f"(do {updated.get('end_date')})."
                            )
                            if unfilled > 0.5:
                                end_txt = (
                                    f" {project_end:%d.%m.%Y}"
                                    if project_end is not None
                                    else ""
                                )
                                message += (
                                    f" Nie zmieściło się {unfilled:.0f} h do "
                                    f"końca projektu{end_txt}."
                                )
                                _set_assignment_flash(message, "warning")
                            else:
                                _set_assignment_flash(message)
                            st.rerun()
                if delete_col.button("Usuń wybrane", key="delete_assignment_btn"):
                    target = find_assignment(assignments, selected_assignment)
                    if target and target.get("wrike_task_id"):
                        st.session_state.setdefault("wrike_pending_delete", set()).add(
                            target["wrike_task_id"]
                        )
                    st.session_state["constructor_assignments"] = [
                        a for a in assignments if a["id"] != selected_assignment
                    ]
                    if st.session_state.get("form_edit_id") == selected_assignment:
                        st.session_state.pop("form_edit_id", None)
                    st.session_state["assignments_dirty"] = True
                    _set_assignment_flash("Usunięto obciążenie (zapisz, aby usunąć task w Wrike).")
                    st.rerun()

    st.divider()
    render_save_controls("bottom")


# ---- UI --------------------------------------------------------------------
def build_resource_usage_figure(
    assignments: List[Dict[str, Any]],
    constructors: List[str],
    effective_daily_hours: float,
    strict: bool = False,
    height: int = 800,
) -> Optional[go.Figure]:
    """Build the free-hours figure. In strict mode only the passed constructors
    are shown (used for the single-constructor preview in the form)."""
    expanded = expand_assignment_days(assignments)

    present_constructors = (
        sorted(
            {
                str(value).upper()
                for value in expanded["constructor"].dropna().tolist()
                if str(value).strip()
            }
        )
        if not expanded.empty
        else []
    )

    if strict:
        available_constructors = [
            str(c).strip().upper() for c in constructors if str(c).strip()
        ]
    else:
        available_constructors = [
            str(c).strip().upper() for c in constructors if str(c).strip()
        ]
        for constructor in present_constructors:
            if constructor not in available_constructors:
                available_constructors.append(constructor)
    if not available_constructors:
        return None

    # Load a wide window (a year back and a year forward) so the chart can be
    # panned/zoomed, but keep the default view at today → +2 months.
    today = date.today()
    data_start = today - timedelta(days=365)
    data_end = today + timedelta(days=365)
    default_start = today
    default_end = today + timedelta(days=60)

    if expanded.empty:
        assigned_map: Dict[Tuple[Any, str], float] = {}
        short_map: Dict[Tuple[Any, str], List[str]] = {}
    else:
        assigned = (
            expanded.groupby(["date", "constructor"], as_index=False)["hours"].sum()
        )
        assigned_map = {
            (row["date"], row["constructor"]): float(row["hours"])
            for _, row in assigned.iterrows()
        }
        short_map = {}
        for (day_value, constructor), group in expanded.groupby(["date", "constructor"]):
            labels: List[str] = []
            for _, row in group.iterrows():
                if row["kind"] == "external":
                    text = str(row["label"] or row["project"] or "Inne zadanie")
                else:
                    text = str(row["project"] or "")
                token = (text.strip().split() or [""])[0]
                if token and token not in labels:
                    labels.append(token)
            short_map[(day_value, constructor)] = labels
    business_days = [d.date() for d in pd.bdate_range(data_start, data_end)]
    holidays = st.session_state.get("holidays", set())
    holiday_names = st.session_state.get("holiday_names", {})
    vacations = st.session_state.get("vacations", {})
    holiday_days = [d for d in business_days if d in holidays]
    rows: List[Dict[str, Any]] = []
    for day in business_days:
        if day in holidays:
            continue  # holidays are marked with a blue vertical line instead
        date_str = day.strftime("%d.%m.%Y")
        for constructor in available_constructors:
            is_vacation = day in vacations.get(constructor, set())
            if is_vacation:
                free = 0.0
                hover = (
                    f"<b>{constructor}</b><br>Dzień: {date_str}<br>"
                    f"Urlop<br>Zajęte: {float(effective_daily_hours):.2f} h<br>"
                    f"Wolne godziny: 0.00"
                )
            else:
                used = assigned_map.get((day, constructor), 0.0)
                free = max(0.0, float(effective_daily_hours) - used)
                labels = short_map.get((day, constructor), [])
                if labels:
                    hover = (
                        f"<b>{constructor}</b><br>Dzień: {date_str}<br>"
                        f"Projekt: {', '.join(labels)}<br>Wolne godziny: {free:.2f}"
                    )
                else:
                    hover = (
                        f"<b>{constructor}</b><br>Dzień: {date_str}<br>"
                        f"Wolne godziny: {free:.2f}"
                    )
            rows.append(
                {
                    "date": day,
                    "constructor": constructor,
                    "free_hours": round(free, 2),
                    "hover": hover,
                    "is_vacation": is_vacation,
                }
            )
    free_df = pd.DataFrame(rows)
    if free_df.empty:
        return None
    # Initials are stacked deterministically: constructors that share the same
    # number of free hours on a given day are shifted vertically in alphabetical
    # order, so they never hide each other (no random jitter).
    free_df["date"] = pd.to_datetime(free_df["date"])
    free_df = free_df.sort_values(["date", "free_hours", "constructor"])
    group_keys = ["date", "free_hours"]
    free_df["_rank"] = free_df.groupby(group_keys).cumcount()
    free_df["_count"] = free_df.groupby(group_keys)["constructor"].transform("size")
    step = 0.15
    centered = free_df["_rank"] - (free_df["_count"] - 1) / 2.0
    odd_stack = free_df["_count"] % 2 == 1
    # Half a step keeps the middle initial off the gridline.  Shifting towards
    # the centre of the axis (up for low levels, down for high levels) keeps
    # the labels inside the 0..6 band where possible — including a lone
    # constructor, which would otherwise sit exactly on the line.
    direction = free_df["free_hours"].gt(3.0).map({True: -0.5, False: 0.5})
    offsets = centered + odd_stack.astype(float) * direction
    free_df["y_pos"] = free_df["free_hours"] + offsets * step
    # A vacation means the constructor is fully occupied (6h), i.e. 0 free hours.
    # Place vacations below the whole daily stack, as if they occupied one extra
    # constructor slot outside the pool, so they never collide with normal initials.
    vacation_mask = free_df["is_vacation"]
    if vacation_mask.any():
        normal_min = (
            free_df[~vacation_mask].groupby("date")["y_pos"].min()
        )
        vacation_rank = free_df[vacation_mask].groupby("date").cumcount()
        base = free_df.loc[vacation_mask, "date"].map(normal_min).fillna(0.0)
        free_df.loc[vacation_mask, "y_pos"] = base - step * (vacation_rank + 1)

    ordered_constructors = sorted(available_constructors)
    palette = px.colors.qualitative.Plotly
    color_map = {
        constructor: palette[index % len(palette)]
        for index, constructor in enumerate(ordered_constructors)
    }

    fig = go.Figure()
    for constructor in ordered_constructors:
        subset = free_df[free_df["constructor"] == constructor]
        if subset.empty:
            continue
        normal = subset[~subset["is_vacation"]]
        vacation = subset[subset["is_vacation"]]
        if not normal.empty:
            fig.add_trace(
                go.Scatter(
                    x=normal["date"],
                    y=normal["y_pos"],
                    mode="text",
                    text=[constructor] * len(normal),
                    textposition="middle right",
                    textfont=dict(color=color_map[constructor], size=10),
                    marker=dict(color=color_map[constructor]),
                    name=constructor,
                    hovertext=normal["hover"].tolist(),
                    hoverinfo="text",
                )
            )
        if not vacation.empty:
            # Shift the whole badge (circle + initial) slightly right so the
            # initial lines up with the other initials, which start at the date
            # point.  Keeping both in one trace moves the circle together with
            # the initial instead of leaving it behind on the date.
            vacation_x = vacation["date"] + pd.Timedelta(hours=8)
            fig.add_trace(
                go.Scatter(
                    x=vacation_x,
                    y=vacation["y_pos"],
                    mode="markers+text",
                    text=[constructor] * len(vacation),
                    textposition="middle center",
                    textfont=dict(color="red", size=10),
                    marker=dict(
                        color="white", size=16, line=dict(color="red", width=1)
                    ),
                    name=constructor,
                    hovertext=vacation["hover"].tolist(),
                    hoverinfo="text",
                )
            )
    y_min = -0.7
    if vacation_mask.any():
        # Leave room under the deepest vacation marker.
        y_min = min(y_min, float(free_df.loc[vacation_mask, "y_pos"].min()) - step * 1.5)
    fig.update_yaxes(
        range=[y_min, 6.7],
        tickvals=[0, 1, 2, 3, 4, 5, 6],
        gridcolor="#e5e5e5",
        griddash="dot",
        title_text="Wolne godziny",
        fixedrange=True,
    )
    fig.update_xaxes(
        tickmode="linear",
        tick0="2024-01-01",  # a Monday, so weekly gridlines fall on Mondays
        dtick=7 * 24 * 3600 * 1000,
        range=[default_start, default_end],
        showgrid=True,
        gridcolor="#ebebeb",
        title_text="Dzień",
    )
    fig.update_layout(
        height=height,
        showlegend=False,
        margin=dict(l=50, r=20, t=20, b=50),
        dragmode="pan",
    )
    # Red vertical marker on today's date.
    fig.add_vline(
        x=pd.Timestamp(today),
        line_width=2,
        line_dash="dash",
        line_color="red",
    )
    fig.add_annotation(
        x=pd.Timestamp(today),
        y=0.98,
        xref="x",
        yref="paper",
        text="DZIŚ",
        showarrow=False,
        textangle=90,
        xanchor="left",
        yanchor="top",
        xshift=4,
        font=dict(size=11, color="red"),
    )
    # Blue vertical markers on holidays (business-day holidays only).
    for holiday_day in holiday_days:
        fig.add_vline(
            x=pd.Timestamp(holiday_day),
            line_width=1,
            line_dash="dash",
            line_color="blue",
        )
        fig.add_annotation(
            x=pd.Timestamp(holiday_day),
            y=0.98,
            xref="x",
            yref="paper",
            text=str(holiday_names.get(holiday_day, "") or ""),
            showarrow=False,
            textangle=90,
            xanchor="left",
            yanchor="top",
            xshift=4,
            font=dict(size=9, color="blue"),
        )
    return fig


def render_resource_usage(
    assignments: List[Dict[str, Any]],
    constructors: List[str],
    effective_daily_hours: float,
) -> None:
    st.subheader("ZUŻYCIE ZASOBÓW")
    st.caption(
        "Wolne godziny konstruktorów. Każdy inicjał to jeden konstruktor danego dnia, "
        "a jego wysokość oznacza pozostały (nieprzypisany) czas względem dziennego limitu "
        "6 godzin. Inicjały o tej samej liczbie wolnych godzin są układane jedne nad "
        "drugimi, zawsze w kolejności alfabetycznej. Urlop pokazano na poziomie 0 wolnych "
        "godzin (6 h zajęte). Domyślnie widoczne są najbliższe dwa "
        "miesiące — przesuń lub przybliż wykres, aby zobaczyć inne okresy (dane obejmują "
        "rok wstecz i rok w przód)."
    )
    if st.session_state.get("assignments_dirty"):
        st.warning(
            "Widok obejmuje niezapisane zmiany. Przejdź do DAILY PLANNED EFFORT "
            "i kliknij „Zapisz”, aby utrwalić je na dysku."
        )
    if not constructors:
        st.info("Brak konstruktorów. Dodaj ich w panelu bocznym.")
        return
    if not assignments:
        st.info("Brak przypisań konstruktorów. Dodaj je w DAILY PLANNED EFFORT.")
        return
    fig = build_resource_usage_figure(assignments, constructors, effective_daily_hours)
    if fig is None:
        st.info("Brak danych do wyświetlenia.")
        return
    st.plotly_chart(fig, use_container_width=True)


def main() -> None:
    st.set_page_config(
        page_title=f"KPI DASHBOARD v{get_app_version()}", layout="wide"
    )
    reset_logs()
    reset_perf_metrics()
    check_password()
    _init_resource_state()
    title_col, version_col = st.columns([6, 1])
    with title_col:
        st.title("KPI DASHBOARD")
    with version_col:
        render_version_badge()

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
            "Otwarte zadania po terminie oceniamy tak, jakby zakończyły się dziś — ocena "
            "maleje wraz z narastającym opóźnieniem. Otwarte zadania w terminie (także "
            "z terminem dziś) nie są oceniane."
        )
        st.header("Konfiguracja")
        base_url = DEFAULT_BASE_URL
        api_key = DEFAULT_API_KEY
        client_folder = DEFAULT_CLIENT_FOLDER
        core_task_type = DEFAULT_CORE_TASK_TYPE
        bom_task_type = DEFAULT_BOM_TASK_TYPE
        rev_task_type = DEFAULT_REV_TASK_TYPE
        planned_field_id = DEFAULT_PLANNED_FIELD_ID
        completed_status_id = DEFAULT_COMPLETED_STATUS_ID
        internal_folder = DEFAULT_INTERNAL_FOLDER
        constructors_folder = DEFAULT_CONSTRUCTORS_FOLDER
        in_progress_status_id = DEFAULT_IN_PROGRESS_STATUS_ID
        todo_status_id = DEFAULT_TODO_STATUS_ID
        constructor_completed_status_id = DEFAULT_CONSTRUCTOR_COMPLETED_STATUS_ID
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
        st.header("Dni wolne (globalne)")
        st.caption("Firmowe dni wolne dla wszystkich (np. odbiór święta z soboty).")
        manual_days = st.session_state.get("manual_off_days", set())
        new_off = st.date_input(
            "Dodaj dzień wolny", value=date.today(), key="manual_off_new"
        )
        if st.button("Dodaj dzień wolny", key="manual_off_add"):
            if isinstance(new_off, date):
                manual_days.add(new_off)
                st.session_state["manual_off_days"] = manual_days
                save_manual_off_days(manual_days)
                _refresh_off_days()
                st.rerun()
        if manual_days:
            st.caption("Dodane dni:")
            for day_value in sorted(manual_days):
                if st.button(
                    f"✖ {day_value.strftime('%d.%m.%Y')}",
                    key=f"del_manual_off_{day_value.isoformat()}",
                ):
                    manual_days.discard(day_value)
                    st.session_state["manual_off_days"] = manual_days
                    save_manual_off_days(manual_days)
                    _refresh_off_days()
                    st.rerun()
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

    if st.session_state["page"] == "ZUŻYCIE ZASOBÓW":
        render_resource_usage(
            assignments=st.session_state["constructor_assignments"],
            constructors=st.session_state["constructors"],
            effective_daily_hours=float(st.session_state["effective_daily_hours"]),
        )
        return

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
    selected_project = st.selectbox(
        "Wybierz projekt:",
        options=sorted_project_ids,
        index=None,
        placeholder="Wybierz projekt…",
        format_func=lambda pid: project_map[pid],
    )
    selected_projects = [selected_project] if selected_project else []

    if not selected_projects:
        st.info("Wybierz projekt, aby zobaczyć KPI.")
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
            "rev_task_type": rev_task_type,
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
            rev_task_type=rev_task_type,
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

            eop_flash = st.session_state.pop("eop_flash", None)
            if eop_flash:
                flash_message, flash_level = eop_flash
                if flash_level == "warning":
                    st.warning(flash_message)
                else:
                    st.success(flash_message)

            selected_project_id = selected_projects[0]
            project_title = project_map.get(selected_project_id, "")
            eop_number = (project_title.strip().split() or [""])[0]
            eop_task = find_eop_task(
                base_url, api_key, constructors_folder, eop_number
            )
            eop_task_due = (
                _task_date_part((eop_task.get("dates") or {}).get("due"))
                if eop_task is not None
                else None
            )
            eop_default = eop_task_due
            eop_key = (
                f"daily_eop_{selected_project_id}_"
                f"{eop_default.isoformat() if eop_default else 'none'}"
            )

            def _on_eop_change(
                key: str,
                old_value: Optional[date],
                number: str,
            ) -> None:
                new_value = st.session_state.get(key)
                if new_value is None or new_value == old_value:
                    return
                sync_errors, comment_errors = set_eop_date(
                    base_url,
                    api_key,
                    constructors_folder,
                    number,
                    new_value,
                    old_due=old_value,
                    in_progress_status_id=in_progress_status_id,
                    todo_status_id=todo_status_id,
                )
                if sync_errors:
                    st.session_state[key] = old_value
                    st.session_state["eop_flash"] = (
                        "Nie udało się zapisać EOP: "
                        + " | ".join(sync_errors),
                        "warning",
                    )
                elif comment_errors:
                    st.session_state["eop_flash"] = (
                        f"Zapisano EOP {new_value:%d.%m.%Y}. "
                        "Nie udało się dodać komentarzy w taskach "
                        "konstruktorów: "
                        + " | ".join(comment_errors),
                        "warning",
                    )
                else:
                    st.session_state["eop_flash"] = (
                        f"Zapisano EOP {new_value:%d.%m.%Y} "
                        "w niezależnym tasku EOP.",
                        "success",
                    )

            range_col, eop_col = st.columns(2)
            with range_col:
                date_range = st.date_input(
                    "Zakres dat",
                    min_value=min_date,
                    max_value=max_date,
                    key="daily_effort_range",
                )
            with eop_col:
                st.date_input(
                    "EOP",
                    value=eop_default,
                    key=eop_key,
                    on_change=_on_eop_change,
                    args=(
                        eop_key,
                        eop_default,
                        eop_number,
                    ),
                    help=(
                        "Data EOP czytana wyłącznie z taska "
                        "[PLAN] {numer} [EOP] w folderze KONSTRUKTORZY. "
                        "Task EOP jest niezależny od milestone "
                        "'Mechanical Design Ready'."
                    ),
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
                workdays_in_range = int(pd.bdate_range(start_filter, end_filter).size)

                assignments = st.session_state["constructor_assignments"]
                off_days = build_off_days(
                    st.session_state.get("holidays", set()),
                    st.session_state.get("vacations", {}),
                    st.session_state.get("constructors", []),
                )
                expanded_assignments = expand_assignment_days(
                    assignments, start_filter, end_filter, off_days
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

                selected_project_ids = set(selected_projects)
                if (
                    core_df is not None
                    and not core_df.empty
                    and "is_top_level" in core_df.columns
                ):
                    top_core_df = core_df[core_df["is_top_level"] == True]
                else:
                    top_core_df = core_df
                full_plan_h = (
                    float(
                        pd.to_numeric(
                            core_df.loc[
                                core_df["project_id"].isin(selected_project_ids),
                                "daily_planned_hours",
                            ],
                            errors="coerce",
                        )
                        .fillna(0.0)
                        .sum()
                    )
                    if core_df is not None
                    and not core_df.empty
                    and "daily_planned_hours" in core_df.columns
                    and "project_id" in core_df.columns
                    else 0.0
                )
                full_assigned_project_h = float(
                    sum(
                        _assignment_total_hours(a)
                        for a in assignments
                        if a.get("kind") != "external"
                        and str(a.get("project_id") or "")
                        in selected_project_ids
                    )
                )
                full_assigned_external_h = float(
                    sum(
                        _assignment_total_hours(a)
                        for a in assignments
                        if a.get("kind") == "external"
                    )
                )
                average_daily_h = total_h / workdays_in_range if workdays_in_range else 0.0
                plan_display_h = int(round(full_plan_h))
                assigned_display_h = int(round(full_assigned_project_h))
                a_adjustment = int(summary.get("completion_savings_hours", 0))
                b_completed = int(summary.get("completion_overrun_hours", 0))
                b_unfinished = int(summary.get("unfinished_overrun_hours", 0))
                b_adjustment = b_completed + b_unfinished
                remaining_display_h = (
                    plan_display_h
                    - assigned_display_h
                    + a_adjustment
                    + b_adjustment
                )

                remaining_plan_h = max(
                    0,
                    int(
                        round(
                            float(summary.get("planned_hours", 0))
                            - float(summary.get("used_total_hours", 0))
                        )
                    ),
                )
                end_date = eop_default if isinstance(eop_default, date) else max_date
                if isinstance(end_date, datetime):
                    end_date = end_date.date()
                future_start = date.today()
                future_end = (
                    end_date
                    if end_date is not None and end_date >= future_start
                    else future_start
                )
                future_assignments = expand_assignment_days(
                    assignments, future_start, future_end, off_days
                )
                if future_assignments.empty:
                    future_project_assignments = future_assignments
                else:
                    future_project_assignments = future_assignments[
                        (future_assignments["kind"] == "project")
                        & (
                            future_assignments["project_id"].isin(
                                selected_projects
                            )
                        )
                    ]
                assigned_future_project_h = (
                    float(future_project_assignments["hours"].sum())
                    if not future_project_assignments.empty
                    else 0.0
                )
                assigned_future_display_h = int(round(assigned_future_project_h))
                remaining_capacity_h = (
                    remaining_plan_h - assigned_future_display_h
                )

                def _signed_hours(value: int) -> str:
                    return f"{value:+d}"

                first_equation = (
                    f"{plan_display_h} - {assigned_display_h} "
                    f"{_signed_hours(a_adjustment)} {_signed_hours(b_adjustment)} "
                    f"= {_signed_hours(remaining_display_h)}"
                )
                second_equation = (
                    f"{remaining_plan_h} - {assigned_future_display_h} "
                    f"= {_signed_hours(remaining_capacity_h)}"
                )

                c1, c2 = st.columns([4, 1])
                c1.metric(
                    "Plan - przypisane + A + B = pozostało",
                    first_equation,
                    help=(
                        "Pełny plan (planned effort) minus wszystkie godziny "
                        "przypisane do wybranych projektów, bez filtra zakresu dat. "
                        "A to ujemna/zerowa korekta planu względem ALLOCATED po "
                        "zakończeniu zadania. B to dodatnia/zerowa korekta: dla "
                        "ukończonych Used ponad ALLOCATED, a dla niezakończonych "
                        "Used ponad Plan. A i B nie są niwelowane. Wynik ujemny "
                        "oznacza, że przypisano więcej godzin niż plan."
                    ),
                )
                c1.metric(
                    "Pozostały plan - przypisane = pozostało",
                    second_equation,
                    help=(
                        "Pozostały plan = Plan - Used (całość), czyli plan "
                        "pomniejszony o wszystkie dzienne przydziały, również "
                        "przyszłe. Przypisane to "
                        "godziny przypisane do wybranych projektów od dziś do końca "
                        "planu — nie są wliczane inne projekty ani zadania zewnętrzne. "
                        "Wynik dodatni oznacza godziny pozostałe do przypisania."
                    ),
                )
                c2.metric("Dni roboczych", n_dates)
                st.caption(
                    f"A = {_signed_hours(a_adjustment)} h (oszczędności planu vs "
                    "ALLOCATED). "
                    f"B = {_signed_hours(b_adjustment)} h "
                    f"(ukończone: {_signed_hours(b_completed)} h, "
                    f"niezakończone: {_signed_hours(b_unfinished)} h). "
                    "A i B są pokazane osobno, bez wzajemnego niwelowania."
                )
                if end_date is not None:
                    st.caption(
                        f"Pozostały plan: {remaining_plan_h} h "
                        f"(Plan {int(summary.get('planned_hours', 0))} - Used "
                        f"całość {int(summary.get('used_total_hours', 0))}). "
                        f"Przypisane do wybranych projektów od dziś do "
                        f"{end_date.strftime('%d.%m.%Y')}: "
                        f"{assigned_future_display_h} h."
                    )
                if remaining_capacity_h > 0:
                    st.caption(
                        f"Pozostało do przypisania: {remaining_capacity_h} h."
                    )
                elif remaining_capacity_h < 0:
                    st.caption(
                        f"Przypisano więcej niż wynosi pozostały plan o "
                        f"{abs(remaining_capacity_h)} h."
                    )
                if remaining_display_h < 0:
                    st.caption(
                        f"Przypisano więcej niż plan o "
                        f"{abs(remaining_display_h)} h."
                    )
                st.caption(
                    f"Przypisania łącznie: projekty "
                    f"{full_assigned_project_h:.1f} h, inne zadania "
                    f"{full_assigned_external_h:.1f} h."
                )
                if full_assigned_project_h <= 0 and full_assigned_external_h <= 0:
                    st.caption("Brak przypisań konstruktorów.")
                elif full_assigned_project_h <= 0:
                    st.caption(
                        "Brak przypisań do wybranych projektów — "
                        "są tylko inne zadania."
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

        render_resource_planning_section(
            project_map=project_map,
            constructors=st.session_state.get("constructors", []),
            effective_daily_hours=float(
                st.session_state.get("effective_daily_hours", 6.0)
            ),
            base_url=base_url,
            api_key=api_key,
            internal_folder=internal_folder,
            constructors_folder=constructors_folder,
            in_progress_status_id=in_progress_status_id,
            todo_status_id=todo_status_id,
            completed_status_id=constructor_completed_status_id,
            core_df=core_df,
            default_project_id=(
                selected_projects[0] if selected_projects else None
            ),
            core_task_type=core_task_type,
            bom_task_type=bom_task_type,
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
        ("revisions_allocated_hours", "Poprawki [h]"),
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

    # ---- KPI recalc ----
    task_df_kpi = task_df
    bom_df_kpi = bom_df
    planned_h = summary["planned_hours"]
    alloc_h = summary["allocated_hours"]
    used_h = summary.get("used_until_yesterday", 0)
    planned_missing = summary["planned_missing"]
    ratio_total = round(alloc_h / planned_h * 100) if planned_h > 0 else None

    # KPI 1: average delivery-time score for BOM items due today or earlier.
    # Completed items use their completion date; open overdue items are scored
    # as if they were closed today, so the delay penalty grows day by day.
    if not bom_df_kpi.empty:
        due_past = (
            bom_df_kpi[bom_df_kpi["due_today_or_past"] == True]
            if "due_today_or_past" in bom_df_kpi
            else pd.DataFrame()
        )
    else:
        due_past = pd.DataFrame()
    kpi1_today = date.today()
    kpi1_scores = []
    for _, row in due_past.iterrows():
        score = compute_kpi1_score(
            start_date=row.get("start_date"),
            due_date=row.get("due_date"),
            completed=bool(row.get("completed", False)),
            completed_date=row.get("completed_date"),
            tolerance_pct=kpi1_tolerance_pct,
            as_of=kpi1_today,
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
        revisions_h = summary.get("revisions_allocated_hours", 0)
        allocated_with_revisions_h = summary.get(
            "allocated_with_revisions_hours", alloc_h + revisions_h
        )
        revisions_share_pct = summary.get("revisions_share_pct", 0.0)
        st.metric(
            "Poprawki (h)",
            revisions_h,
            help="Suma Allocated z tasków typu REV Task. Poprawki nie obciążają "
            "zadań nadrzędnych i nie są wliczane do Allocated (h).",
        )
        st.metric(
            "Allocated łącznie (h)",
            allocated_with_revisions_h,
            help="Allocated bez poprawek + Poprawki.",
        )
        if allocated_with_revisions_h > 0:
            st.metric(
                "Udział poprawek",
                f"{revisions_share_pct:.1f}%",
                help=(
                    f"{revisions_h} h poprawek / {allocated_with_revisions_h} h "
                    "allocated łącznie."
                ),
            )
        st.metric("Used until yesterday (h)", used_h)
        if ratio_total is not None:
            st.metric("Alloc / Planned", f"{ratio_total}%")
        if summary["time_progress_avg"]:
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
        "- **KPI 1 — terminowość BOM:** średnia ocen pozycji BOM, których termin już minął "
        "(ukończonych lub wciąż otwartych). Ocena wynosi 1 do końca planowanego czasu "
        "(od startu do terminu), potem liniowo spada zgodnie z tolerancją X ustawioną "
        "w panelu i osiąga 0 po jej przekroczeniu. Otwarte pozycje po terminie oceniamy "
        "tak, jakby zakończyły się dziś, więc ich ocena maleje z każdym dniem opóźnienia; "
        "otwarte pozycje w terminie oraz pozycje bez wymaganych dat są pomijane.\n"
        "- **KPI 2 — Used / Plan:** suma „Used until yesterday” podzielona przez sumę planu "
        "dla ukończonych pozycji BOM. To ważony wskaźnik: większy plan ma proporcjonalnie "
        "większy wpływ. „Used” pochodzi z dziennych przydziałów Wrike do wczoraj, nie z "
        "potwierdzonych wpisów czasu pracy.\n"
        "- **Alloc / Planned:** suma przydzielonych godzin (Allocated) podzielona przez sumę "
        "znanych godzin planu dla Core Items i BOM. Allocated i Used są liczone z wierszy "
        "top-level, a Planned jest sumą zdeduplikowanego planu, żeby nie podwajać godzin "
        "zadań nadrzędnych i podrzędnych; poprawki (REV Task) nie są wliczane do Allocated.\n"
        "- **Poprawki / udział poprawek:** suma Allocated z tasków typu REV Task jest "
        "pokazana osobno. Udział poprawek = Poprawki / (Allocated + Poprawki).\n"
        "- **Ścieżka czasu (avg):** średnia liczona jako upływ czasu od startu do ukończenia "
        "(albo do dziś, jeśli brak daty ukończenia) podzielony przez kalendarzowy czas od "
        "startu do terminu. Może przekroczyć 100%, jeśli minął termin; pokazuje się tylko "
        "bez aktywnego filtra zakresu dat."
    )

if __name__ == "__main__":
    main()
