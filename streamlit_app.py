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
"""

from __future__ import annotations

import json
import os
from collections import defaultdict
from datetime import date, datetime, timedelta
from functools import lru_cache
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

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
DEFAULT_CORE_PROJECT_TYPE = os.getenv("WRIKE_CORE_PROJECT_TYPE_ID", "IEAGWGLXPIAHEHH3")
DEFAULT_PLANNED_FIELD_ID = os.getenv("WRIKE_PLANNED_EFFORT_FIELD_ID", "IEAGWGLXJUALG3VY")
DEFAULT_COMPLETED_STATUS_ID = os.getenv("WRIKE_COMPLETED_STATUS_ID", "IEAGWGLXJMGYX4ND")
SKIPPED_CORE_PROJECT_TITLES = {"3. Mechanical Design"}


# ---- Logging ---------------------------------------------------------------
def _init_logs() -> None:
    if "logs" not in st.session_state:
        st.session_state["logs"] = []


def log(msg: str) -> None:
    _init_logs()
    st.session_state["logs"].append(msg)


def reset_logs() -> None:
    st.session_state["logs"] = []


# ---- Helpers ---------------------------------------------------------------
def iso_to_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        # Wrike returns without timezone sometimes
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
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


def is_completed(task: Dict[str, Any], completed_status_id: str) -> bool:
    return task.get("customStatusId") == completed_status_id


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


def render_df(
    df: pd.DataFrame,
    cols: list[str | tuple[str, str]],
    column_config: Optional[Dict[str, Any]] = None,
) -> None:
    """Render dataframe with safe fallback if Styler blows up."""
    df_local = df.copy()
    for bool_col in ["due_today_or_past", "completed"]:
        if bool_col in df_local:
            df_local[bool_col] = df_local[bool_col].map(bool_symbol)
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
                label_config[column] = st.column_config.TextColumn(label)
        final_config = {**final_config, **label_config}
        st.dataframe(
            df_local[ordered_cols]
            .style.applymap(color_ratio, subset=["alloc_vs_plan_pct"])
            .applymap(color_time, subset=["time_progress_pct"])
            .format(fmt),
            use_container_width=True,
            column_config=final_config,
        )
    except Exception as exc:  # noqa: BLE001
        log(f"Styler failed, showing plain dataframe. Error: {exc}")
        st.dataframe(
            df_local[ordered_cols],
            use_container_width=True,
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


@st.cache_data(ttl=300, show_spinner=False)
def fetch_client_projects(
    base_url: str, api_key: str, client_folder_id: str
) -> List[Dict[str, Any]]:
    params = {"project": "true", "descendants": "false"}
    data = api_get(base_url, api_key, f"folders/{client_folder_id}/folders", params=params)
    return data.get("data", [])


@st.cache_data(ttl=300, show_spinner=False)
def fetch_tasks_for_project(
    base_url: str, api_key: str, project_id: str
) -> List[Dict[str, Any]]:
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

    return tasks


@st.cache_data(ttl=300, show_spinner=False)
def fetch_projects_with_customfields(
    base_url: str, api_key: str, project_id: str
) -> List[Dict[str, Any]]:
    """Fetch all descendant projects (including core projects) with customFields."""
    params: Dict[str, Any] = {
        "descendants": "true",
        "project": "true",
        "fields": json.dumps(["customFields"]),
        "pageSize": 1000,
    }
    items: List[Dict[str, Any]] = []
    next_token: Optional[str] = None
    page_count = 0

    while True:
        page_params = dict(params)
        if next_token:
            page_params["nextPageToken"] = next_token
        try:
            data = api_get(base_url, api_key, f"folders/{project_id}/folders", params=page_params)
        except RuntimeError as exc:
            if "nextPageToken" in str(exc):
                log(f"Invalid nextPageToken for projects; stopping pagination. Error: {exc}")
                break
            raise

        page_items = data.get("data", [])
        items.extend(page_items)
        next_token = data.get("nextPageToken") or None
        page_count += 1
        log(
            f"Fetched projects page size={len(page_items)}, total={len(items)}, "
            f"nextPageToken={next_token}"
        )
        if not next_token or page_count >= 10:
            if page_count >= 10:
                log("Stopped projects pagination after 10 pages (safety limit).")
            break
    return items


@st.cache_data(ttl=300, show_spinner=False)
def fetch_core_project_tasks(
    base_url: str, api_key: str, core_project_id: str
) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {
        "descendants": "true",
        "fields": json.dumps(["effortAllocation"]),
        "pageSize": 1000,
        "subTasks": "true",
    }
    items: List[Dict[str, Any]] = []
    next_token: Optional[str] = None

    while True:
        page_params = dict(params)
        if next_token:
            page_params["nextPageToken"] = next_token
        data = api_get(base_url, api_key, f"folders/{core_project_id}/tasks", params=page_params)
        page_items = data.get("data", [])
        items.extend(page_items)
        next_token = data.get("nextPageToken") or None
        if not next_token:
            break
    return items


def clear_wrike_cache() -> None:
    fetch_client_projects.clear()
    fetch_tasks_for_project.clear()
    fetch_projects_with_customfields.clear()
    fetch_core_project_tasks.clear()


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
    core_project_type: str,
    skip_project_titles: Optional[Set[str]] = None,
) -> Tuple[
    Callable[[str], Optional[str]],
    Callable[[str], Optional[str]],
]:

    skip_titles = skip_project_titles or set()

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
    def nearest_core_project(task_id: str) -> Optional[str]:
        task = tasks_by_id.get(task_id)
        if not task:
            return None
        core_override = task.get("_core_project_id")
        if core_override:
            return core_override
        if task.get("customItemTypeId") == core_project_type and task.get("title") not in skip_titles:
            return task_id
        for parent_id in parent_ids(task):
            found = nearest_core_project(parent_id)
            if found:
                return found
        return None

    return nearest_core_task, nearest_core_project


def aggregate_core_items(
    tasks: List[Dict[str, Any]],
    core_task_type: str,
    core_project_type: str,
    planned_field_id: str,
    completed_status_id: str,
    project_lookup: Optional[Dict[str, str]] = None,
    allowed_project_ids: Optional[Set[str]] = None,
    extra_alloc_by_project: Optional[Dict[str, int]] = None,
    extra_used_by_project: Optional[Dict[str, int]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    tasks_by_id, _ = build_indexes(tasks)
    nearest_core_task, nearest_core_project = make_nearest_core_resolvers(
        tasks_by_id,
        core_task_type,
        core_project_type,
        skip_project_titles=SKIPPED_CORE_PROJECT_TITLES,
    )

    today = date.today()
    core_task_rows = []
    core_project_rows = []

    # Pre-compute allocated minutes per task
    alloc_by_task = {tid: allocated_minutes(task) for tid, task in tasks_by_id.items()}
    cutoff_date = date.today() - timedelta(days=1)
    used_minutes_by_task = {tid: effort_minutes_until(task, cutoff_date) for tid, task in tasks_by_id.items()}
    planned_seen = 0
    customfields_seen = 0
    planned_projects_sum = 0  # planned effort z projektów (core i zwykłych)

    lookup = project_lookup or {}
    extra_alloc_by_project = extra_alloc_by_project or {}
    extra_used_by_project = extra_used_by_project or {}

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
        planned_hours = extract_planned_hours(task, planned_field_id)
        if planned_hours is not None:
            planned_hours = int(round(planned_hours))
        if task.get("customFields"):
            customfields_seen += 1
        if planned_hours is not None:
            planned_seen += 1
        is_project = bool(task.get("project") or task.get("scope") == "WsProject")
        project_id, project_title = resolve_client_project(task)
        task_title = task.get("title")
        if allowed_project_ids is not None and (not project_id or project_id not in allowed_project_ids):
            continue
        is_skipped_project = ctype == core_project_type and task_title in SKIPPED_CORE_PROJECT_TITLES
        if is_skipped_project:
            continue
        if is_project and planned_hours:
            planned_projects_sum += planned_hours
        if ctype not in {core_task_type, core_project_type}:
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
        time_progress = None
        if start and due and due > start and start <= today:
            span = (due - start).days
            if span > 0:
                time_progress = int(round(((today - start).days / span) * 100))

        item_type_label = "Core task" if ctype == core_task_type else "Core project"
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
            "warnings": [],
        }
        if planned_hours is None:
            base_row["warnings"].append("Brak Planned effort")

        if ctype == core_task_type:
            alloc_minutes = sum_alloc(nearest_core_task, tid, include_self=True)
            base_row["allocated_hours"] = round(alloc_minutes / 60)
            used_minutes = sum_used(nearest_core_task, tid, include_self=True)
            base_row["used_hours_until_yesterday"] = minutes_to_hours(used_minutes)
            base_row["type"] = item_type_label
            core_task_rows.append(base_row)
        else:
            extra_alloc = extra_alloc_by_project.get(tid, 0)
            alloc_minutes = sum_alloc(nearest_core_project, tid, include_self=False) + extra_alloc
            base_row["allocated_hours"] = round(alloc_minutes / 60)
            extra_used = extra_used_by_project.get(tid, 0)
            used_minutes = sum_used(nearest_core_project, tid, include_self=False) + extra_used
            base_row["used_hours_until_yesterday"] = minutes_to_hours(used_minutes)
            base_row["type"] = item_type_label
            core_project_rows.append(base_row)

    task_df = pd.DataFrame(core_task_rows)
    project_df = pd.DataFrame(core_project_rows)

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
    project_df = add_ratio(project_df)

    if not task_df.empty and "title" in task_df:
        task_df = task_df.sort_values("title")
    if not project_df.empty and "title" in project_df:
        project_df = project_df.sort_values("title")

    # Unified core items view
    core_items_df = pd.concat([project_df, task_df], ignore_index=True) if (not project_df.empty or not task_df.empty) else pd.DataFrame()

    # KPI summary
    def completion_stats(df: pd.DataFrame) -> Tuple[int, int]:
        if df.empty:
            return 0, 0
        due_df = df[df["due_today_or_past"] == True]
        return int(due_df["completed"].sum()), int(len(due_df))

    completed, due_total = completion_stats(pd.concat([task_df, project_df], ignore_index=True))
    planned_total = pd.concat(
        [task_df.get("planned_hours", pd.Series(dtype=float)), project_df.get("planned_hours", pd.Series(dtype=float))],
        ignore_index=True,
    )
    alloc_total = pd.concat(
        [task_df.get("allocated_hours", pd.Series(dtype=float)), project_df.get("allocated_hours", pd.Series(dtype=float))],
        ignore_index=True,
    )
    used_series = pd.concat(
        [
            task_df.get("used_hours_until_yesterday", pd.Series(dtype=float)),
            project_df.get("used_hours_until_yesterday", pd.Series(dtype=float)),
        ],
        ignore_index=True,
    )
    used_clean = used_series.dropna()
    used_sum = int(used_clean.sum()) if not used_clean.empty else 0

    summary = {
        "allocated_hours": int(round(alloc_total.sum())),
        "planned_hours": int(round(planned_projects_sum)),
        "planned_missing": int(planned_total.isna().sum()),
        "completed_due": completed,
        "due_total": due_total,
        "planned_seen": planned_seen,
        "customfields_seen": customfields_seen,
        "time_progress_avg": int(round(
            pd.concat(
                [
                    task_df.get("time_progress_pct", pd.Series(dtype=float)),
                    project_df.get("time_progress_pct", pd.Series(dtype=float)),
                ],
                ignore_index=True,
            ).dropna().mean()
            if not pd.concat(
                [
                    task_df.get("time_progress_pct", pd.Series(dtype=float)),
                    project_df.get("time_progress_pct", pd.Series(dtype=float)),
                ],
                ignore_index=True,
            ).dropna().empty
            else 0,
        )),
        "used_until_yesterday": used_sum,
    }

    return project_df, task_df, summary


def build_tree_view(
    tasks: List[Dict[str, Any]],
    core_task_type: str,
    core_project_type: str,
) -> List[Dict[str, Any]]:
    tasks_by_id, children = build_indexes(tasks)
    nearest_core_task, nearest_core_project = make_nearest_core_resolvers(
        tasks_by_id,
        core_task_type,
        core_project_type,
        skip_project_titles=SKIPPED_CORE_PROJECT_TITLES,
    )

    def label_with_link(item_id: str, role: Optional[str] = None) -> str:
        item = tasks_by_id.get(item_id, {})
        title = item.get("title") or "(bez tytułu)"
        label = f"{role}: {title}" if role else title
        permalink = item.get("permalink")
        if permalink:
            return f"[{label}]({permalink})"
        return label

    # Build grouping from core project -> core task -> other descendants (for inspection)
    tree: Dict[str, Dict[str, Any]] = {}
    for tid, task in tasks_by_id.items():
        cp = nearest_core_project(tid)
        ct = nearest_core_task(tid)
        if not cp and not ct:
            continue  # outside of monitored items

        if cp and tasks_by_id.get(cp, {}).get("title") in SKIPPED_CORE_PROJECT_TITLES:
            cp = None
        if cp:
            node = tree.setdefault(
                cp,
                {
                    "id": cp,
                    "title": tasks_by_id.get(cp, {}).get("title"),
                    "permalink": tasks_by_id.get(cp, {}).get("permalink"),
                    "core_tasks": defaultdict(list),
                    "loose_tasks": [],
                },
            )
            if ct and ct != cp:
                node["core_tasks"][ct].append(tid)
            elif tid != cp:
                node["loose_tasks"].append(tid)
        elif ct:
            # Task belongs to core task but not within a core project chain
            node = tree.setdefault(
                f"__orphan_ct_{ct}",
                {"id": ct, "title": tasks_by_id.get(ct, {}).get("title"), "core_tasks": defaultdict(list), "loose_tasks": []},
            )
            if tid != ct:
                node["core_tasks"][ct].append(tid)

    readable = []
    for cp_id, node in tree.items():
        entry = {
            "core_project": label_with_link(cp_id, role="Core item"),
            "core_tasks": [],
            "other_tasks": [],
        }
        for ct_id, task_ids in node["core_tasks"].items():
            entry["core_tasks"].append(
                {
                    "core_task": label_with_link(ct_id, role="Core item"),
                    "tasks": [label_with_link(t) for t in task_ids if t in tasks_by_id],
                }
            )
        entry["other_tasks"] = [label_with_link(t) for t in node["loose_tasks"] if t in tasks_by_id]
        readable.append(entry)
    return readable


# ---- UI --------------------------------------------------------------------
def main() -> None:
    st.set_page_config(page_title="Wrike KPI – Core Items", layout="wide")
    reset_logs()
    st.title("Design Department KPI Board")

    with st.sidebar:
        st.header("Konfiguracja")
        base_url = st.text_input("Wrike API base URL", value=DEFAULT_BASE_URL)
        api_key = st.text_input("API key", value=DEFAULT_API_KEY or "", type="password")
        client_folder = st.text_input("Folder ID z projektami klienckimi", value=DEFAULT_CLIENT_FOLDER)
        core_project_type = st.text_input("CustomItemTypeId – Core project", value=DEFAULT_CORE_PROJECT_TYPE)
        core_task_type = st.text_input("CustomItemTypeId – Core task", value=DEFAULT_CORE_TASK_TYPE)
        planned_field_id = st.text_input("CustomFieldId – Planned effort (godz.)", value=DEFAULT_PLANNED_FIELD_ID)
        completed_status_id = st.text_input("CustomStatusId – status Completed", value=DEFAULT_COMPLETED_STATUS_ID)
        st.caption("Parametry są też ładowane z .env; tu możesz je nadpisać.")
        if "refresh_timestamp" not in st.session_state:
            st.session_state["refresh_timestamp"] = None
        if st.button("Odśwież dane z Wrike", key="refresh"):
            clear_wrike_cache()
            st.session_state["refresh_timestamp"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        if st.session_state["refresh_timestamp"]:
            st.caption(f"Ostatnie odświeżenie: {st.session_state['refresh_timestamp']}")

    if not api_key or not client_folder:
        st.warning("Uzupełnij API key i ID folderu z projektami, aby pobrać dane.")
        return

    # Step 1: projekty klienckie
    try:
        projects = fetch_client_projects(base_url, api_key, client_folder)
    except Exception as exc:  # noqa: BLE001
        st.error(str(exc))
        return

    project_map = {p["id"]: p.get("title", p["id"]) for p in projects}
    if not project_map:
        st.info("Brak dostępnych projektów w zdefiniowanym folderze.")
        return
    default_selection = [next(iter(project_map.keys()))]
    selected_projects = st.multiselect(
        "Wybierz projekt(y):",
        options=list(project_map.keys()),
        default=default_selection,
        format_func=lambda pid: project_map[pid],
    )

    if not selected_projects:
        st.info("Zaznacz przynajmniej jeden projekt, aby zobaczyć KPI.")
        return

    # Step 2: taski projektu
    cutoff_date = date.today() - timedelta(days=1)
    extra_alloc_by_project: Dict[str, int] = defaultdict(int)
    extra_used_by_project: Dict[str, int] = defaultdict(int)
    cutoff_date = date.today() - timedelta(days=1)
    core_project_ids: Set[str] = set()
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
                    if task.get("customItemTypeId") == core_project_type:
                        core_project_ids.add(tid)
                    task["_selected_project_id"] = pid
                    tasks.append(task)
                    existing_ids.add(tid)
                project_items = fetch_projects_with_customfields(base_url, api_key, pid)
                for item in project_items:
                    item.setdefault("customItemTypeId", core_project_type)
                    item.setdefault("entityTypeId", "WsProject")
                    tid = item.get("id")
                    if not tid or tid in existing_ids:
                        continue
                    core_project_ids.add(tid)
                    item["_selected_project_id"] = pid
                    tasks.append(item)
                    existing_ids.add(tid)
            for cp_id in core_project_ids:
                extra_tasks = fetch_core_project_tasks(base_url, api_key, cp_id)
                for task in extra_tasks:
                    tid = task.get("id")
                    if not tid:
                        continue
                    extra_alloc_by_project[cp_id] += allocated_minutes(task)
                    extra_used_by_project[cp_id] += effort_minutes_until(task, cutoff_date)
        except Exception as exc:  # noqa: BLE001
            st.error(str(exc))
            return

    if not tasks:
        st.info("Brak tasków w wybranych projektach.")
        return

    # Step 3-5: KPI + drzewo
    project_df, task_df, summary = aggregate_core_items(
        tasks,
        core_task_type=core_task_type,
        core_project_type=core_project_type,
        planned_field_id=planned_field_id,
        completed_status_id=completed_status_id,
        project_lookup=project_map,
        allowed_project_ids=set(selected_projects),
        extra_alloc_by_project=extra_alloc_by_project,
        extra_used_by_project=extra_used_by_project,
    )
    core_cols = [
        ("type", "Type"),
        ("project", "Project"),
        ("title", "Title"),
        ("allocated_hours", "Alloc [h]"),
        ("used_hours_until_yesterday", "Used until yesterday (h)"),
        ("planned_hours", "Plan [h]"),
        ("alloc_vs_plan_pct", "Alloc vs plan %"),
        ("time_progress_pct", "Time progress %"),
        ("due_date", "Due date"),
        ("start_date", "Start date"),
        ("completed", "Completed"),
        ("due_today_or_past", "Due today/past"),
        ("permalink", "Permalink"),
        ("warnings", "Warnings"),
    ]

    def render_kpi_block(summary: Dict[str, Any], completion_label: str) -> None:
        col1, col2, col3 = st.columns(3)
        ratio_total_inner = (
            round(summary["allocated_hours"] / summary["planned_hours"] * 100, 1)
            if summary["planned_hours"] > 0
            else None
        )
        completion_pct_inner = (
            0
            if summary["due_total"] == 0
            else int(round((summary["completed_due"] / summary["due_total"]) * 100))
        )
        with col1:
            st.metric("Allocated (h)", summary["allocated_hours"])
            st.metric("Used until yesterday (h)", summary.get("used_until_yesterday", 0.0))
            if ratio_total_inner is not None:
                st.metric("Alloc / Planned", f"{ratio_total_inner}%")
            if summary["time_progress_avg"]:
                st.metric("Ścieżka czasu (avg)", f"{summary['time_progress_avg']}%")
        col2.metric("Planned (h)", summary["planned_hours"], delta=f"braki: {summary['planned_missing']}")
        col3.metric(
            completion_label,
            f"{summary['completed_due']} / {summary['due_total']}",
            delta=f"{completion_pct_inner}%",
        )

    render_kpi_block(summary, "Zakończone / Planowane")
    st.caption(
        f"Debug planned effort: customFields zaczytane dla {summary['customfields_seen']} core items; "
        f"planned effort znaleziono w {summary['planned_seen']}."
    )
    st.subheader("Core Items (global)")
    render_df(
        pd.concat(
            [project_df, task_df], ignore_index=True
        )
        if not project_df.empty or not task_df.empty
        else pd.DataFrame(),
        core_cols,
        column_config={"permalink": st.column_config.LinkColumn("Link", display_text="otwórz")},
    )
    with st.expander("Log (debug)"):
        _init_logs()
        st.write("\n".join(st.session_state["logs"]))

    def color_ratio(val):
        if pd.isna(val):
            return ""
        if val <= 80:
            return "background-color:#2e7d32; color:#ffffff"  # dark green, white text
        if val <= 110:
            return "background-color:#f9a825; color:#000000"  # amber, black text
        return "background-color:#c62828; color:#ffffff"      # dark red, white text

    def color_time(val):
        if pd.isna(val):
            return ""
        if val <= 60:
            return "background-color:#2e7d32; color:#ffffff"  # green
        if val <= 100:
            return "background-color:#f9a825; color:#000000"  # amber
        return "background-color:#c62828; color:#ffffff"      # red (przekroczony harmonogram)

    def bool_symbol(val: Any) -> str:
        if val is True:
            return "✅"
        if val is False:
            return "🔴"
        return ""

if __name__ == "__main__":
    main()
