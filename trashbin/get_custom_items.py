#!/usr/bin/env python3
"""
Wrike Custom Items Fetcher
Fetches custom items (tasks/projects) by custom item type ID and extracts all available data.
"""

import os
import json
import sys
import argparse
import requests
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the API key from environment variables
API_KEY = os.getenv('WRIKE_API_KEY')
# Allow overriding base URL; default to EU cluster
BASE_URL = os.getenv('WRIKE_BASE_URL', "https://app-eu.wrike.com/api/v4")

# Custom item type IDs from the requirements
CORE_TASK_TYPE_ID = "IEAGWGLXPIAHEHEZ"  # Core tasks custom item type
CORE_PROJECT_TYPE_ID = "IEAGWGLXPIAHEHH3"  # Core projects custom item type

# Custom field ID for planned effort
PLANNED_EFFORT_FIELD_ID = "IEAGWGLXJUALG3VY"

# Default field set we want back for every item/subitem
# Default field sets; exclude fields the API rejects (e.g., "dates")
DEFAULT_TASK_FIELDS = [
    "customItemTypeId",
    "customFields",  # potrzebne, żeby odczytać IEAGWGLXJUALG3VY
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
]

SAFE_TASK_FIELDS = [
    "customFields",
    "effortAllocation",
    "subTaskIds",
    "customItemTypeId",
]

DEFAULT_FOLDER_FIELDS = [
    "customItemTypeId",
    "customFields",
    "permalink",
]

SAFE_FOLDER_FIELDS = [
    "customFields",
    "customItemTypeId",
]

MINIMAL_TASK_FIELDS = ["customItemTypeId"]
MINIMAL_FOLDER_FIELDS = ["customItemTypeId"]

def fetch_all_tasks(fields: Optional[List[str]] = None,
                    custom_item_type_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """
    Fetch all tasks from Wrike API with pagination.
    
    Args:
        fields: List of fields to request (None for default fields)
    
    Returns:
        List of task data dictionaries
    """
    url = f"{BASE_URL}/tasks"
    headers = {'Authorization': f'bearer {API_KEY}'}
    params = {'pageSize': 100}  # Max page size

    params_with_fields = params.copy()
    # Try to pre-filter by custom item type if provided (singular form)
    if custom_item_type_ids:
        params_with_fields['customItemTypeId'] = custom_item_type_ids[0]
    # Try several field sets progressively
    candidate_field_sets = []
    if fields:
        candidate_field_sets.append(fields)
    candidate_field_sets.append(SAFE_TASK_FIELDS)
    candidate_field_sets.append(MINIMAL_TASK_FIELDS)
    candidate_field_sets.append(None)  # no fields
    field_set_index = 0
    if candidate_field_sets[field_set_index]:
        params_with_fields['fields'] = ",".join(candidate_field_sets[field_set_index])

    all_tasks = []
    next_page_token = None

    try:
        while True:
            if next_page_token:
                params_with_fields['nextPageToken'] = next_page_token
                params.pop('nextPageToken', None)
            else:
                params_with_fields.pop('nextPageToken', None)
                params.pop('nextPageToken', None)

            response = requests.get(url, headers=headers, params=params_with_fields)

            if response.status_code == 400 and b"customitemtypeid" in response.content.lower():
                # Remove filter if not supported
                params_with_fields.pop('customItemTypeId', None)
                print("[INFO] Retrying task fetch without customItemTypeId filter.")
                continue

            if response.status_code == 400 and b"customitemtypeid" in response.content.lower():
                params_with_fields.pop('customItemTypeId', None)
                print("[INFO] Retrying folder fetch without customItemTypeId filter.")
                continue

            if response.status_code == 400 and b"fields" in response.content.lower():
                field_set_index += 1
                if field_set_index < len(candidate_field_sets):
                    next_fields = candidate_field_sets[field_set_index]
                    if next_fields:
                        params_with_fields['fields'] = ",".join(next_fields)
                        print("[INFO] Retrying task fetch with reduced field set.")
                    else:
                        params_with_fields.pop('fields', None)
                        print("[INFO] Retrying task fetch without fields (previous fields invalid).")
                    continue

            if response.status_code != 200:
                print(f"[ERROR] Failed to fetch tasks: {response.status_code}")
                print(f"[DEBUG] Response: {response.content}")
                break

            data = response.json()
            tasks = data.get('data', [])
            all_tasks.extend(tasks)

            next_page_token = data.get('nextPageToken')
            if not next_page_token:
                break

        return all_tasks

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Task fetch error: {e}")
        return []

def fetch_all_folders(fields: Optional[List[str]] = None,
                      custom_item_type_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """
    Fetch all folders from Wrike API with pagination.
    
    Args:
        fields: List of fields to request (None for default fields)
    
    Returns:
        List of folder data dictionaries
    """
    url = f"{BASE_URL}/folders"
    headers = {'Authorization': f'bearer {API_KEY}'}
    params = {}  # No pagination supported for folderTree

    params_with_fields = params.copy()
    candidate_field_sets = []
    if fields:
        candidate_field_sets.append(fields)
    candidate_field_sets.append(SAFE_FOLDER_FIELDS)
    candidate_field_sets.append(MINIMAL_FOLDER_FIELDS)
    candidate_field_sets.append(None)
    field_set_index = 0
    if candidate_field_sets[field_set_index]:
        params_with_fields['fields'] = ",".join(candidate_field_sets[field_set_index])

    try:
        while True:
            response = requests.get(url, headers=headers, params=params_with_fields)

            if response.status_code == 400 and b"fields" in response.content.lower():
                field_set_index += 1
                if field_set_index < len(candidate_field_sets):
                    next_fields = candidate_field_sets[field_set_index]
                    if next_fields:
                        params_with_fields['fields'] = ",".join(next_fields)
                        print("[INFO] Retrying folder fetch with reduced field set.")
                    else:
                        params_with_fields.pop('fields', None)
                        print("[INFO] Retrying folder fetch without fields (previous fields invalid).")
                    continue

            if response.status_code == 400 and b"pageSize" in response.content.lower():
                # Should not happen; we don't send pageSize now
                params_with_fields.pop('pageSize', None)
                continue

            if response.status_code != 200:
                print(f"[ERROR] Failed to fetch folders: {response.status_code}")
                print(f"[DEBUG] Response: {response.content}")
                break

            data = response.json()
            return data.get('data', [])

        return []

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Folder fetch error: {e}")
        return []

def filter_by_custom_item_type(items: List[Dict[str, Any]], custom_item_type_ids: List[str]) -> List[Dict[str, Any]]:
    """
    Filter items by custom item type ID.
    
    Args:
        items: List of items (tasks or folders)
        custom_item_type_ids: List of custom item type IDs to filter by
    
    Returns:
        Filtered list of items
    """
    filtered = []
    for item in items:
        if item.get('customItemTypeId') in custom_item_type_ids:
            filtered.append(item)
    return filtered

def fetch_subtasks_for_task(task_id: str, fields: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """
    Fetch subtasks for a task.
    
    Args:
        task_id: Task API ID
    
    Returns:
        List of subtask data dictionaries
    """
    url = f"{BASE_URL}/tasks/{task_id}/subtasks"
    headers = {'Authorization': f'bearer {API_KEY}'}
    params = {'pageSize': 100}

    params_with_fields = params.copy()
    candidate_field_sets = []
    if fields:
        candidate_field_sets.append(fields)
    candidate_field_sets.append(SAFE_TASK_FIELDS)
    candidate_field_sets.append(MINIMAL_TASK_FIELDS)
    candidate_field_sets.append(None)
    field_set_index = 0
    if candidate_field_sets[field_set_index]:
        params_with_fields['fields'] = ",".join(candidate_field_sets[field_set_index])

    all_subtasks = []
    next_page_token = None

    try:
        while True:
            if next_page_token:
                params_with_fields['nextPageToken'] = next_page_token
                params.pop('nextPageToken', None)
            else:
                params_with_fields.pop('nextPageToken', None)
                params.pop('nextPageToken', None)

            response = requests.get(url, headers=headers, params=params_with_fields)

            if response.status_code == 400 and b"fields" in response.content.lower():
                field_set_index += 1
                if field_set_index < len(candidate_field_sets):
                    next_fields = candidate_field_sets[field_set_index]
                    if next_fields:
                        params_with_fields['fields'] = ",".join(next_fields)
                        print(f"[INFO] Retrying subtask fetch for {task_id} with reduced field set.")
                    else:
                        params_with_fields.pop('fields', None)
                        print(f"[INFO] Retrying subtask fetch for {task_id} without fields (previous fields invalid).")
                    continue

            if response.status_code == 200:
                data = response.json()
                subtasks = data.get('data', [])
                all_subtasks.extend(subtasks)
            elif response.status_code == 400:
                # No subtasks endpoint or no subtasks available
                break
            else:
                print(f"[WARNING] Subtask fetch failed for task {task_id}: {response.status_code}")
                break

            next_page_token = data.get('nextPageToken')
            if not next_page_token:
                break

        return all_subtasks

    except requests.exceptions.RequestException as e:
        print(f"[INFO] Subtask fetch error for task {task_id}: {e}")
        return []

def fetch_subtasks_for_folder(folder_id: str, fields: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """
    Fetch tasks within a folder (as "subtasks" for folders).
    
    Args:
        folder_id: Folder API ID
    
    Returns:
        List of task data dictionaries within the folder
    """
    url = f"{BASE_URL}/folders/{folder_id}/tasks"
    headers = {'Authorization': f'bearer {API_KEY}'}
    params = {'pageSize': 100}

    params_with_fields = params.copy()
    candidate_field_sets = []
    if fields:
        candidate_field_sets.append(fields)
    candidate_field_sets.append(SAFE_TASK_FIELDS)
    candidate_field_sets.append(MINIMAL_TASK_FIELDS)
    candidate_field_sets.append(None)
    field_set_index = 0
    if candidate_field_sets[field_set_index]:
        params_with_fields['fields'] = ",".join(candidate_field_sets[field_set_index])

    all_tasks = []
    next_page_token = None

    try:
        while True:
            if next_page_token:
                params_with_fields['nextPageToken'] = next_page_token
                params.pop('nextPageToken', None)
            else:
                params_with_fields.pop('nextPageToken', None)
                params.pop('nextPageToken', None)

            response = requests.get(url, headers=headers, params=params_with_fields)

            if response.status_code == 400 and b"fields" in response.content.lower():
                field_set_index += 1
                if field_set_index < len(candidate_field_sets):
                    next_fields = candidate_field_sets[field_set_index]
                    if next_fields:
                        params_with_fields['fields'] = ",".join(next_fields)
                        print(f"[INFO] Retrying folder task fetch for {folder_id} with reduced field set.")
                    else:
                        params_with_fields.pop('fields', None)
                        print(f"[INFO] Retrying folder task fetch for {folder_id} without fields (previous fields invalid).")
                    continue

            if response.status_code == 200:
                data = response.json()
                tasks = data.get('data', [])
                all_tasks.extend(tasks)
            elif response.status_code == 400:
                # No tasks in folder
                break
            else:
                print(f"[WARNING] Task fetch failed for folder {folder_id}: {response.status_code}")
                break

            next_page_token = data.get('nextPageToken')
            if not next_page_token:
                break

        return all_tasks

    except requests.exceptions.RequestException as e:
        print(f"[INFO] Task fetch error for folder {folder_id}: {e}")
        return []

def fetch_tasks_for_project_folder(folder_id: str, fields: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """
    Fetch tasks within a project folder, including descendants and subtasks.
    Mirrors: /folders/{id}/tasks?descendants=true&subTasks=true
    """
    url = f"{BASE_URL}/folders/{folder_id}/tasks"
    headers = {'Authorization': f'bearer {API_KEY}'}
    params = {'descendants': True, 'subTasks': True, 'pageSize': 100}

    params_with_fields = params.copy()
    candidate_field_sets = []
    if fields:
        candidate_field_sets.append(fields)
    candidate_field_sets.append(SAFE_TASK_FIELDS)
    candidate_field_sets.append(MINIMAL_TASK_FIELDS)
    candidate_field_sets.append(None)
    field_set_index = 0
    if candidate_field_sets[field_set_index]:
        params_with_fields['fields'] = ",".join(candidate_field_sets[field_set_index])

    all_tasks = []
    next_page_token = None

    try:
        while True:
            if next_page_token:
                params_with_fields['nextPageToken'] = next_page_token
                params.pop('nextPageToken', None)
            else:
                params_with_fields.pop('nextPageToken', None)
                params.pop('nextPageToken', None)

            response = requests.get(url, headers=headers, params=params_with_fields)

            if response.status_code == 400 and b"fields" in response.content.lower():
                field_set_index += 1
                if field_set_index < len(candidate_field_sets):
                    next_fields = candidate_field_sets[field_set_index]
                    if next_fields:
                        params_with_fields['fields'] = ",".join(next_fields)
                        print(f"[INFO] Retrying project-folder task fetch for {folder_id} with reduced field set.")
                    else:
                        params_with_fields.pop('fields', None)
                        print(f"[INFO] Retrying project-folder task fetch for {folder_id} without fields (previous fields invalid).")
                    continue

            if response.status_code != 200:
                print(f"[ERROR] Failed to fetch tasks for folder {folder_id}: {response.status_code}")
                print(f"[DEBUG] Response: {response.content}")
                break

            data = response.json()
            tasks = data.get('data', [])
            all_tasks.extend(tasks)

            next_page_token = data.get('nextPageToken')
            if not next_page_token:
                break

        return all_tasks

    except requests.exceptions.RequestException as e:
        print(f"[INFO] Project folder task fetch error for folder {folder_id}: {e}")
        return []

def extract_planned_effort(item: Dict[str, Any]) -> Optional[float]:
    """
    Extract planned effort from custom fields.
    
    Args:
        item: Item data dictionary
    
    Returns:
        Planned effort in hours or None if not found
    """
    for field in item.get('customFields', []):
        if field.get('id') == PLANNED_EFFORT_FIELD_ID:
            value = field.get('value')
            try:
                # Assuming value is in minutes, convert to hours
                return float(value) / 60.0 if value else None
            except (ValueError, TypeError):
                return None
    return None

def extract_spent_effort(item: Dict[str, Any]) -> Optional[float]:
    """
    Extract spent effort from effortAllocation.
    
    Args:
        item: Item data dictionary
    
    Returns:
        Spent effort in hours or None if not found
    """
    effort_allocation = item.get('effortAllocation', {})
    spent_effort = effort_allocation.get('spentEffort')
    if spent_effort is not None:
        try:
            # Assuming spent effort is in minutes, convert to hours
            return float(spent_effort) / 60.0
        except (ValueError, TypeError):
            return None
    return None

def extract_total_effort(item: Dict[str, Any]) -> Optional[float]:
    """
    Extract total effort from effortAllocation.
    
    Args:
        item: Item data dictionary
    
    Returns:
        Total effort in hours or None if not found
    """
    effort_allocation = item.get('effortAllocation', {})
    total_effort = effort_allocation.get('totalEffort')
    if total_effort is not None:
        try:
            # Assuming total effort is in minutes, convert to hours
            return float(total_effort) / 60.0
        except (ValueError, TypeError):
            return None
    return None

def format_item_output(item: Dict[str, Any], item_kind: str, subtasks: List[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Format item data into a structured output.
    
    Args:
        item: Item data dictionary
        subtasks: List of subtask data dictionaries
    
    Returns:
        Structured dictionary with item information
    """
    output = {
        'item': item,
        'extracted_data': {
            'id': item.get('id'),
            'title': item.get('title'),
            'type': 'Task' if item_kind == 'task' else 'Folder',
            'custom_item_type_id': item.get('customItemTypeId'),
            'status': item.get('status'),
            'importance': item.get('importance'),
            'dates': item.get('dates', {}),
            'due_date': item.get('dates', {}).get('due'),
            'planned_effort_hours': extract_planned_effort(item),
            'spent_effort_hours': extract_spent_effort(item),
            'total_effort_hours': extract_total_effort(item),
            'effort_allocation': item.get('effortAllocation', {}),
            'custom_fields': item.get('customFields', []),
            'subtask_count': len(subtasks) if subtasks else 0,
            'subtasks': subtasks or [],
            'metadata': {
                'fetched_at': datetime.now(timezone.utc).isoformat(),
                'item_id': item.get('id')
            }
        }
    }
    
    # Extract UI ID from permalink if available
    permalink = item.get('permalink', '')
    if 'id=' in permalink:
        try:
            ui_id = permalink.split('id=')[1].split('&')[0]
            output['extracted_data']['metadata']['ui_id'] = ui_id
        except (IndexError, AttributeError):
            pass
    
    return output

def print_item_summary(output: Dict[str, Any]):
    """
    Print a human-readable summary of the item data.
    
    Args:
        output: Formatted item output dictionary
    """
    item = output['item']
    extracted = output['extracted_data']
    
    print(f"\n{'='*80}")
    print(f"ITEM: {extracted['title']}")
    print(f"{'='*80}")
    print(f"ID: {extracted['id']}")
    print(f"Type: {extracted['type']}")
    print(f"Custom Item Type ID: {extracted['custom_item_type_id']}")
    print(f"Status: {extracted['status']}")
    
    if extracted['due_date']:
        print(f"Due Date: {extracted['due_date']}")
    
    if extracted['planned_effort_hours'] is not None:
        print(f"Planned Effort: {extracted['planned_effort_hours']:.2f} hours")
    
    if extracted['spent_effort_hours'] is not None:
        print(f"Spent Effort: {extracted['spent_effort_hours']:.2f} hours")
    
    if extracted['total_effort_hours'] is not None:
        print(f"Total Effort: {extracted['total_effort_hours']:.2f} hours")
    
    print(f"Number of subtasks: {extracted['subtask_count']}")
    
    if extracted['subtasks']:
        print(f"\nSUBTASKS:")
        for subtask in extracted['subtasks']:
            subtask_title = subtask.get('title', 'Unknown')
            subtask_id = subtask.get('id')
            subtask_spent = extract_spent_effort(subtask)
            print(f"  - {subtask_title} (ID: {subtask_id}): ", end="")
            if subtask_spent is not None:
                print(f"Spent Effort: {subtask_spent:.2f} hours")
            else:
                print("No spent effort data")
    
    print(f"{'='*80}")

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Fetch Wrike custom items by custom item type ID')
    parser.add_argument('--task-type-id', default=CORE_TASK_TYPE_ID, 
                       help=f'Custom item type ID for tasks (default: {CORE_TASK_TYPE_ID})')
    parser.add_argument('--project-type-id', default=CORE_PROJECT_TYPE_ID,
                       help=f'Custom item type ID for projects (default: {CORE_PROJECT_TYPE_ID})')
    parser.add_argument('--project-folder-id', help='Folder ID to pull project tasks (descendants + subTasks)')
    parser.add_argument('--fields', help='Comma-separated list of fields to request')
    parser.add_argument('--output', '-o', help='Output JSON file')
    parser.add_argument('--quiet', '-q', action='store_true', help='Minimal console output')
    
    args = parser.parse_args()
    
    if not API_KEY:
        print("[ERROR] WRIKE_API_KEY not found in .env file")
        sys.exit(1)
    
    if not args.quiet:
        print(f"[INFO] Fetching custom items of type:")
        print(f"[INFO]   - Tasks: {args.task_type_id}")
        print(f"[INFO]   - Projects: {args.project_type_id}")
    
    # Determine which fields to request
    if args.fields:
        task_fields = [f.strip() for f in args.fields.split(',')]
        folder_fields = task_fields
    else:
        # Request broad field set so we have "all possible parameters"
        task_fields = DEFAULT_TASK_FIELDS
        folder_fields = DEFAULT_FOLDER_FIELDS
    
    # Fetch all tasks (or tasks in project folder) and folders
    if args.project_folder_id:
        if not args.quiet:
            print(f"[INFO] Fetching tasks from project folder {args.project_folder_id} (descendants + subTasks)...")
        tasks = fetch_tasks_for_project_folder(args.project_folder_id, task_fields)
    else:
        if not args.quiet:
            print("[INFO] Fetching all tasks...")
        tasks = fetch_all_tasks(task_fields, [args.task_type_id])
    
    if not args.quiet:
        print("[INFO] Fetching all folders...")
    folders = fetch_all_folders(folder_fields, [args.project_type_id])
    
    if not args.quiet:
        print(f"[INFO] Found {len(tasks)} tasks and {len(folders)} folders")
    
    # Filter items by custom item type (defensive, though API already filtered)
    custom_item_type_ids = [args.task_type_id, args.project_type_id]
    
    filtered_tasks = filter_by_custom_item_type(tasks, custom_item_type_ids)
    filtered_folders = filter_by_custom_item_type(folders, custom_item_type_ids)
    
    if not args.quiet:
        print(f"[INFO] Found {len(filtered_tasks)} tasks and {len(filtered_folders)} folders matching custom item types")
    
    # Combine all filtered items keeping their origin
    all_items = [(t, 'task') for t in filtered_tasks] + [(f, 'folder') for f in filtered_folders]
    
    if not all_items:
        print("[WARNING] No items found matching the specified custom item types")
        sys.exit(0)
    
    # Process each item
    all_outputs = []
    
    for item, item_kind in all_items:
        # Fetch subtasks
        if item_kind == 'task':
            subtasks = fetch_subtasks_for_task(item['id'], task_fields)
        else:
            # Subtasks-of-folder are tasks, so request task field set
            subtasks = fetch_subtasks_for_folder(item['id'], task_fields)
        
        # Format output
        output = format_item_output(item, item_kind, subtasks)
        all_outputs.append(output)
        
        # Print summary if not quiet
        if not args.quiet:
            print_item_summary(output)
    
    # Save to JSON file if requested
    if args.output:
        try:
            with open(args.output, 'w', encoding='utf-8') as f:
                json.dump(all_outputs, f, indent=2, default=str)
            
            if not args.quiet:
                print(f"[OK] Data saved to: {args.output}")
        except Exception as e:
            print(f"[ERROR] Could not save to file: {e}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
