import json
import os
import requests
from typing import Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the API key from environment variables
API_KEY = os.getenv('WRIKE_API_KEY')
CORE_TASK_ID = "IEAGWGLXPIAHEHEZ"
CORE_PROJECT_ID = "IEAGWGLXPIAHEHH3"
SPACE_ID = "MQAAAAEFfBip"  # From custom item type data


def fetch_tasks_paginated(
    space_id: Optional[str] = None,
    fields: Optional[list] = None,
    page_size: int = 200
) -> list:
    """
    Fetch tasks from Wrike API with pagination.
    
    If space_id is provided, fetches tasks from that space.
    Returns all tasks across all pages.
    """
    if space_id:
        url = f"https://app-eu.wrike.com/api/v4/spaces/{space_id}/tasks"
    else:
        url = "https://app-eu.wrike.com/api/v4/tasks"
    
    headers = {'Authorization': f'bearer {API_KEY}'}
    params = {'pageSize': page_size}
    
    if fields:
        params['fields'] = json.dumps(fields)
    
    all_tasks = []
    next_page_token = None
    
    while True:
        if next_page_token:
            params['nextPageToken'] = next_page_token
        else:
            # Ensure nextPageToken is not in params for first request
            params.pop('nextPageToken', None)
        
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code != 200:
            print(f"DEBUG: Request failed with status {response.status_code}")
            print(f"DEBUG: Response: {response.content}")
        
        response.raise_for_status()
        
        data = response.json()
        tasks = data.get('data', [])
        all_tasks.extend(tasks)
        
        next_page_token = data.get('nextPageToken')
        if not next_page_token:
            break
    
    return all_tasks


def fetch_subtasks(task_id: str) -> list:
    """Fetch subtasks for a given task ID using the subtasks endpoint."""
    url = f"https://app-eu.wrike.com/api/v4/tasks/{task_id}/subtasks"
    headers = {'Authorization': f'bearer {API_KEY}'}
    params = {}
    # Might need pagination
    all_subtasks = []
    next_page_token = None
    
    while True:
        if next_page_token:
            params['nextPageToken'] = next_page_token
        else:
            params.pop('nextPageToken', None)
        
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            print(f"DEBUG subtasks: Status {response.status_code}, Response: {response.content}")
        response.raise_for_status()
        
        data = response.json()
        subtasks = data.get('data', [])
        all_subtasks.extend(subtasks)
        
        next_page_token = data.get('nextPageToken')
        if not next_page_token:
            break
    
    return all_subtasks


def fetch_task_subtask_ids(task_id: str) -> list[str]:
    """Return the IDs of subtasks of `task_id` via the tasks query."""
    tasks = fetch_tasks_by_ids([task_id], fields=['subTaskIds'])
    if not tasks:
        return []
    return tasks[0].get('subTaskIds', [])


def fetch_tasks_by_ids(task_ids: list[str], fields: list[str] | None = None) -> list:
    """Fetch multiple task records by their IDs (used for subtasks)."""
    if not task_ids:
        return []
    ids_segment = ",".join(task_ids)
    url = f"https://app-eu.wrike.com/api/v4/tasks/{ids_segment}"
    params = {}
    if fields:
        params['fields'] = json.dumps(fields)
    headers = {
        'Authorization': f'bearer {API_KEY}'
    }
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json().get('data', [])


def calculate_total_effort(subtasks: list) -> float:
    """Calculate the total effort of subtasks."""
    total_effort = 0.0
    for subtask in subtasks:
        effort = subtask.get('effort', 0.0)
        total_effort += effort
    return total_effort


def fetch_custom_item_types(space_id: str | None = None) -> list:
    """Fetch available custom item types, optionally narrowed to a space."""
    base_url = "https://app-eu.wrike.com/api/v4/custom_item_types"
    url = base_url if space_id is None else f"https://app-eu.wrike.com/api/v4/spaces/{space_id}/custom_item_types"
    headers = {
        'Authorization': f'bearer {API_KEY}'
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json().get('data', [])


def find_custom_item_type_id(title: str, space_id: str | None = None) -> str | None:
    """Return the ID of the custom item type matching the provided title."""
    items = fetch_custom_item_types(space_id=space_id)
    for item in items:
        if item.get('title') == title:
            return item.get('id')
    return None


def fetch_spaces() -> list:
    """Fetch all spaces."""
    url = "https://app-eu.wrike.com/api/v4/spaces"
    headers = {'Authorization': f'bearer {API_KEY}'}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json().get('data', [])


def filter_tasks_by_custom_item_type(tasks: list, custom_item_type_id: str) -> list:
    """Filter tasks by custom item type ID."""
    return [task for task in tasks if task.get('customItemTypeId') == custom_item_type_id]


def print_task_keys(tasks: list, limit: int = 3):
    """Print keys and important fields of tasks for debugging."""
    for i, task in enumerate(tasks[:limit]):
        print(f"  Task {i+1} keys: {list(task.keys())}")
        if 'customItemTypeId' in task:
            print(f"    customItemTypeId: {task['customItemTypeId']}")
        if 'dates' in task:
            dates = task['dates']
            print(f"    dates: type={dates.get('type')}, start={dates.get('start')}, due={dates.get('due')}, duration={dates.get('duration')}")
        if 'effortAllocation' in task:
            effort = task['effortAllocation']
            print(f"    effortAllocation: {effort}")
        if 'superTaskIds' in task:
            print(f"    superTaskIds: {task['superTaskIds']}")


if __name__ == "__main__":
    try:
        # Test fetching spaces to validate space ID
        print("Fetching spaces...")
        spaces = fetch_spaces()
        print(f"Found {len(spaces)} spaces")
        for space in spaces[:5]:
            print(f"  Space: {space.get('title')} - ID: {space.get('id')}")
        
        # First, fetch tasks without fields parameter to see default fields
        print("\nFetching tasks without fields parameter (default fields):")
        default_tasks = fetch_tasks_paginated(space_id=SPACE_ID, page_size=10)
        print(f"Found {len(default_tasks)} tasks")
        print_task_keys(default_tasks)
        
        # Then fetch with customItemTypeId field
        print("\nFetching tasks with customItemTypeId field:")
        tasks_with_field = fetch_tasks_paginated(
            space_id=SPACE_ID,
            fields=['customItemTypeId', 'effortAllocation', 'superTaskIds'],
            page_size=200
        )
        print(f"Found {len(tasks_with_field)} tasks")
        print_task_keys(tasks_with_field)
        
        # Filter tasks by Core Task custom item type
        core_tasks = filter_tasks_by_custom_item_type(tasks_with_field, CORE_TASK_ID)
        print(f"\nFound {len(core_tasks)} tasks of type 'Core task'")
        
        if core_tasks:
            # Calculate total effort for first 3 Core tasks
            print("\nCalculating effort for Core tasks:")
            for task in core_tasks[:3]:
                task_id = task['id']
                try:
                    subtasks = fetch_subtasks(task_id)
                    total_effort = calculate_total_effort(subtasks)
                    print(f"Task: {task.get('title')} (ID: {task_id}), Due: {task.get('dueDate')}, Total Effort: {total_effort}")
                except requests.exceptions.RequestException as e:
                    print(f"Error fetching subtasks for Task ID {task_id}: {e}")
        else:
            print("No Core tasks found in this space.")
            print("Trying to fetch tasks from entire account (no space filter)...")
            all_account_tasks = fetch_tasks_paginated(            fields=['customItemTypeId', 'effortAllocation', 'superTaskIds'], page_size=200)
            print(f"Found {len(all_account_tasks)} tasks in account")
            core_tasks_account = filter_tasks_by_custom_item_type(all_account_tasks, CORE_TASK_ID)
            print(f"Found {len(core_tasks_account)} Core tasks in entire account")
            if core_tasks_account:
                print_task_keys(core_tasks_account, limit=3)
                # Test subtask fetching for first task
                if core_tasks_account:
                    first_task = core_tasks_account[0]
                    task_id = first_task['id']
                    print(f"\nTesting subtask fetch for task ID: {task_id}")
                    try:
                        subtasks = fetch_subtasks(task_id)
                        print(f"Number of subtasks: {len(subtasks)}")
                        for subtask in subtasks[:3]:
                            print(f"  Subtask: {subtask.get('title')}, Effort: {subtask.get('effort')}")
                        total_effort = calculate_total_effort(subtasks)
                        print(f"Total effort of subtasks: {total_effort}")
                    except requests.exceptions.RequestException as e:
                        print(f"Error fetching subtasks: {e}")
        
        # Look up Core custom item types by exact title
        print("\nLooking up custom item type IDs:")
        for title in ("Core projects", "Core tasks"):
            custom_item_id = find_custom_item_type_id(title)
            print(f"{title} custom item type ID: {custom_item_id or 'not found'}")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
