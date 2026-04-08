#!/usr/bin/env python3
"""
Wrike Task Details Fetcher
Fetches complete details for a Wrike task including timelogs and subtasks.
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
BASE_URL = "https://app-eu.wrike.com/api/v4"

# Default fields to request for tasks (empty = default fields)
DEFAULT_TASK_FIELDS = None  # Use default fields

def convert_ui_id_to_api_id(ui_id: str, id_type: str = "ApiV2Task") -> Optional[str]:
    """
    Convert UI ID to API ID using the /ids endpoint.
    
    Args:
        ui_id: UI ID (e.g., 4397662421)
        id_type: Type of ID to convert
    
    Returns:
        API ID or None if conversion fails
    """
    url = f"{BASE_URL}/ids"
    headers = {'Authorization': f'bearer {API_KEY}'}
    params = {
        'type': id_type,
        'ids': f'[{ui_id}]'
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code != 200:
            print(f"[DEBUG] ID conversion failed: {response.status_code}")
            return None
        
        data = response.json()
        if data.get('data') and len(data['data']) > 0:
            return data['data'][0].get('id')
        
        return None
        
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] ID conversion error: {e}")
        return None

def get_api_id(item_id: str) -> str:
    """
    Convert any item ID format to API ID.
    If it's already an API ID (starts with letters), return as-is.
    Otherwise, try to convert from UI ID.
    """
    # Check if it looks like an API ID (starts with letter, not all digits)
    if not item_id.isdigit():
        return item_id
    
    # Try to convert UI ID to API ID
    api_id = convert_ui_id_to_api_id(item_id)
    if api_id:
        return api_id
    
    # If conversion fails, try with different ID types
    id_types = ["ApiV2Task", "ApiV2Folder", "ApiV2Project", "Task", "Folder", "Project"]
    for id_type in id_types:
        api_id = convert_ui_id_to_api_id(item_id, id_type)
        if api_id:
            print(f"[INFO] Converted using type {id_type}")
            return api_id
    
    # If all conversions fail, return original ID (will likely fail later)
    print(f"[WARNING] Could not convert ID {item_id} to API ID")
    return item_id

def fetch_item_details(item_id: str, fields: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
    """
    Fetch item details by API ID. Could be a task or folder.
    
    Args:
        item_id: Item API ID
        fields: List of fields to request (None for default fields)
    
    Returns:
        Item data dictionary or None if not found
    """
    # First try as task
    url = f"{BASE_URL}/tasks/{item_id}"
    headers = {'Authorization': f'bearer {API_KEY}'}
    params = {}
    
    if fields:
        params['fields'] = json.dumps(fields)
    
    try:
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json()
            tasks = data.get('data', [])
            if tasks:
                return tasks[0]
        
        # If not found as task, try as folder
        url = f"{BASE_URL}/folders/{item_id}"
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json()
            folders = data.get('data', [])
            if folders:
                return folders[0]
        
        return None
        
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Item fetch error: {e}")
        return None

def fetch_items_by_ids(item_ids: List[str]) -> List[Dict[str, Any]]:
    """
    Fetch multiple items by their IDs.
    
    Args:
        item_ids: List of item IDs to fetch
    
    Returns:
        List of item data dictionaries
    """
    items = []
    
    for item_id in item_ids:
        api_id = get_api_id(item_id)
        item_data = fetch_item_details(api_id)
        if item_data:
            items.append(item_data)
        else:
            print(f"[WARNING] Could not fetch item with ID {item_id}")
    
    return items

def extract_task_details(task_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract due date and planned effort from task data.
    
    Args:
        task_data: Task data dictionary
    
    Returns:
        Dictionary with extracted details
    """
    due_date = task_data.get('dates', {}).get('due')
    planned_effort = None
    
    # Extract planned effort from custom fields
    for field in task_data.get('customFields', []):
        if field.get('id') == 'IEAGWGLXJUALG3VY':  # Custom field ID for planned effort
            planned_effort = field.get('value')
            # Convert from minutes to hours if needed
            try:
                planned_effort = float(planned_effort) / 60.0 if planned_effort else None
            except (ValueError, TypeError):
                planned_effort = None
    
    return {
        'id': task_data['id'],
        'title': task_data.get('title'),
        'due_date': due_date,
        'planned_effort': planned_effort
    }

def fetch_subtasks(task_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Fetch subtasks for a task.
    Note: Wrike API doesn't have a direct /subtasks endpoint, so we need to
    get all tasks and filter by superTaskIds, or use the parent tasks endpoint.
    """
    # Get subtask IDs from the task data
    subtask_ids = task_data.get('subTaskIds', [])
    if not subtask_ids:
        return []
    
    # Fetch details for all subtasks
    return fetch_multiple_tasks(subtask_ids)

def fetch_multiple_tasks(task_ids: List[str]) -> List[Dict[str, Any]]:
    """
    Fetch multiple tasks by their IDs.
    
    Args:
        task_ids: List of task API IDs
    
    Returns:
        List of task data dictionaries
    """
    if not task_ids:
        return []
    
    # Wrike API supports up to 100 IDs per request
    max_batch = 100
    all_tasks = []
    
    for i in range(0, len(task_ids), max_batch):
        batch = task_ids[i:i + max_batch]
        ids_segment = ",".join(batch)
        
        url = f"{BASE_URL}/tasks/{ids_segment}"
        headers = {'Authorization': f'bearer {API_KEY}'}
        
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                tasks = data.get('data', [])
                all_tasks.extend(tasks)
            else:
                print(f"[WARNING] Batch fetch failed: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] Batch fetch error: {e}")
    
    return all_tasks

def extract_subtask_spent_effort(subtasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Extract spent effort from subtasks.
    
    Args:
        subtasks: List of subtask data
    
    Returns:
        List of dictionaries with subtask ID and spent effort
    """
    subtask_efforts = []
    
    for subtask in subtasks:
        spent_effort = subtask.get('effortAllocation', {}).get('spentEffort')
        
        subtask_efforts.append({
            'id': subtask['id'],
            'title': subtask['title'],
            'spent_effort': spent_effort
        })
    
    return subtask_efforts

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Fetch complete Wrike task details')
    parser.add_argument('--item-ids', nargs='+', help='List of item IDs to fetch (e.g., IEAGWGLXPIAHEHEZ, IEAGWGLXPIAHEHH3)')
    parser.add_argument('--fields', help='Comma-separated list of fields to request')
    parser.add_argument('--no-timelogs', action='store_true', help='Skip fetching timelogs')
    parser.add_argument('--no-subtasks', action='store_true', help='Skip fetching subtasks')
    parser.add_argument('--output', '-o', help='Output JSON file (default: task_<id>_full.json)')
    parser.add_argument('--quiet', '-q', action='store_true', help='Minimal console output')
    
    args = parser.parse_args()
    
    if not API_KEY:
        print("[ERROR] WRIKE_API_KEY not found in .env file")
        sys.exit(1)
    
    if not args.item_ids:
        print("[ERROR] No item IDs provided")
        sys.exit(1)
    
    if not args.quiet:
        print(f"[INFO] Fetching items for IDs: {args.item_ids}")
    
    # Fetch items by their IDs
    items = fetch_items_by_ids(args.item_ids)
    
    for item in items:
        # Extract main item details
        details = extract_task_details(item)
        
        # Fetch subtasks and extract their spent efforts (if item is a task)
        if 'subTaskIds' in item:  # Check if it's a task with subtasks
            subtasks = fetch_subtasks(item)
            subtask_efforts = extract_subtask_spent_effort(subtasks)
        else:
            subtasks = []
            subtask_efforts = []
        
        # Print or process the extracted information as needed
        print(f"\n{'='*80}")
        print(f"ITEM DETAILS:")
        print(f"{'='*80}")
        print(f"ID: {details['id']}")
        print(f"Title: {details['title']}")
        print(f"Type: {'Task' if 'subTaskIds' in item else 'Folder'}")
        print(f"Due Date: {details['due_date']}")
        print(f"Planned Effort: {details['planned_effort']} hours")
        
        if subtask_efforts:
            print(f"\nSUBTASKS:")
            for subtask in subtask_efforts:
                print(f"  - {subtask['title']} (ID: {subtask['id']}): Spent Effort: {subtask['spent_effort']} minutes")
        else:
            print(f"\nNo subtasks found.")

# Ensure the rest of the code is implemented as needed

if __name__ == "__main__":
    sys.exit(main())