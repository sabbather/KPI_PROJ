import os
import json
import requests
from typing import Optional, Dict, Any
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the API key from environment variables
API_KEY = os.getenv('WRIKE_API_KEY')
BASE_URL = "https://app-eu.wrike.com/api/v4"

def get_task_by_id(task_id: str, fields: Optional[list] = None) -> Optional[Dict[str, Any]]:
    """
    Get task details by ID.
    
    Args:
        task_id: Task ID (could be UI ID or API ID)
        fields: Optional list of fields to request
    
    Returns:
        Task data as dictionary or None if not found
    """
    url = f"{BASE_URL}/tasks/{task_id}"
    headers = {'Authorization': f'bearer {API_KEY}'}
    params = {}
    
    if fields:
        params['fields'] = json.dumps(fields)
    
    try:
        response = requests.get(url, headers=headers, params=params)
        print(f"DEBUG: Request URL: {response.request.url}")
        print(f"DEBUG: Status Code: {response.status_code}")
        
        if response.status_code != 200:
            print(f"DEBUG: Response content: {response.content}")
            return None
        
        data = response.json()
        return data.get('data', [{}])[0]  # Return first task in array
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching task {task_id}: {e}")
        return None

def convert_ui_id_to_api_id(ui_id: str, id_type: str = "ApiV2Task") -> Optional[str]:
    """
    Convert UI ID to API ID using the /ids endpoint.
    
    Args:
        ui_id: UI ID (e.g., 4397662421)
        id_type: Type of ID to convert (ApiV2Task, ApiV2Folder, etc.)
    
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
        print(f"DEBUG ID conversion: Status Code: {response.status_code}")
        print(f"DEBUG ID conversion: Response: {response.content}")
        
        if response.status_code != 200:
            return None
        
        data = response.json()
        # Response format: {"data": [{"id": "API_ID", "legacyId": UI_ID}]}
        if data.get('data') and len(data['data']) > 0:
            return data['data'][0].get('id')
        
        return None
        
    except requests.exceptions.RequestException as e:
        print(f"Error converting ID {ui_id}: {e}")
        return None

def get_task_by_permalink(permalink: str) -> Optional[Dict[str, Any]]:
    """
    Get task by permalink.
    
    Args:
        permalink: Full permalink URL
    
    Returns:
        Task data as dictionary or None if not found
    """
    url = f"{BASE_URL}/tasks"
    headers = {'Authorization': f'bearer {API_KEY}'}
    params = {'permalink': permalink}
    
    try:
        response = requests.get(url, headers=headers, params=params)
        print(f"DEBUG permalink: Status Code: {response.status_code}")
        
        if response.status_code != 200:
            print(f"DEBUG permalink: Response: {response.content}")
            return None
        
        data = response.json()
        tasks = data.get('data', [])
        if tasks:
            return tasks[0]
        
        return None
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching task by permalink: {e}")
        return None

def get_task_all_details(task_id: str) -> Optional[Dict[str, Any]]:
    """
    Get all possible details for a task using multiple approaches.
    
    Args:
        task_id: Task ID (could be UI ID or API ID)
    
    Returns:
        Complete task details or None if not found
    """
    print(f"\n=== Attempting to get details for task ID: {task_id} ===")
    
    # Try 1: Direct fetch (assuming it's an API ID)
    print("\n1. Trying direct fetch as API ID...")
    task_data = get_task_by_id(task_id)
    
    if task_data:
        print(f"   [OK] Success! Found task: {task_data.get('title', 'Unknown')}")
        return task_data
    else:
        print("   [ERROR] Direct fetch failed")
    
    # Try 2: Convert UI ID to API ID
    print("\n2. Trying ID conversion (UI ID to API ID)...")
    api_id = convert_ui_id_to_api_id(task_id)
    
    if api_id:
        print(f"   [OK] Converted to API ID: {api_id}")
        print(f"   Fetching task with API ID...")
        task_data = get_task_by_id(api_id)
        if task_data:
            print(f"   [OK] Success! Found task: {task_data.get('title', 'Unknown')}")
            return task_data
        else:
            print("   [ERROR] Fetch with converted API ID failed")
    else:
        print("   [ERROR] ID conversion failed")
    
    # Try 3: Try with different ID types
    print("\n3. Trying different ID types for conversion...")
    id_types = ["ApiV2Task", "ApiV2Folder", "ApiV2Project", "Task", "Folder", "Project"]
    
    for id_type in id_types:
        print(f"   Trying type: {id_type}")
        api_id = convert_ui_id_to_api_id(task_id, id_type)
        if api_id:
            print(f"   [OK] Converted with type {id_type}: {api_id}")
            task_data = get_task_by_id(api_id)
            if task_data:
                print(f"   [OK] Success! Found task: {task_data.get('title', 'Unknown')}")
                return task_data
    
    print("   [ERROR] All conversion types failed")
    
    # Try 4: Try constructing permalink
    print("\n4. Trying permalink approach...")
    permalink = f"https://www.wrike.com/open.htm?id={task_id}"
    task_data = get_task_by_permalink(permalink)
    
    if task_data:
        print(f"   [OK] Success with permalink! Found task: {task_data.get('title', 'Unknown')}")
        return task_data
    else:
        print("   [ERROR] Permalink approach failed")
    
    print(f"\n[ERROR] Could not retrieve task with ID: {task_id}")
    print("   Please check if the ID is correct and you have access to the task.")
    
    return None

def print_task_details(task_data: Dict[str, Any]):
    """
    Print all available task details in a readable format.
    """
    if not task_data:
        print("No task data to display")
        return
    
    print("\n" + "="*80)
    print("TASK DETAILS")
    print("="*80)
    
    # Basic information
    print(f"\n[ BASIC INFORMATION ]")
    print(f"   ID: {task_data.get('id')}")
    print(f"   Title: {task_data.get('title')}")
    print(f"   Status: {task_data.get('status')}")
    print(f"   Importance: {task_data.get('importance')}")
    print(f"   Permalink: {task_data.get('permalink')}")
    
    # Dates
    print(f"\n[ DATES ]")
    dates = task_data.get('dates', {})
    print(f"   Type: {dates.get('type')}")
    print(f"   Start: {dates.get('start')}")
    print(f"   Due: {dates.get('due')}")
    print(f"   Duration: {dates.get('duration')} minutes")
    
    # Effort allocation
    print(f"\n[ EFFORT ALLOCATION ]")
    effort = task_data.get('effortAllocation', {})
    if effort:
        print(f"   Mode: {effort.get('mode')}")
        print(f"   Total Effort: {effort.get('totalEffort')} minutes")
        allocated = effort.get('allocatedEffort')
        if allocated:
            print(f"   Allocated Effort: {allocated} minutes")
        
        resp_allocation = effort.get('responsibleAllocation', [])
        if resp_allocation:
            print(f"   Responsible Allocation:")
            for i, allocation in enumerate(resp_allocation):
                user_id = allocation.get('userId')
                daily = allocation.get('dailyAllocation', [])
                print(f"     User {i+1}: {user_id}")
                for day in daily:
                    print(f"       {day.get('date')}: {day.get('effortMinutes')} min")
    
    # Relationships
    print(f"\n[ RELATIONSHIPS ]")
    print(f"   Super Task IDs: {task_data.get('superTaskIds', [])}")
    print(f"   Sub Task IDs: {task_data.get('subTaskIds', [])}")
    print(f"   Parent IDs: {task_data.get('parentIds', [])}")
    
    # Custom fields and item type
    print(f"\n[ CUSTOM DATA ]")
    print(f"   Custom Item Type ID: {task_data.get('customItemTypeId')}")
    print(f"   Custom Status ID: {task_data.get('customStatusId')}")
    print(f"   Entity Type ID: {task_data.get('entityTypeId')}")
    
    # Additional metadata
    print(f"\n[ METADATA ]")
    print(f"   Account ID: {task_data.get('accountId')}")
    print(f"   Created: {task_data.get('createdDate')}")
    print(f"   Updated: {task_data.get('updatedDate')}")
    print(f"   Scope: {task_data.get('scope')}")
    
    # Counts
    print(f"\n[ COUNTS ]")
    print(f"   Attachment Count: {task_data.get('attachmentCount')}")
    
    # Finance (if available)
    finance = task_data.get('finance', {})
    if finance:
        print(f"\n[ FINANCE ]")
        print(f"   Billing Type: {finance.get('billingType')}")
        print(f"   Actual Cost: {finance.get('actualCost')}")
        print(f"   Actual Fees: {finance.get('actualFees')}")
        print(f"   Planned Cost: {finance.get('plannedCost')}")
        print(f"   Planned Fees: {finance.get('plannedFees')}")
    
    # All fields (for debugging)
    print(f"\n[ ALL FIELDS AVAILABLE ]")
    for key, value in task_data.items():
        if key not in ['dates', 'effortAllocation', 'finance', 'responsibleAllocation']:
            print(f"   {key}: {value}")
    
    print("\n" + "="*80)

def main():
    """
    Main function to test task ID retrieval.
    """
    # Test with the provided UI task ID
    ui_task_id = "4397662421"
    
    # Also test with a known API ID from our previous run
    # (commented out for now)
    # api_task_id = "MAAAAAEHNoFk"
    
    print("=== Wrike Task Details Fetcher ===")
    print(f"Testing with UI Task ID: {ui_task_id}")
    
    task_data = get_task_all_details(ui_task_id)
    
    if task_data:
        print_task_details(task_data)
        
        # Save to JSON file for reference
        output_file = f"task_{ui_task_id}_details.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(task_data, f, indent=2, default=str)
        
        print(f"\n[OK] Task details saved to: {output_file}")
    else:
        print(f"\n[ERROR] Could not retrieve task details for ID: {ui_task_id}")
        print("\nPossible reasons:")
        print("1. The task ID might be incorrect")
        print("2. You don't have access to this task")
        print("3. The task might be in a different account/region")
        print("4. The ID might need to be converted differently")

if __name__ == "__main__":
    main()