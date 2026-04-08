import json, os, requests
from urllib.parse import urljoin
project = "MQAAAAEHXFe3"
headers = {"Authorization": f"bearer {os.environ['WRIKE_API_KEY']}"}
params = {"descendants": "true", "fields": json.dumps(["effortAllocation"]), "pageSize": 1000}
url = urljoin(os.environ.get("WRIKE_BASE_URL", "https://app-eu.wrike.com/api/v4") + "/", f"folders/{project}/tasks")
total = 0
next_token = None
while True:
    if next_token:
        params["nextPageToken"] = next_token
    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    data = resp.json()
    for item in data.get("data", []):
        effort = (item.get("effortAllocation") or {}).get("allocatedEffort") or (item.get("effortAllocation") or {}).get("totalEffort") or 0
        total += int(effort or 0)
    next_token = data.get("nextPageToken")
    if not next_token:
        break
print("Total effort minutes:", total)
print("Total effort hours:", total / 60)
