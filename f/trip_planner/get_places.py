import wmill
import requests

# Config
NOCODB_BASE_URL = "http://nocodb:8080/api/v2"
TABLE_ID_PLACES = "mskymv8m15lqiku" 

def main(trip_id: int = None, destination_id: int = None):
    token = wmill.get_variable("f/trip_planner/nocodb_token")
    
    url = f"{NOCODB_BASE_URL}/tables/{TABLE_ID_PLACES}/records"
    headers = { "xc-token": token, "Accept": "application/json" }
    
    where = []
    if trip_id: where.append(f"(trip_id,eq,{trip_id})")
    if destination_id: where.append(f"(destination_id,eq,{destination_id})")
    
    params = {}
    if where: params["where"] = f"({'~and'.join(where)})"
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()["list"]
    except Exception as e:
        return {"error": str(e)}
