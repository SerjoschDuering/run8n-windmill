import wmill
import requests

# Config
NOCODB_BASE_URL = "http://nocodb:8080/api/v2"
TABLE_ID_ITINERARY = "mozrv6us3br6emr" 

def main(trip_id: int):
    token = wmill.get_variable("f/trip_planner/nocodb_token")
    
    url = f"{NOCODB_BASE_URL}/tables/{TABLE_ID_ITINERARY}/records"
    headers = { "xc-token": token, "Accept": "application/json" }
    params = {
        "where": f"(trip_id,eq,{trip_id})",
        "sort": "day_number"
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()["list"]
    except Exception as e:
        return {"error": str(e)}
