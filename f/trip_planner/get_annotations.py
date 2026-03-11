import wmill
import requests

# Config
NOCODB_BASE_URL = "http://nocodb:8080/api/v2"
TABLE_ID_ANNOTATIONS = "m0mc4ym4puyvdjj" 

def main(trip_id: int):
    token = wmill.get_variable("f/trip_planner/nocodb_token")
    
    url = f"{NOCODB_BASE_URL}/tables/{TABLE_ID_ANNOTATIONS}/records"
    headers = {
        "xc-token": token,
        "Accept": "application/json"
    }
    params = {
        "where": f"(trip_id,eq,{trip_id})",
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()["list"]
    except Exception as e:
        return {"error": str(e)}
