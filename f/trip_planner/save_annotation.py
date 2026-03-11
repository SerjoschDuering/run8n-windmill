import wmill
import requests

# Config
NOCODB_BASE_URL = "http://nocodb:8080/api/v2"
TABLE_ID_ANNOTATIONS = "m0mc4ym4puyvdjj"

def main(trip_id: int, geojson: dict, notes: str = ""):
    token = wmill.get_variable("f/trip_planner/nocodb_token")
    
    url = f"{NOCODB_BASE_URL}/tables/{TABLE_ID_ANNOTATIONS}/records"
    headers = {
        "xc-token": token,
        "Content-Type": "application/json"
    }
    
    data = {
        "trip_id": trip_id,
        "geojson": geojson,
        "notes": notes,
        "layer": "brainstorm"
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        return {"error": str(e)}
