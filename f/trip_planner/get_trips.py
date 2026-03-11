import wmill
import requests

# Config
NOCODB_BASE_URL = "http://nocodb:8080/api/v2"
TABLE_ID_TRIPS = "mloio93evq1v9dx"

def main():
    token = wmill.get_variable("f/trip_planner/nocodb_token")
    
    url = f"{NOCODB_BASE_URL}/tables/{TABLE_ID_TRIPS}/records"
    headers = {
        "xc-token": token,
        "Accept": "application/json"
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()["list"]
    except Exception as e:
        return {"error": str(e)}
