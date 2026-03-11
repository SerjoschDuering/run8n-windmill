import wmill
import requests

# Config
NOCODB_BASE_URL = "http://nocodb:8080/api/v2"
BASE_ID = "pleta3afya5xign"

# Table Mapping
TABLES = {
    "trips": "mloio93evq1v9dx",
    "destinations": "moxoutsm9t8kbnr",
    "annotations": "m0mc4ym4puyvdjj",
    "transport": "mvzr9i72zo8ddxg",
    "itinerary": "mozrv6us3br6emr",
    "accommodations": "mi9u75r2r0plok3",
    "places": "mskymv8m15lqiku"
}

def main(method: str, table_key: str, data: dict = {}, params: dict = {}):
    token = wmill.get_variable("f/trip_planner/nocodb_token")
    table_id = TABLES.get(table_key)
    
    if not table_id:
        return {"error": f"Table key '{table_key}' not found"}

    url = f"{NOCODB_BASE_URL}/tables/{table_id}/records"
    headers = {
        "xc-token": token,
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    
    try:
        if method.upper() == "GET":
            response = requests.get(url, headers=headers, params=params)
        elif method.upper() == "POST":
            response = requests.post(url, headers=headers, json=data)
        else:
            return {"error": f"Method {method} not supported"}
            
        response.raise_for_status()
        return response.json()
    except Exception as e:
        return {"error": str(e)}
