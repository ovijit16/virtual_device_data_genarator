import requests
import json
import random
import os

# Configuration
GET_URL = "http://agritech-backend-api.nodesdigitalbd.com:8083/api/data/pipeline/v1/kafka/get/task/fisheries"
POST_URL = "http://agritech-backend-api.nodesdigitalbd.com:8083/api/data/pipeline/v1/kafka/post/task/fisheries"
MOTHER_DEVICE_SERIAL = "050609I0J789ND24"
VARIABLE_NAME = "DissolvedOxygen"
TIMESTAMP_FILE = "last_timestamp.txt"

def get_latest_mother_data():
    try:
        response = requests.get(GET_URL)
        data = response.json()
        
        relevant_data = [
            item for item in data 
            if item['deviceSerial'] == MOTHER_DEVICE_SERIAL and item['valueVariable'] == VARIABLE_NAME
        ]
        
        if not relevant_data:
            return None
        
        # Sort by the 'timestamp' field (numeric string) descending
        latest = sorted(relevant_data, key=lambda x: int(x['timestamp']), reverse=True)[0]
        return latest
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

def post_data(serial_suffix, base_value, original_ts):
    base_val_float = float(base_value)
    
    # --- NEW LOGIC START ---
    if 0 <= base_val_float <= 2.0:
        # If value is between 0 and 2, increase by 0.1 to 0.5
        random_offset = random.uniform(0.1, 0.5)
        print(f"Value {base_val_float} is in 0-2 range. Increasing by {round(random_offset, 3)}")
    else:
        # Otherwise, use the original -1.0 to +1.0 range
        random_offset = random.uniform(-1.0, 1.0)
    # --- NEW LOGIC END ---

    new_value = round(base_val_float + random_offset, 3)
    
    # Ensure value doesn't go below 0 (important for Dissolved Oxygen)
    if new_value < 0:
        new_value = 0.01

    payload_dict = {
        "deviceName": f"NDLNode #{MOTHER_DEVICE_SERIAL}{serial_suffix}",
        "deviceSerial": f"{MOTHER_DEVICE_SERIAL}{serial_suffix}",
        "valueVariable": "DissolvedOxygen",
        "variableName": "Dissolved Oxygen",
        "valueUnits": "mg/L",
        "valueMeasure": str(new_value),
        "ts": original_ts 
    }
    
    wrapped_payload = {
        "payload": json.dumps(payload_dict)
    }

    try:
        res = requests.post(POST_URL, json=wrapped_payload, headers={'Content-Type': 'application/json'})
        print(f"Posted {serial_suffix}: Status {res.status_code}, Value {new_value}, TS {original_ts}")
    except Exception as e:
        print(f"Error posting {serial_suffix}: {e}")

def main():
    last_processed_id = ""
    if os.path.exists(TIMESTAMP_FILE):
        with open(TIMESTAMP_FILE, "r") as f:
            last_processed_id = f.read().strip()

    latest_data = get_latest_mother_data()
    
    if not latest_data:
        print("No mother device data found.")
        return

    current_id = latest_data['timestamp']

    if current_id == last_processed_id:
        print(f"No new data. Timestamp {current_id} already processed.")
        return

    print(f"New data detected! (New ID: {current_id})")

    mother_ts_string = latest_data['ts'] 
    for suffix in ["V1", "V2", "V3"]:
        post_data(suffix, latest_data['valueMeasure'], mother_ts_string)

    with open(TIMESTAMP_FILE, "w") as f:
        f.write(current_id)

if __name__ == "__main__":
    main()
