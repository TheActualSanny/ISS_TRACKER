import requests
import json
import time

url = "https://api.wheretheiss.at/v1/satellites/25544"

payload = {}
headers = {}

# Main functions used for managing: raw_data.json
def write_Json(data, filename = 'raw_data.json'):
    with open(filename, 'w') as f:
        json.dump(data, f, indent = 4)

def modify_Json(new_iss):
    with open('raw_data.json', 'r') as f:
        data = json.load(f)
        iss = data['iss_data']
        iss.append(new_iss)
    return data

while True:
    response = requests.request("GET", url, headers=headers, data=payload)
    time.sleep(2)
    write_Json(modify_Json(response.json()))
    