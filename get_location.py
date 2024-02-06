'''FETCHES CURRENT LOCATION FROM THE API'''

import requests
import json
import pandas as pd

url = "https://api.bigdatacloud.net/data/reverse-geocode-client"
payload = {}
headers = {}

def get_location(df):
    params = {'latitude' : df.iloc[-1]['latitude'], 'longitude' : df.iloc[-1]['longitude'], 'localityLanguage' : 'en'}
    response = requests.request("GET", url, headers=headers, data=payload, params=params)
    data = json.loads(response.text)
    info = data['localityInfo']['informative'][0]['name']
    return info
