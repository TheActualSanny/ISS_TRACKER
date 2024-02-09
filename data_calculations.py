'''CALCULATES TRAVELLED DISTANCE, GETS CURRENT LOCATION AND WRITES IT INTO iss_normalized TABLE.'''

import os
import dotenv
import time
import psycopg2
import numpy as np
import pandas as pd
from logging_setup import logg
from get_location import get_location
from write_to_table import write

dotenv.load_dotenv()

#  Calculates the aproximate distance travelled in the last 4-5 minutes
def calculate_distance(df):
    avrg_Velocity = round(np.sum(df['velocity']) / df.count()['velocity'], 4)
    time_Difference = df.iloc[-1]['timestamp'] - df.iloc[0]['timestamp']
    return round(avrg_Velocity * time_Difference / 3600, 4) 

# Loads the data from the last 4-5 minutes into the pandas DataFrame
def get_data():
    curr.execute('SELECT * FROM iss_25544_warehouse WHERE timestamp>=((SELECT max(timestamp) FROM iss_25544_warehouse) - %s)', (seconds,))
    data = curr.fetchall()
    df = pd.DataFrame(data=data, columns=columns)
    return df



#  Setup: Connecting to the database and setting up columns
seconds =  int(os.getenv('CHECK_TIME')) * 60
columns = ['pos', 'name', 'id', 'latitude', 'longitude', 'altitude', 
           'velocity', 'visibility', 'footprint', 'timestamp', 'daynum', 'solar_lat', 'solar_lon', 'units']

conn = psycopg2.connect(
    host = os.getenv('HOST'),
    user = os.getenv('USER'),
    password = os.getenv('PASS'),
    database = os.getenv('DB')
)
curr = conn.cursor()
key = 1

# Note: if the CHECK_TIME > 4, on the first iteration, the program will calculate the distance travelled in the first 4 minutes.
# Main loop
while True:
    time.sleep(seconds)
    df = get_data()
    location = get_location(df)
    distance = calculate_distance(df)
    write(location, distance, df.iloc[-1], conn, curr)
    logg.info(f'Travelled approx. {distance} kilometers in the last {int(os.getenv('CHECK_TIME')) - 1} - {int(os.getenv('CHECK_TIME'))} minutes. Current Location: {location}')


conn.commit()
curr.close()
conn.close()
