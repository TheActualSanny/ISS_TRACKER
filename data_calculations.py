'''CALCULATES TRAVELLED DISTANCE, GETS CURRENT LOCATION AND WRITES IT INTO iss_normalized TABLE.'''

import os
import dotenv
import time
import psycopg2
import numpy as np
import pandas as pd
from logging_setup import logg
from get_location import get_location


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

#  Function which writes into the iss_normalized table
def write_to_table(location, distance, row):
    with conn:
        curr.execute('''CREATE TABLE IF NOT EXISTS iss_normalized (
                name text,
                id integer,
                latitude real,
                longitude real,
                altitude real,
                velocity real,
                visibility text,
                footprint real,
                timestamp integer,
                daynum real,
                solar_lat real,
                solar_lon real,
                units text,
                aprox_distance real,
                curr_location text   
        ) PARTITION BY LIST(visibility)''')

        curr.execute('''CREATE TABLE IF NOT EXISTS iss_day PARTITION OF iss_normalized FOR VALUES IN ('daylight')''')
        curr.execute('''CREATE TABLE IF NOT EXISTS iss_eclipsed PARTITION OF iss_normalized FOR VALUES IN ('eclipsed')''')
        curr.execute('''INSERT INTO iss_normalized VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ''', (row['name'], int(row['id']), float(row['latitude']), row['longitude'], row['altitude'], row['velocity'], row['visibility'],
                        row['footprint'], int(row['timestamp']), row['daynum'], row['solar_lat'], row['solar_lon'], row['units'],
                        float(distance), str(location)))



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


# Note: if the CHECK_TIME >= 5, on the first iteration, the program will calculate the distance travelled in the first 4 minutes.
# Main loop
while True:
    time.sleep(seconds)
    df = get_data()
    location = get_location(df)
    distance = calculate_distance(df)
    write_to_table(location, distance, df.iloc[-1])
    logg.info(f'Travelled approx. {distance} kilometers in the last {int(os.getenv('CHECK_TIME')) - 1} - {int(os.getenv('CHECK_TIME'))} minutes. Current Location: {location}')


conn.commit()
curr.close()
conn.close()
