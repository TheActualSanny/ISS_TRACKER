'''MANAGES iss_25544_warehouse TABLE'''

import os
import json
import time
import dotenv
import psycopg2


dotenv.load_dotenv()

conn = psycopg2.connect(
    host = os.getenv('HOST'),
    user = os.getenv('USER'),
    password = os.getenv('PASS'),
    database = os.getenv('DB')
)
curr = conn.cursor()

curr.execute('''CREATE TABLE IF NOT EXISTS iss_25544_warehouse (
             pos integer PRIMARY KEY,
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
             units text
)''')

index = 1

def get_json(): # Gets the json data from the data lake
    with open('raw_data.json', 'r') as f:
        json_data = json.load(f)
        main_data = json_data['iss_data']
        return main_data

def insert_data(inst): # Inserts a new row into the iss_25544_warehouse table
    global index
    curr.execute('''INSERT INTO iss_25544_warehouse VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (index, inst['name'], inst['id'], inst['latitude'], inst['longitude'], inst['altitude'], inst['velocity'], inst['visibility'],
                      inst['footprint'], inst['timestamp'], inst['daynum'], inst['solar_lat'], inst['solar_lon'], inst['units']))
    index += 1

def manage_warehouse(data): # Checks the data json data and inserts new rows into the iss_25544_warehouse table
    global index
    if not data:
        raise ValueError('No data received.')
    
    curr.execute('SELECT * FROM iss_25544_warehouse')
    table_data = curr.fetchall()
    timestamps = [i[9] for i in table_data]
    with conn:
        for inst in data:
            try:
                if inst['timestamp'] not in timestamps:
                    insert_data(inst)
            except psycopg2.ProgrammingError:
                insert_data(inst)


while True:
    time.sleep(120)
    data = get_json()
    manage_warehouse(data)



conn.commit()
conn.close()
curr.close()
