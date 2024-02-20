'''MANAGES iss_25544_warehouse TABLE'''

import os
import json
import time
import dotenv
import psycopg2
import needed_queries as const

dotenv.load_dotenv()


def get_json() -> None:
    '''Gets the json data from the data lake'''
    with open('raw_data.json', 'r') as f: ## name
        json_data = json.load(f)
        main_data = json_data['iss_data']
        return main_data

def insert_data(inst, curr) -> None:
    '''Inserts a new row into the iss_25544_warehouse table'''
 
    curr.execute(const.insert_into_warehouse, (inst['name'], inst['id'], inst['latitude'], inst['longitude'], inst['altitude'], inst['velocity'], inst['visibility'],
                      inst['footprint'], inst['timestamp'], inst['daynum'], inst['solar_lat'], inst['solar_lon'], inst['units']))
   

def manage_warehouse(data, conn, curr) -> None:
    '''Checks the data json data and inserts new rows into the iss_25544_warehouse table'''
    if not data:  # must add the cursor here
        raise ValueError('No data received.')
    
    curr.execute('SELECT * FROM iss_25544_warehouse')
    table_data = curr.fetchall()
    timestamps = [i[9] for i in table_data]
    with conn:
        for inst in data:
            try:
                if inst['timestamp'] not in timestamps:
                    insert_data(inst, curr)
            except psycopg2.ProgrammingError:
                insert_data(inst, curr)
    conn.close()
    curr.close()


while True:
    time.sleep(2)
    conn = const.connector()
    curr = conn.cursor()
    data = get_json()
    manage_warehouse(data, conn, curr)



conn.commit()
conn.close()
curr.close()
