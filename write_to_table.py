import psycopg2
import pandas

def write(location, distance, row, conn, curr):
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