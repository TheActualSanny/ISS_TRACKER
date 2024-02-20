'''CONTAINS THE CONSTANT VARIABLES, MAINLY TABLE CREATION QUERIES'''
import psycopg2
import os
import dotenv

dotenv.load_dotenv()

def connector():
    return psycopg2.connect(
        host = os.getenv('HOST'),
        user = os.getenv('USER'),
        password = os.getenv('PASS'),
        database = os.getenv('DB'))

def normalized_values(row, dist, loc) -> None:
    final = (row['name'], int(row['id']), float(row['latitude']), row['longitude'], 
             row['altitude'], row['velocity'], row['visibility'], row['footprint'], 
             int(row['timestamp']), row['daynum'], row['solar_lat'], row['solar_lon'], 
             row['units'], float(dist), str(loc))
    return final

create_warehouse = {    # QUERY WHICH CREATES THE iss_25544_warehouse TABLE
             'pos' : 'SERIAL PRIMARY KEY,',
             'name' : 'text,',  # must change into varchar to set a limit
             'id'  : 'integer,',
             'latitude' : 'real,',
             'longitude' : 'real,',
             'altitude' : 'real,',
             'velocity' : 'real,',
             'visibility' : 'text,',
             'footprint' : 'real,',
             'timestamp' : 'integer,',
             'daynum' : 'real,',
             'solar_lat' : 'real,',
             'solar_lon' : 'real,',
             'units' : 'text'
}

create_normalized = {    # QUERY WHICH CREATES THE iss_normalized TABLE
            'name' : 'text,',
            'id' : 'integer,',
            'latitude' : 'real,',
            'longitude' : 'real,',
            'altitude' : 'real,',
            'velocity' : 'real,',
            'visibility' : 'text,',
            'footprint' : 'real,',
            'timestamp' : 'integer,',
            'daynum' : 'real,',
            'solar_lat' : 'real,',
            'solar_lon' : 'real,',
            'units' : 'text,',
            'aprox_distance' : 'real,',
            'curr_location' : 'text'
}



insert_into_warehouse = 'INSERT INTO iss_25544_warehouse(name, id, latitude, longitude, altitude, velocity, visibility, footprint, timestamp, daynum, solar_lat, solar_lon, units) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

