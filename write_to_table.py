import psycopg2
import pandas
import needed_queries as const

def write(location, distance, row, conn, curr) -> None: #must add into seperate file as dictionary
    with conn:
        curr.execute('''CREATE TABLE IF NOT EXISTS iss_normalized ({}) PARTITION BY LIST(visibility)'''.format(' '.join([' '.join(inst) for inst in const.create_normalized.items()]))) ## check documentation
        curr.execute('''CREATE TABLE IF NOT EXISTS iss_day PARTITION OF iss_normalized FOR VALUES IN ('daylight')''')
        curr.execute('''CREATE TABLE IF NOT EXISTS iss_eclipsed PARTITION OF iss_normalized FOR VALUES IN ('eclipsed')''')
        curr.execute('''INSERT INTO iss_normalized VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ''', const.normalized_values(row, distance, location)) #must add into seperate file, add an unpacking operator
    conn.close()
    curr.close()
    
