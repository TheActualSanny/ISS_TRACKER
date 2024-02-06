INSTRUCTIONS FOR USE:

1. Create a database in PostgreSQL.
   
2. Create a .env file with the following variables:
  HOST = 'your_host'
  USER = 'your_username'
  PASS = 'your_password'
  DB = 'your_database'
  CHECK_TIME = 'X' : The data_Calculations.py will be run every X minutes.

3. After creating all of the necessary files, run the main.sh file.

BASIC OVERVIEW OF THE PROGRAM:

1. Raw data about the ISS is getting inserted into the raw_data.json file every two seconds.
2. Every two minutes, the data from the raw_data.json file which has not been iserted into the iss_25544_warehouse PostgreSQL table, will be inserted.
3. Every X minutes (Input X in CHECK_TIME), the distance travelled by the ISS in the last (X - 1) to X minutes and the current location of the station is calculated.
4. Calculated data + the last row of iss_25544_warehouse table is inserted into the iss_normalized table, which is partitioned based on 'visibility' ('daylight' or 'eclipsed')
