'''BASIC LOGGING SETUP, USED IN OTHER MODULES'''

import logging

logg = logging.Logger('calculations')
logg.setLevel(logging.INFO) # CONFIGURE THE LOG LEVEL HERE
format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file = logging.FileHandler('calculation_Logs.log')
stream = logging.StreamHandler()
file.setFormatter(format)
stream.setFormatter(format)
logg.addHandler(file)
logg.addHandler(stream)
