"""
Determine the stations within a bounding box from Mesowest
and insert them into the tbl_stations
"""

import mysql.connector
from mysql.connector import errorcode
import os
from MesoPy import Meso

basin = 'BRB_bbox'
bbox='-116.4,43.05,-114.45,44.44'     # BRB

basin = 'TUOL_bbox'
bbox='-119.98,37.7,-119.19,38.35' # TUOL

# connect to db, returns connection cnx
execfile(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'database_connect.py'))

cursor = cnx.cursor()

#------------------------------------------------------------------------------ 
# set the parameters for the Mesowest query and build
p = {}
p['bbox'] = bbox
p['token'] = 'e505002015c74fa6850b2fc13f70d2da'     # API token

m = Meso(token=p['token'])

print 'Downloading data ...' 

data = m.metadata(bbox=p['bbox'])
                            
    
print 'Total stations = %i' % len(data['STATION'])

#------------------------------------------------------------------------------ 
# go through each station and grab the data

# table columns
columns = ['station_id', 'client', 'source']

# data to insert
values = ['temp', basin, 'Mesowest']

for index,s in enumerate(data['STATION']):
   
    # determine station id
    station_id = str(s['STID'])
    
    
    try:
    
        print 'Adding %s info to database' % station_id
        
        values[0] = station_id
        
        # insert into the database
        val = []
        for i,f in enumerate(columns):
            val.append("%s='%s'" % (f, values[i]))
                                  
                                           
        add_data = "INSERT INTO tbl_stations (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s" % \
            (",".join(columns), ','.join(['%s' for i in values]), ','.join(val))
        
        cursor.execute(add_data, values)
        
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
        print 'Error loading data into database for %s: ' % (station_id)
        
        
        
cnx.close()
    