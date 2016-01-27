'''
20151221 Scott Havens

Download the stations from the CDEC website and insert into
the database

'''


import numpy as np
import pandas as pd
from ulmo import cdec
from datetime import datetime
import pytz, os
import units
import mysql.connector
from mysql.connector import errorcode


print 'Start --> %s' % datetime.now()

# set some parameters
wy = 2016   # current water year

# connect to db, returns connection cnx
execfile(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'database_connect.py'))
cursor = cnx.cursor()

#------------------------------------------------------------------------------ 
# variables to grab, see http://cdec.water.ca.gov/misc/senslist.html for full list

sensor_id = np.array([2, 3, 4, 9, 10, 12, 18, 26, 103]) - 1# pandas can't index it right
mesowest = np.array(['precip_accum', 'snow_water_equiv', 'air_temp', 'wind_speed', 'wind_direction',
                     'relative_humidity', 'snow_depth', 'solar_radiation', 'solar_radiation']) # cooresponding mesowest variables
v = cdec.historical.get_sensors(sensor_id)
v['mesowest'] = pd.Series(mesowest, index=v.index)

sensor_id = sensor_id + 1

#------------------------------------------------------------------------------ 
# determine what stations to use
qry = "SELECT station_id FROM tbl_stations WHERE source='CDEC'"
cursor.execute(qry)

station_id = []
for sta in cursor:
    station_id.append(sta[0])

# station_id = ['TUM']

# prepare other querys
qryDate = "SELECT max(date_time) AS d FROM tbl_raw_data WHERE station_id='%s'"

#------------------------------------------------------------------------------ 
# go through each station and grab the data


# determine start and end
endTime = datetime.utcnow()
# endTime = datetime(2015,11,2,0,0,0)
startTime_wy = datetime(wy-1, 9, 30) # start a day early to ensure that all values are grabed

# pst_tz = pytz.timezone('America/Metlakatla')
pst_tz = pytz.timezone('US/Pacific')
utc_tz = pytz.timezone('UTC')

for s in station_id:
    
    #------------------------------------------------------------------------------ 
    # determine the last date
    cursor.execute(qryDate % s)
    d = cursor.fetchall()
    
    if d[0][0] is not None:
        startTime = d[0][0]
        startTime = startTime.replace(tzinfo=utc_tz).astimezone(pst_tz)
#         startTime = datetime(wy-1, 9, 30)
    else:
        startTime = startTime_wy
    
    print 'Downloading %s --> %s ...' % (s, startTime)
        
    #------------------------------------------------------------------------------ 
    # grab the data from CDEC
    data = cdec.historical.get_data([s], resolutions='hourly', sensor_ids=sensor_id, start=startTime, end=endTime)
    
    # remove any empty dataframes
    for x in data[s].keys():
        if data[s][x].empty:
            del data[s][x]
            
    # if both instantaneous solar and avg solar, remove instantenous
    if ('SOLAR RADIATION' in data[s].keys()) and ('SOLAR RADIATION AVG' in data[s].keys()):
        del data[s]['SOLAR RADIATION']

    if data[s]:
        
        # variables returned from CDEC
        variables = data[s].keys()
    
        #------------------------------------------------------------------------------
        # align the CDEC variables to the Mesowest variables
        meso = [ v[v.Description == variables[x]]['mesowest'].values[0] for x in range(len(variables)) ]
    
        #------------------------------------------------------------------------------
        # make the data frame useful and combine
        r = pd.concat([data[s][x] for x in variables], axis=1, keys=meso)
        r.columns = meso
        
        r = units.convertUnits(r)   # convert units from english to metric
        r = r.replace(np.nan, 'NULL', regex=True)   # replace NaN with NULL for MySQL
            
        #------------------------------------------------------------------------------ 
        # insert data into database
        N = len(r)  # number of measurements
        
        d = r.index.tolist()
        values = r.values
    #     mst_tz = pytz.timezone('MST')
        
        
        try:
        
            print 'Adding %s data to database, %i records...' % (s, N)
            columns = ['station_id', 'date_time'] + meso
            
            for i in range(N):
                
                # the current record
                record = r.iloc[i]
                
                # get the date_time index
                # need to convert to UTC from PST, and maybe PSD later?
                date_time = r.index.tolist()[i]
                t = date_time.replace(tzinfo=pst_tz).astimezone(utc_tz)
                
                # get all the values
#                 record.air_temp = 100 + i
                values = record.values.tolist()
                for i,vi in enumerate(values):
                    if vi == 'NULL':
                        values[i] = None
                 
                # insert into the database
                val = []
                for i,f in enumerate(meso):
                    if not values[i]:
                        val.append("%s=NULL" % f)
                    else:
                        val.append("%s='%s'" % (f, values[i]))
                 
                values = [s, t.strftime("%Y-%m-%d %H:%M:%S")] + values
                            
                add_data = "INSERT INTO tbl_raw_data (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s" % \
                    (",".join(columns), ','.join(['%s' for i in values]), ','.join(val))
                    
                add_data2 = "INSERT INTO tbl_level1 (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s" % \
                    (",".join(columns), ','.join(['%s' for i in values]), ','.join(val))
                
                
                cursor.execute(add_data, values)
                cursor.execute(add_data2, values)
                
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
            print 'Error loading data into database for %s (%s): ' % (s, date_time)

    else:
        print 'Station %s has no data' % s


cnx.close()

print 'Done --> %s' % datetime.now()
