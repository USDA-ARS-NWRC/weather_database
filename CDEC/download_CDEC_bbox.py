'''
20160705 Scott Havens

Download the stations from the CDEC website and insert into
the database for those stations within the bounding box

'''


import numpy as np
import pandas as pd
from ulmo import cdec
from datetime import datetime
import pytz, os
import units
import mysql.connector
from mysql.connector import errorcode

# connect to db, returns connection cnx
execfile(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'database_connect.py'))


def downloadData(startTime, endTime, bbox):
    """
    Download data from CDEC between the startTime and endTime for all those 
    stations within bbox
    
    Args:
        startTime (datetime): start time for data
        endTime (datetime): end time for data
        bbox (str): 'll_lat, ll_lon, ur_lat, ur_lon' for bounding box
        
    Returns:
        Nothing
    
    """

    print 'Start --> %s' % datetime.now()

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
    all_stations = cdec.historical.get_stations()
    
    # have to check the crappy CDEC datafile
    all_stations = all_stations[all_stations['junk'].isnull()]
    all_stations['lat'] = pd.to_numeric(all_stations['lat'], errors='coerce')
    all_stations['lon'] = pd.to_numeric(all_stations['lon'], errors='coerce')
    
    bbox = [np.abs(float(b)) for b in bbox.split(',')]
    
    ind = (all_stations['lon'] <= bbox[0]) & (all_stations['lon'] >= bbox[2]) & \
        (all_stations['lat'] >= bbox[1]) & (all_stations['lat'] <= bbox[3])
    
    station_id = all_stations.index[ind].tolist()
    
    
    # prepare other querys
    qryDate = "SELECT max(date_time) AS d FROM tbl_raw_data WHERE station_id='%s'"
    
    #------------------------------------------------------------------------------ 
    # go through each station and grab the data
    
    # pst_tz = pytz.timezone('America/Metlakatla')
    pst_tz = pytz.timezone('US/Pacific')
    utc_tz = pytz.timezone('UTC')
    
    for s in station_id:
        
        #------------------------------------------------------------------------------ 
        # determine the last date
        cursor.execute(qryDate % s)
        d = cursor.fetchall()
                
#         print 'Downloading %s --> %s ...' % (s, startTime)
            
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
