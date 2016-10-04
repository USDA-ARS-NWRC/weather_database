'''
20160415 Scott Havens

Download from Mesowest for a bounding box

'''


import numpy as np
from datetime import datetime
import mysql.connector
from mysql.connector import errorcode
import os
from MesoPy import Meso
import pandas as pd
import argparse
import multiprocessing as mp

import json

nthreads = 3

# connect to db, returns connection cnx
cnx = None
with open('../db_config.json', 'r') as f:
    config = json.load(f)
    f.close()
     
        
def db_init():
    global cnx

    cnx = mysql.connector.connect(user=config['user'], 
                              password=config['password'],
                              host=config['host'],
                              database=config['db'])
    
#     execfile(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'database_connect.py'))

def MesowestData(startTime, endTime, bbox):
    """
    Call Mesowest for the data in bbox between startTime and endTime
    """
    
    #------------------------------------------------------------------------------ 
    # set the parameters for the Mesowest query and build
    p = {}
    p['start'] = startTime.strftime('%Y%m%d%H%M')      # start time
    p['end'] = endTime.strftime('%Y%m%d%H%M')          # end time
    p['obstimezone'] = 'utc'    # observation time zone
    p['units'] = 'metric'       # units
    p['bbox'] = bbox
    p['token'] = 'e505002015c74fa6850b2fc13f70d2da'     # API token
    p['vars'] = 'air_temp,dew_point_temperature,relative_humidity,wind_speed,wind_direction,wind_gust,solar_radiation,snow_smoothed,precip_accum,snow_depth,snow_accum,precip_storm,snow_interval,snow_water_equiv' 
    
    m = Meso(token=p['token'])
    
    print 'Downloading data ...' 
    
    data = m.timeseries(start=p['start'], end=p['end'], obstimezone=p['obstimezone'],
                                bbox=p['bbox'], units=p['units'], vars=p['vars'])
    
    return data


def getStationData(s):
    """
    get the station data into the database using the connection
    """
    
#     cnx1 = cnx.get_connection()
    cursor = cnx.cursor()

    # determine station id
    station_id = str(s['STID'])
    
    # variables that were returned
    var = s['SENSOR_VARIABLES'].keys()
    v = {}
    for i in s['SENSOR_VARIABLES']:
        if s['SENSOR_VARIABLES'][i]:
            v[s['SENSOR_VARIABLES'][i].keys()[0]] = i
        
    # columns to insert
    cols = v.keys()
    cols.append('station_id')
              
    # convert to dataframe
    r = pd.DataFrame.from_dict(s['OBSERVATIONS'], orient='columns')
    r['date_time'] = pd.to_datetime(r['date_time']) # convert to date_time
    
    # rename the columns and make date time the index    
    r.rename(columns = v, inplace=True)
    r = r[var]  # only take those that we wanted in case something made it through
    r = r.set_index('date_time')
    
    r = r.replace(np.nan, 'NULL', regex=True)   # replace NaN with NULL for MySQL
        
    #------------------------------------------------------------------------------ 
    # insert data into database
    N = len(r)  # number of measurements
    
#     d = r.index.tolist()
#     values = r.values
    
    
    try:
    
        print 'Adding %s data to database, %i records...' % (station_id, N)
                
                
        VALUES = []
        ivars = var[:]
        ivars.remove('date_time')
        
        for i in range(N):
            
            # the current record
            record = r.iloc[i]
            date_time = record.name
        
            # get all the values
            values = record.values.tolist()
            for i,vi in enumerate(values):
                if vi == 'NULL':
                    values[i] = 'NULL'
                elif type(values[i]) is not str:
                    values[i] = str(values[i])
        
            vi = ','.join(values)
            
            # the VALUES part of the insert for each row
            VALUES.append("('%s','%s',%s)" % (station_id, date_time.strftime("%Y-%m-%d %H:%M:%S"), vi))
        
        VALUES = ',\n'.join(VALUES)
        
        UPDATE = []
        for i in ivars:
            UPDATE.append("%s=VALUES(%s)" % (i, i))
        tbl1_UPDATE = UPDATE[:]
        UPDATE = ',\n'.join(UPDATE)
        
        tbl1_UPDATE.append('av_del=0')
        tbl1_UPDATE = ',\n'.join(tbl1_UPDATE)
        
        
        add_data = "INSERT INTO tbl_raw_data (station_id,date_time,%s) VALUES %s ON DUPLICATE KEY UPDATE %s" % \
            (",".join(ivars), VALUES, UPDATE)
            
        add_data2 = "INSERT INTO tbl_level1 (station_id,date_time,%s) VALUES %s ON DUPLICATE KEY UPDATE %s" % \
            (",".join(ivars), VALUES, tbl1_UPDATE)
        
        cursor.execute(add_data)
        cursor.execute(add_data2)
                        
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
        print 'Error loading data into database for %s (%s): ' % (station_id, date_time)
        
    cursor.close()
#     cnx1.close()
    

def downloadData(startTime, endTime, bbox):
    
    data = MesowestData(startTime, endTime, bbox)
                                
    print 'Total stations = %i' % len(data['STATION'])
    
    #------------------------------------------------------------------------------ 
    # go through each station and grab the data
    
    pool = mp.Pool(processes=nthreads, initializer=db_init)
    pool.map(getStationData, data['STATION'])
    pool.close()
    pool.join()
    
#     db_init()
#     for index,s in enumerate(data['STATION']):
#         getStationData(s)
        
    

    
    

def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Download Mesowest data',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--start', required=True, dest='start',
        help='Start date')
    parser.add_argument(
        '--end', required=True, dest='end',
        help='End date')
    parser.add_argument(
        '--bbox', required=True, dest='bbox',
        help='Bounding box string "lonmin,latmin,lonmax,latmax"')
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    """
    python download_Mesowest_historic.py --start=2013-10-01 --end=2013-10-05 --bbox='-116.4,43.05,-114.45,44.44'
    """
    args = arg_parse()

    print 'Start --> %s' % datetime.now()
       
    # determine start and end
    startTime = pd.to_datetime(args.start) # start a day early to ensure that all values are grabed
    endTime = pd.to_datetime(args.end)
    
    print 'Downloading between %s and %s' % (startTime, endTime)
        
    downloadData(startTime, endTime, args.bbox)
    
    cnx.close()
    
    print 'Done --> %s' % datetime.now()
