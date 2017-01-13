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
import pytz

import json

nthreads = 3

# connect to db, returns connection cnx
cnx = None

    
    
p = {}
p['start'] = None      # start time
p['end'] = None          # end time
p['obstimezone'] = 'utc'    # observation time zone
p['units'] = 'metric'       # units
p['stid'] = None
p['bbox'] = None
p['token'] = 'e505002015c74fa6850b2fc13f70d2da'     # API token
p['vars'] = 'air_temp,dew_point_temperature,relative_humidity,wind_speed,wind_direction,wind_gust,solar_radiation,snow_smoothed,precip_accum,snow_depth,snow_accum,precip_storm,snow_interval,snow_water_equiv'
     
        
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
#     p = {}
    p['start'] = startTime.strftime('%Y%m%d%H%M')      # start time
    p['end'] = endTime.strftime('%Y%m%d%H%M')          # end time
#     p['obstimezone'] = 'utc'    # observation time zone
#     p['units'] = 'metric'       # units
    p['bbox'] = bbox
#     p['token'] = 'e505002015c74fa6850b2fc13f70d2da'     # API token
#     p['vars'] = 'air_temp,dew_point_temperature,relative_humidity,wind_speed,wind_direction,wind_gust,solar_radiation,snow_smoothed,precip_accum,snow_depth,snow_accum,precip_storm,snow_interval,snow_water_equiv' 
    
    m = Meso(token=p['token'])
    
    print 'Downloading data ...' 
    
    data = m.timeseries(start=p['start'], end=p['end'], obstimezone=p['obstimezone'],
                                bbox=p['bbox'], units=p['units'], vars=p['vars'])
    
    return data

def currentMesowestData(startTime, endTime, stid):
    """
    Call Mesowest for the data in bbox between startTime and endTime
    """
    
    #------------------------------------------------------------------------------ 
    # set the parameters for the Mesowest query and build
#     p = {}
    p['start'] = startTime.strftime('%Y%m%d%H%M')      # start time
    p['end'] = endTime.strftime('%Y%m%d%H%M')          # end time
#     p['obstimezone'] = 'utc'    # observation time zone
#     p['units'] = 'metric'       # units
    p['stid'] = stid
#     p['token'] = 'e505002015c74fa6850b2fc13f70d2da'     # API token
#     p['vars'] = 'air_temp,dew_point_temperature,relative_humidity,wind_speed,wind_direction,wind_gust,solar_radiation,snow_smoothed,precip_accum,snow_depth,snow_accum,precip_storm,snow_interval,snow_water_equiv' 
    
    m = Meso(token=p['token'])
    
#     print 'Downloading data ...' 
    try:
        data = m.timeseries(start=p['start'], end=p['end'], obstimezone=p['obstimezone'],
                                    stid=p['stid'], units=p['units'], vars=p['vars'])
    except Exception as e:
        print '%s -- %s' % (stid, e)
        data = None
    
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
    #     r = pd.DataFrame.from_dict(s['OBSERVATIONS'], orient='columns') # pk 1-9-17 replaced this line with
    r = pd.DataFrame(s['OBSERVATIONS'],columns=s['OBSERVATIONS'].keys())  # this one to preserve column order
    r['date_time'] = pd.to_datetime(r['date_time']) # convert to date_time
    
    # rename the columns and make date time the index    
    r.rename(columns = v, inplace=True)
    r = r.set_index('date_time')
    vkeep = r.columns.isin(var)
    r = r[r.columns[vkeep]]  # only take those that we wanted in case something made it through
    
    
    r = r.replace(np.nan, 'NULL', regex=True)   # replace NaN with NULL for MySQL
        
    #------------------------------------------------------------------------------ 
    # insert data into database
    N = len(r)  # number of measurements
    
#     d = r.index.tolist()
#     values = r.values
    
    
    try:
    
        print 'Adding %s data to database, %i records...' % (station_id, N)
                
                
        VALUES = []
        #         ivars = var[:]
        #         ivars.remove('date_time')
        ivars = v.values()
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
        
    
def downloadCurrentData(endTime):
    """
    Download the current data from Mesowest
    """
    
    # get database connection
    db_init()
    cursor = cnx.cursor()
    
    # determine the stations to download, this will only download from clients with '*WY'
    dd, wy = water_day(endTime)
    sqry = "SELECT station_id from tbl_stations WHERE client LIKE '%%%i' AND source='Mesowest'" % wy
    cursor.execute(sqry)
    stations = cursor.fetchall()
    
    # go through each and get the data
    for stid in stations:
        
        stid = stid[0]
        
        # determine the last value for the station
        qry = "SELECT max(date_time) + INTERVAL 1 MINUTE AS d FROM tbl_raw_data WHERE station_id='%s'" % stid
        cursor.execute(qry)
        startTime = cursor.fetchone()
        
        if startTime[0] is not None:
            startTime = pd.to_datetime(startTime[0], utc=True)
        else:
            dd, wy = water_day(endTime)
            startTime = pd.to_datetime(datetime(wy-1, 10, 1), utc=True)
        
        
        data = currentMesowestData(startTime, endTime, stid)
        
        if data is not None:
            getStationData(data['STATION'][0])
    
    
    
def water_day(indate):
    """
    Determine the decimal day in the water year
    
    Args:
        indate: datetime object
        
    Returns:
        dd: decimal day from start of water year
    
    20160105 Scott Havens
    """
    tp = indate.timetuple()
    
    # create a test start of the water year    
    test_date = datetime(tp.tm_year, 10, 1, 0, 0, 0)
    test_date = test_date.replace(tzinfo=pytz.timezone(indate.tzname()))
    
    # check to see if it makes sense
    if indate < test_date:
        wy = tp.tm_year
    else:
        wy = tp.tm_year + 1

    print "Water year %i" % wy

        
    # actual water year start
    wy_start = datetime(wy-1, 10, 1, 0, 0, 0)
    wy_start = wy_start.replace(tzinfo=pytz.timezone(indate.tzname()))
    
    # determine the decimal difference
    d = indate - wy_start
    dd = d.days + d.seconds/86400.0
    
    return dd, wy  
    

def arg_parse():
    """"""

    parser = argparse.ArgumentParser(
        description='Download Mesowest data',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--current', required=False, dest='current', action='store_true',
        help='Download the current data')
    parser.add_argument(
        '--start', required=False, dest='start',
        help='Start date')
    parser.add_argument(
        '--end', required=False, dest='end',
        help='End date')
    parser.add_argument(
        '--bbox', required=False, dest='bbox',
        help='Bounding box string "lonmin,latmin,lonmax,latmax"')
    parser.add_argument('config', metavar='N', type=str, nargs=1,
                    help='Database configuration JSON')
    
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    """
    python download_Mesowest_historic.py --start=2013-10-01 --end=2013-10-05 --bbox='-116.4,43.05,-114.45,44.44'
    """
    args = arg_parse()
    
    with open(args.config[0], 'r') as f:
        config = json.load(f)
        f.close()

    print 'Start --> %s' % datetime.now()
       
    # if there is a bbox, then its a histroic download
    if args.bbox is not None:
        # determine start and end
        startTime = pd.to_datetime(args.start) # start a day early to ensure that all values are grabed
        endTime = pd.to_datetime(args.end)
        
        print 'Downloading between %s and %s' % (startTime, endTime)
            
        downloadData(startTime, endTime, args.bbox)
    
    elif args.current:
        # download the current data
        endTime = pd.to_datetime(datetime.utcnow(), utc=True)
        downloadCurrentData(endTime)
        
    
    cnx.close()
    
    print 'Done --> %s' % datetime.now()
