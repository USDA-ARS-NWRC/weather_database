"""
Load historic data from NRCS that was obtained with their get_historic.sh script
which downloads a CSV file of hourly data if available. This will process all CSV
files that are within the directory provided. Therefore, the utility must be ran
multiple times for multiple years.

If multiple sensors exist, only the first one will be used.

20160916 Scott Havens
"""

import sys, os
import pandas as pd
import glob
import pytz
import mysql.connector
from mysql.connector import errorcode

# connect to db, returns connection cnx
execfile(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'database_connect.py'))

# NRCS uses numbers so link them to the Mesowest NRCS station
sitenum2meso = {
                306: 'ATAI1',
                312: 'BASI1',
                382: 'CCDI1',
                423: 'COZI1',
                450: 'DHDI1',
                489: 'GLNI1',
                490: 'GLSI1',
                496: 'GGSI1',
                550: 'JKPI1',
                637: 'MRKI1',
                674: 'ORRI1',
                704: 'PRAI1',
                769: 'SRSI1',
                830: 'TRMI1',
                845: 'VNNI1',
                978: 'BOGI1',
                }

# the histroic data isn't complete with wind and solar so we'll
# still have to use the Mesowest data
# NOTE: the spaces have been removed from the NRCS names
vars2meso = {
             'WTEQ.I-1(in)': 'snow_water_equiv',
             'PREC.I-1(in)': 'precip_accum',
             'TOBS.I-1(degC)': 'air_temp',
             'SNWD.I-1(in)': 'snow_depth',
             'WDIRV.H-1(degr)': 'wind_direction',
             'WSPDX.H-1(mph)': 'wind_gust',
             'WSPDV.H-1(mph)': 'wind_speed',
             'RHUMV.H-1(pct)': 'relative_humidity',
             'SRADV.H-1(watt)': 'solar_radiation',
             'Date_Time': 'date_time',
             'SiteId': 'station_id'
             }

# units which are multiplied to the current values
units = {
             'snow_water_equiv': 25.4,
             'precip_accum': 25.4,
             'air_temp': 1,
             'snow_depth': 25.4,
             'wind_direction': 1,
             'wind_gust': 0.44704,
             'wind_speed': 0.44704,
             'relative_humidity': 1,
             'solar_radiation': 1,
             }


# NRCS uses PST timezone for all stations, therefore, it's always 8 hours
# behind UTC
date_offset = pd.DateOffset(hours=8)


def load_historic(fileName):
    """
    Load in the historic file. Dates are in PST!
    
    Args:
        fileName: file name of the csv file to load
    """
    
    print fileName
    
    cursor = cnx.cursor()
    
    # read in the file and ignore the first line
    data = pd.read_csv(fileName, header=1, parse_dates=[['Date', 'Time']])
    
    
    # remove the spaces and rename the columns
    data.rename(columns=lambda x: x.replace(" ",""), inplace=True)
    data.rename(columns=vars2meso, inplace=True)
    
    # filter to only the columns that we need
    idx = data.columns.isin(vars2meso.values())
    data = data.loc[:,idx]
    
    # convert from PST to UTC and convert to string
    data['date_time'] = data['date_time'] + date_offset
    data['date_time'] = data['date_time'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
    
    # filter the dataframe to only the stations we'd like to save
    data = data[data['station_id'].isin(sitenum2meso.keys())]
    
    # if there is some data left
    if not data.empty:
        
        columns = data.columns
        UPDATE = []
        for c in columns:
            UPDATE.append("%s=VALUES(%s)" % (c,c))
        UPDATE = ','.join(UPDATE)
        
        # convert from English units to Metric
        data[data == -99.9] = pd.np.NaN
        for key, val in units.items():
            if key in columns:
                data[key] *= val
            
        
        
        N = len(data)
        s = data['station_id'].unique()[0]
        
        VALUES = []
        
        try:
                
            print 'Adding %s data to database, %i records...' % (s, N)
            
            
            for i in range(N):
                
                # the current record
                record = data.iloc[i]
                                
                # get all the values
                values = record.values.tolist()
                for i,vi in enumerate(values):
                    if pd.isnull(vi):
                        values[i] = 'NULL'
                    else:
                        values[i] = "'%s'" % str(vi)

                VALUES.append("(%s)" % ",".join(values))        
             
            VALUES = ','.join(VALUES)           
                                         
            add_data = "INSERT INTO tbl_raw_data (%s) VALUES %s ON DUPLICATE KEY UPDATE %s" % \
                (",".join(columns), VALUES, UPDATE)
                
            add_data2 = "INSERT INTO tbl_level1 (%s) VALUES %s ON DUPLICATE KEY UPDATE %s" % \
                (",".join(columns), VALUES, UPDATE)
            
            
            cursor.execute(add_data)
            cursor.execute(add_data2)
                
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
            print 'Error loading data into database for %s: ' % (s)
        
        
            
 
def load_metadata():
    """
    Copy the metadata from the Mesowest stations and replace the station_id
    """   
    
    print 'metadata'
    
    
    cursor = cnx.cursor(dictionary=True)
    
    for sitenum,meso in sitenum2meso.items():
    
        cursor.execute("SELECT * FROM tbl_metadata WHERE primary_id='%s'" % meso)
        d = cursor.fetchall()[0] # return dict
    
        print "Updating metadata for %i (%s)" % (sitenum, d['station_name'])
        
        # start playing with the data
        d['primary_id'] = str(sitenum)
        d['secondary_id'] = meso
        d['source'] = 'NRCS'
        
        UPDATE = ','.join(["%s=VALUES(%s)" % (c,c) for c in d.keys()])
            
        try:
            add_data = "INSERT INTO tbl_metadata (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s" % \
                                (",".join(d.keys()), ','.join(['%s' for i in d.values()]), UPDATE)
                                
            cursor.execute(add_data, d.values())
            
            # insert into the BRB_bbox client
            cols = ['station_id','client','source']
            up = ','.join(["%s=VALUES(%s)" % (c,c) for c in cols])
            values = [str(sitenum), 'BRB_bbox', 'NRCS']    
            
            add_station = "INSERT INTO tbl_stations (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s" % \
                (",".join(cols), ','.join(['%s' for i in cols]), up)
                
            cursor.execute(add_station, values)
            
            
            
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
            print 'Error loading data into database for %s: ' % (sitenum)
    
    
if __name__ == '__main__':
    
    
    # get the arguments, should be just the directory
    d = sys.argv[1]
    
    if d == 'metadata':
        load_metadata()
        
    else:
    
        p = os.path.join(d, '*.csv')
        
        for fileName in glob.glob(p):
            load_historic(fileName)
            
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
