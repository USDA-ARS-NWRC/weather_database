import logging
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from copy import copy
import pytz
import utils
from acid_core.autocleanfft import AutoCleanFFT
from acid_core.auto_cloudfactor import AutoCloudFactor

__author__ = "Scott Havens"
__maintainer__ = "Scott Havens"
__email__ = "scott.havens@ars.usda.gov"
__date__ = "2017-07-27"


class ACID():
    """
    Quality control class that will implemented qualtiy control measures.
    The database has a flag column that will get filled in using codes contained
    in the QC class. The codes will be applied broadly for the timestep and will
    not say which value is bad. Multiple codes can be applied to each row.

    Args:
        config: configuration section for the quality control
        data: DataFrame with columns mataching the ranges keys
        
    Returns:
        DataFrame with the data flagged in 'qc_flag' column
    
    """
    
    acid = {
        'air_temp': {
            'ninterp': 12,
            'scutoff': 0.8,
            'fcutoff': 0.5,
            'window': 24,
            'nfdays': 3,
            },
        'relative_humidity': {
            'ninterp': 12,
            'scutoff': 1.8,
            'fcutoff': 0.5,
            'window': 24,
            'nfdays': 3,
            },
        'wind_speed': {
            'ninterp': 3,
            'scutoff': 0.002,
            'fcutoff': 0.5,
            'window': 24,
            'nfdays': 3,
            },
        'wind_direction': {
            'ninterp': 6,
            'scutoff': 0.00005,
            'fcutoff': 0.8,
            'window': 24,
            'nfdays': 2,
            },
        'solar_radiation': {
            'ninterp': 12,
            'scutoff': 15,
            'fcutoff': 0.5,
            'window': 24,
            'nfdays': 3,
            }
        }
    
    acid_cf = {
        'window': 24*10,
        'scutoff': 50,
        'peakpm': 7,
        'peakfactor': 1.05
        }
    
    table_from = 'tbl_level1'
    table_to = 'tbl_level_auto'
    
    def __init__(self, config, db=None):
        self._logger = logging.getLogger(__name__)
        
        self.config = config
        self.db = db
        
        # number of days to prepend to the data call
        self.prepend = np.max(np.array([i['nfdays'] for key,i in self.acid.items()] + \
                                       [self.acid_cf['window']/24]))
        
        
    def run(self):
        """
        Perform the quality control measures
        """
        
        self.db.db_connect()
        
        # get stations from the client
        stations = self.retrieve_stations()
        
        # get the current local time
        endTime = utils.get_end_time(self.config['timezone'], self.config['end_time'])
        mnt = pytz.timezone(self.config['timezone'])
        
        # if a start time is specified localize it and convert to UTC
        if self.config['start_time'] is not None:
            startTime = pd.to_datetime(self.config['start_time'])
            startTime = mnt.localize(startTime)
            startTime = startTime.tz_convert('UTC')
            start_time_org = copy(startTime)
            startTime = startTime - pd.Timedelta(days=self.prepend)
            
        
        # go through each station
        for stid in stations:
            df = self.db.retrieve_station_data(stid, startTime, endTime)
            
            if not df.empty:
                try:
                    df.drop(['id'], axis=1, inplace=True)
                    
                    # each column and apply the cleaning
                    for key in df.columns:
                        if key in self.acid.keys():
                            df[key] = AutoCleanFFT(df[key], **self.acid[key])
                    
                    # after cleaning solar, calc cloud factor
                    if 'solar_radiation' in df.columns:
                        df['cloud_factor'] = AutoCloudFactor(df['solar_radiation'], **self.acid_cf)
                        
                
                    # truncate to the start and end times
                    df = df.truncate(start_time_org, endTime)
                    
                    # perform some extra calculations for vapor pressure
                    if ('air_temp' in df.columns) & ('relative_humidity' in df.columns):
                        df['vapor_pressure'] = utils.rh2vp(df['air_temp'], df['relative_humidity']/100.0) 
            
                    # convert to date_time from the returned format to MySQL format
                    df['date_time'] = df.index.strftime('%Y-%m-%d %H:%M')
                
                    if self.db:
                        # write out to the database
                        self.db.insert_data(df, 
                                            loc='auto',
                                            description='ACID data for {}'.
                                            format(stid))
                        
                except Exception as e:
                    self._logger.warn('Problem applying ACID to {}'.format(stid))
                    self._logger.warn(e)
        
        self.db.db_close()
              
    def retrieve_stations(self):
        """
        Retrieve the stations given a list of clients
        """
        
        # deteremine the client/s for processing
        client = self.config['client']
        self._logger.info('Client for ACID cleaning: {}'.format(client))
        
        # query to get the mesowest stations for the given client
        client_qry = "SELECT primary_id FROM tbl_stations_view WHERE client='{}'"
        cursor = self.db.cnx.cursor()
        
        stations = []
        for cl in client:
            self._logger.info('Retrieving stations for client {}'.format(cl))
            
            cursor.execute(client_qry.format(cl))
            stations += cursor.fetchall()
        
        # clean up the stations
        stations = [stid[0] for stid in stations]
        stations = list(set(stations)) # remove duplicate stations
        
        return stations
        
    def station_acid(self, data):
        """
        Perform auto cleaning of input data
        """
        
        stid = data.station_id[0]
        cols = data.columns
        r = pd.DataFrame(index=data.index)
        m = pd.DataFrame(index=data.index)
        
        for key, d in self.acid.items():
            if key in cols:
                autota = AutoCleanFFT(data[key], **d)
                data[key] = autota
                
        
        
        
        
        
        