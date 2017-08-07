"""
Interact with the Mesowest API to get metadata and timeseries data
"""

import logging
from MesoPy import Meso
import pandas as pd
from datetime import datetime
import pytz

import utils

__author__ = "Scott Havens"
__maintainer__ = "Scott Havens"
__email__ = "scott.havens@ars.usda.gov"
__date__ = "2017-07-27"


class Mesowest():
    """
    Mesowest class to interact with the Mesowest API. The objective is to
    use MesoPy to get the metadata and timeseries station data given the stations
    in the database.
    
    Args:
        db: :class:`~wxcb.database.Database` instance to use for inserting data
        config: the `[mesoset_*]` section of the config file. These are parameters
            for the Mesowest API and will get passed directly to Mesowest.
    """
    
    token = 'e505002015c74fa6850b2fc13f70d2da'
    
    conversion = {
        'LATITUDE': 'latitude',
        'MNET_ID': 'network',
        'STID': 'primary_id',
        'LONGITUDE': 'longitude',
        'NAME': 'station_name',
        'ELEVATION': 'elevation',
        'STATE': 'state',
        'TIMEZONE': 'timezone'
        }
    
    network_conversion = {
        'LONGNAME': 'primary_provider',
        'SHORTNAME': 'network'
        }
    
    metadata_table = 'tbl_metadata'
    
    def __init__(self, db, config):
        self._logger = logging.getLogger(__name__)
        
        self.db = db
        self.config = config
        
        p = {}
        p['start'] = None      # start time
        p['end'] = None          # end time
        p['obstimezone'] = 'utc'    # observation time zone
        p['units'] = 'metric'       # units
        p['stid'] = None
        p['bbox'] = None
        p['token'] = self.token     # API token
        p['vars'] = 'air_temp,dew_point_temperature,relative_humidity,wind_speed,wind_direction,wind_gust,solar_radiation,snow_smoothed,precip_accum,snow_depth,snow_accum,precip_storm,snow_interval,snow_water_equiv'
         
        self.params = p
        
        self._logger.debug('Initialized Mesowest')
        
    def metadata(self):
        """
        Retrieve the metadata from Mesowest. Two calls are made to the Mesowest API.
        The first call is to get the networks in order to determine the `network` and
        `primary_provider`. The second call retrieves the metadata given the `config`
        parameters. The two dataframes are combined and inserted into the database
        given the :class:`~wxcb.database.Database` instance.
        """
        
        self._logger.info('Obtaining metadata form Mesowest')
        m = Meso(token=self.token)
        
        # get the networks associated with the metadata
        networks = m.networks()
        net = pd.DataFrame(networks['MNET'])
        net['MNET_ID'] = net['ID']
                
        # get metadata from Mesowest
        self._logger.debug('Obtaining metadata for {}'.format(self.config))
        meta = m.metadata(**self.config)
        
        # add the networks to the meta dataframe
        mdf = pd.DataFrame(meta['STATION'])
        mdf = pd.merge(mdf, net, on='MNET_ID')
        
        # pull out the data from Mesowest into the database format
        DF = pd.DataFrame()
        for c in self.conversion:
            DF[self.conversion[c]] = mdf[c]
        
        # fill in the network conversion
        for n in self.network_conversion:
            DF[self.network_conversion[n]] = mdf[n]
        
        # these are the reported lat/long's for the station that may get changed
        # down the road due to location errors
        DF['reported_lat'] = DF['latitude']
        DF['reported_long'] = DF['longitude']
        
        # add the source to the DF
        DF['source'] = 'mesowest'
        
        DF = DF.where((pd.notnull(DF)), None)
        
        # insert the dataframe into the database
        self.db.insert_data(DF, description='Mesowest metadata', metadata=True)
        
        
        
    def data(self):
        """
        Retrieve the data from Mesowest. 
        """
        
        self.db.db_connect()
        
        # deteremine the client/s for processing
        client = self.config['client']
        self._logger.info('Client for Mesowest data collection: {}'.format(client))
        
        # query to get the mesowest stations for the given client
        qry = "SELECT primary_id FROM tbl_stations_view WHERE source='mesowest' AND client='{}'"
        cursor = self.db.cnx.cursor()
        
        # get the current local time
        endTime = utils.get_end_time(self.config['timezone'], self.config['end_time'])
        mnt = pytz.timezone(self.config['timezone'])
#         if self.config['end_time'] is None:
#             endTime = pd.Timestamp('now')
#         else:
#             endTime = pd.to_datetime(self.config['end_time'])
#         endTime = mnt.localize(endTime)
#         endTime = endTime.tz_convert('UTC')
        
        # if a start time is specified localize it and convert to UTC
        if self.config['start_time'] is not None:
            startTime = pd.to_datetime(self.config['start_time'])
            startTime = mnt.localize(startTime)
            startTime = startTime.tz_convert('UTC')
        
        # go through each client and get the stations
        for cl in client:
            self._logger.info('Retrieving current data for client {}'.format(cl))
            
            cursor.execute(qry.format(cl))
            stations = cursor.fetchall()
            
            # go through each and get the data
            for stid in stations:
                stid = stid[0]
                        
                if self.config['start_time'] is None:        
                    # determine the last value for the station
                    qry = "SELECT max(date_time) + INTERVAL 1 MINUTE AS d FROM tbl_level0 WHERE station_id='%s'" % stid
                    cursor.execute(qry)
                    startTime = cursor.fetchone()[0]                 
                
                    if startTime is not None:
                        startTime = pd.to_datetime(startTime, utc=True)
                    else:             
                        # start of the water year, do a local time then convert to UTC       
                        wy = utils.water_day(endTime, self.config['timezone'])
                        startTime = pd.to_datetime(datetime(wy-1, 10, 1), utc=False)
                        startTime = mnt.localize(startTime)
                        startTime = startTime.tz_convert('UTC')
                   
                self._logger.debug('Retrieving data for station {} between {} and {}'.format(
                    stid, startTime.strftime('%Y-%m-%d %H:%M'), endTime.strftime('%Y-%m-%d %H:%M'))) 
                data = self.currentMesowestData(startTime, endTime, stid)
#                  
                if data is not None:
                    df = self.meso2df(data)
                    self.db.insert_data(df, description='Mesowest current data', data=True)
        
        
        cursor.close()
        self.db.db_close()
        
    def currentMesowestData(self, startTime, endTime, stid):
        """
        Call Mesowest for the data in bbox between startTime and endTime
        """
        
        # set the parameters for the Mesowest query and build
        p = self.params
        p['start'] = startTime.strftime('%Y%m%d%H%M')      # start time
        p['end'] = endTime.strftime('%Y%m%d%H%M')          # end time
        p['stid'] = stid
        
        m = Meso(token=p['token'])
        
        try:
            data = m.timeseries(start=p['start'], end=p['end'], obstimezone=p['obstimezone'],
                                        stid=p['stid'], units=p['units'], vars=p['vars'])
        except Exception as e:
            self._logger.warn('{} -- {}'.format(stid, e))
            data = None
        
        return data
    
    def meso2df(self, data):
        """
        Parse the Mesowest retuned data and parse the output into
        a pandas dataframe
        
        Args:
            data: dict returned from Mesowest
        """
        s = data['STATION'][0]
        
        # determine station id
        station_id = str(s['STID'])
        
        # map the variables that where returned with what the names are
        var = s['SENSOR_VARIABLES'].keys()
        v = {}
        for i in s['SENSOR_VARIABLES']:
            if s['SENSOR_VARIABLES'][i]:
                v[list(s['SENSOR_VARIABLES'][i].keys())[0]] = i
                     
        # convert to dataframe
        r = pd.DataFrame(s['OBSERVATIONS'], columns=s['OBSERVATIONS'].keys())  # preserve column order
        
        # convert to date_time from the returned format to MySQL format
        r['date_time'] = pd.to_datetime(r['date_time']) 
        r['date_time'] = r['date_time'].dt.strftime('%Y-%m-%d %H:%M')
        
        # rename the columns and make date time the index    
        r.rename(columns = v, inplace=True)
#         r = r.set_index('date_time')
        vkeep = r.columns.isin(var)
        r = r[r.columns[vkeep]]  # only take those that we wanted in case something made it through
        
        # add the station_id
        r['station_id'] = station_id
            
        return r


