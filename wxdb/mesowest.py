"""
Interact with the Mesowest API to get metadata and timeseries data
"""

import logging
from MesoPy import Meso
import pandas as pd
from datetime import datetime
import pytz
import grequests
import json

import utils

import sys
if sys.version_info[0] < 3: 
    from urlparse import urlparse, parse_qs
else:
    # Python 3
    from urllib.parse import urlparse, parse_qs

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
    
    mesowest_timeseries_url = 'http://api.mesowest.net/v2/stations/timeseries'
    
    def __init__(self, db, config, quality_control=False):
        self._logger = logging.getLogger(__name__)
        
        self.db = db
        self.config = config
        self.qc = quality_control
        
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
        
        # calculate the UTM coordinates
        DF['utm_x'], DF['utm_y'], DF['utm_zone'] = zip(*DF.apply(utils.df_utm, axis=1))
        
        # add the source to the DF
        DF['source'] = 'mesowest'
        
        DF = DF.where((pd.notnull(DF)), None)
        
        # insert the dataframe into the database
        self.db.insert_data(DF, 'metadata', description='Mesowest metadata')
        
        
        
    def data(self):
        """
        Retrieve the data from Mesowest. Build a list of the URL's that 
        need to be fetched and use grequests to fetch the data. Then the
        data retrieval will be significantly sped up.
        """
        
        req = self.build_timeseries_url()
        
        # send the requests to CDEC
        self._logger.info('Sending {} requests to Mesowest API'.format(len(req)))
        res = grequests.map(req)
          
        count = 0      
        for rs in res:
            if rs:
                if rs.status_code == 200:
                    try:
                        data = json.loads(rs.text)
                        df, av = self.meso2df(data)
                        self.db.insert_data(df, 
                                            loc='data',
                                            description='Mesowest data for {}'.
                                            format(df.iloc[0].station_id))
                        self.db.insert_data(av, 
                                            loc='avg_del',
                                            description='Mesowest data for {} averaged'.
                                            format(df.iloc[0].station_id))
                        count += 1
                    except Exception:
                        # the data doest have anything in it
                        q = self.parse_url(rs.url)
                        self._logger.warn('{} - {}'.format(q['stid'][0],data['SUMMARY']['RESPONSE_MESSAGE']))
                        
                    
                        
        self._logger.info('Retrieved {} good responses form Mesowest'.format(count))
        
        
    def build_timeseries_url(self):
        """
        Build the url's to call Mesowest API and adds the url
        to the grequests.
        
        Returns:
            list of grequest.get objects
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
        
        # if a start time is specified localize it and convert to UTC
        if self.config['start_time'] is not None:
            startTime = pd.to_datetime(self.config['start_time'])
            startTime = mnt.localize(startTime)
            startTime = startTime.tz_convert('UTC')
        
        # go through each client and get the stations
        req = []
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
                    
                # build the URL's to retrieve the data
                self._logger.debug('Building url for station {} between {} and {}'.format(
                    stid, startTime.strftime('%Y-%m-%d %H:%M'), endTime.strftime('%Y-%m-%d %H:%M')))
                p = self.timeseries_params(startTime, endTime, stid)
                
                req.append(grequests.get(self.mesowest_timeseries_url, params=p))
                
        cursor.close()
        self.db.db_close()
        
        return req
        
    def timeseries_params(self, startTime, endTime, stid):
        """
        Call Mesowest for the data in bbox between startTime and endTime
        """
        
        # set the parameters for the Mesowest query and build
        p = self.params.copy()
        p['start'] = startTime.strftime('%Y%m%d%H%M')      # start time
        p['end'] = endTime.strftime('%Y%m%d%H%M')          # end time
        p['stid'] = stid
        
        return p
    
    def meso2df(self, data):
        """
        Parse the Mesowest retuned data and parse the output into
        a pandas dataframe
        
        Args:
            data: dict returned from Mesowest
            
        Returns:
            Tuple, DataFrame for the returned values from Mesowest and
            a quality controled DataFrame with bad values removed
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
        r = pd.DataFrame(s['OBSERVATIONS'],columns=s['OBSERVATIONS'].keys())  # preserve column order
        
        # truncate the dataframe to ensure that there isn't a leak over to the next hour
        r['date_time'] = pd.to_datetime(r['date_time']) 
        r.set_index('date_time', inplace=True)
        r = r.truncate(r.first_valid_index().ceil('H'), r.last_valid_index().floor('H'))
        
        # convert to date_time from the returned format to MySQL format
        r['date_time'] = r.index.strftime('%Y-%m-%d %H:%M')
        
        # rename the columns and make date time the index    
        r.rename(columns = v, inplace=True)
#         r = r.set_index('date_time')
        vkeep = r.columns.isin(var)
        r = r[r.columns[vkeep]]  # only take those that we wanted in case something made it through
        
        # add the station_id
        r['station_id'] = station_id
        
        # perform average on the dataframe
        df = utils.average_df(r, station_id)
        
        # quality control
        if self.qc:
            df = self.qc.run(df)
        
        return r, df

    def parse_url(self, url):
        """
        Parse the url that was passed to Mesowest. Pull out the station id
        
        Args:
            url: url string with parameters
            
        Returns:
            query parameter dictionary 
        """
        
        o = urlparse(url)
        return parse_qs(o.query)
        

