"""
Interact with California Data Exchange Center (CDEC) to get metadata and timeseries data
"""

import logging
import pandas as pd
from datetime import datetime
import pytz
import grequests
import requests
import json
import re
import utils

import sys
if sys.version_info[0] < 3: 
    from StringIO import StringIO
    from urlparse import urlparse, parse_qs
else:
    # Python 3
    from urllib.parse import urlparse, parse_qs
    from io import StringIO
    
    
__author__ = "Scott Havens"
__maintainer__ = "Scott Havens"
__email__ = "scott.havens@ars.usda.gov"
__date__ = "2017-08-03"

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

def escape_column(s):
    return re.sub(r'[^\x00-\x7F]+',' ', s)

class CDEC():
    """
    CDEC class to interact with the CDEC website.
    
    Args:
        db: :class:`~wxcb.database.Database` instance to use for inserting data
        config: the `[mesoset_*]` section of the config file. These are parameters
            for the Mesowest API and will get passed directly to Mesowest.
    """
        
    conversion = {
        'LATITUDE': 'latitude',
        'STATION_ID': 'primary_id',
        'LONGITUDE': 'longitude',
        'STATION_NAME': 'station_name',
        'ELEVATION': 'elevation',
        'AGENCY_NAME': 'primary_provider'
        }
        
    metadata_table = 'tbl_metadata'
    
    station_info_url = 'http://cdec.water.ca.gov/cdecstation2/CDecServlet/getStationInfo'
    
    data_csv_url = 'http://cdec.water.ca.gov/cgi-progs/queryCSV'
    
    timezone = 'Etc/GMT-8' # timezone that the data is retrieved in
    
    # sensor mapping 'LONG NAME' : {sensor number, database column}
    sensor_metadata = {
        'PRECIPITATION, ACCUMULATED': {
            'num': 2, 
            'col': 'precip_accum',
            'units': 'inches'},
        'SNOW, WATER CONTENT': {
            'num': 3,
            'col': 'snow_water_equiv',
            'units': 'inches'},
        'TEMPERATURE, AIR':{
            'num': 4,
            'col': 'air_temp',
            'units': 'deg_f'},
        'WIND, SPEED': {
            'num': 9,
            'col': 'wind_speed',
            'units': 'mph'},
        'WIND, DIRECTION': {
            'num': 10,
            'col': 'wind_direction',
            'units': 'degree'},
        'RELATIVE HUMIDITY': {
            'num': 12,
            'col': 'relative_humidity',
            'units': 'pct'},
        'SNOW DEPTH': {
            'num': 18,
            'col': 'snow_depth',
            'units': 'inches'},
        'SOLAR RADIATION ': {
            'num': 26,
            'col': 'solar_radiation',
            'units': 'w/m**2'},
        'SOLAR RADIATION AVG ': {
            'num': 103,
            'col': 'solar_radiation',
            'units': 'w/m**2'}
        }
    
    def __init__(self, db, config=None, quality_control=False):
        self._logger = logging.getLogger(__name__)
        
        self.db = db
        self.config = config
        self.quality_control = quality_control
        
        self.units = {val['col']: val['units'] for key,val in self.sensor_metadata.items()}
        
        self._logger.debug('Initialized CDEC')
        
    def single_station_info(self, stid):
        """
        Query the station info, returns the sensor information as well
        
        Args:
            stid: a single station ID's to fetch
        
        Returns:
            Dataframe for station that will have all the sensors names, and 
            timescale 
        """
        r = requests.get('http://cdec.water.ca.gov/cdecstation2/CDecServlet/getStationInfo',
                         params={'stationID': stid})
        data = json.loads(r.text)
        
        return pd.DataFrame(data['STATION'])
    

    def multi_station_info(self, stids, fields=False):
        """
        Query the station info for a list of stations. Because of the many
        small requests, use async grequests to request all at once.
        
        Args:
            stids: list of station ID's to fetch
            fields: True to return all data or False to return the first record
            
        Returns:
            DataFrame of the station info
        """
        
        # build the requests
        r = []
        for s in stids:
            r.append(grequests.get(self.station_info_url, params={'stationID': s}))
            
        # Because the CDEC site sucks, we have to really thottle the number of 
        # concurrent requests...
        res = grequests.map(r, size=1)
        
        # go through the responses and parse
        df = []
        for rs in res:
            if rs:
                if rs.status_code == 200:
                    try:
                        data = json.loads(rs.text)
                        d = pd.DataFrame(data['STATION'])
                        if fields:
                            df.append(d)
                        else:
                            df.append(d.iloc[0])
                        self._logger.debug('Got metadata for {}'.format(d['STATION_ID'][0]))
                    except Exception:
                        pass
#                         self._logger.debug('Metadata problem for {} - {}'.format(d['STATION_ID'][0], e))
                        
        
        return pd.concat(df, axis=1).T.reset_index()
        
    def metadata(self):
        """
        Retrieve the metadata from Mesowest. Two calls are made to the Mesowest API.
        The first call is to get the networks in order to determine the `network` and
        `primary_provider`. The second call retrieves the metadata given the `config`
        parameters. The two dataframes are combined and inserted into the database
        given the :class:`~wxcb.database.Database` instance.
        """
        
        self._logger.info('Obtaining metadata form CDEC')
                
        # this JSON file only has all the station names     
#         r = urllib.request.urlopen('http://cdec.water.ca.gov/cdecstation2/CDecServlet/getAllStations')
#         data = json.loads(r.read().decode(r.info().get_param('charset') or 'utf-8'))
        
        r = requests.get('http://cdec.water.ca.gov/cdecstation2/CDecServlet/getAllStations')
        data = json.loads(r.text)
        df = pd.DataFrame(data['STATION'])
        
        # request the data for all stations
        info = self.multi_station_info(df['STATION_ID'])
        
        # merge the dataframes, not needed since info has everything
#         d = pd.merge(df['STATION_ID'].to_frame(), info, on='STATION_ID')
                          
        # pull out the data from CDEC into the database format
        DF = pd.DataFrame()
        for c in self.conversion:
            DF[self.conversion[c]] = info[c]

        # these are the reported lat/long's for the station that may get changed
        # down the road due to location errors
        DF['reported_lat'] = DF['latitude']
        DF['reported_long'] = DF['longitude']
         
        # add the source to the DF
        DF['source'] = 'cdec'
        DF['state'] = 'CA'
        DF['timezone'] = self.timezone # need to check with Andrew
         
        DF = DF.where((pd.notnull(DF)), None)
        
        # Since there are still problems with the returned metadata, we need to clean
        # up for any potential UTF-8 characters that made it through
        DF['station_name'] = DF['station_name'].apply(escape_column)
        
        # insert the dataframe into the database
        self.db.insert_data(DF, description='CDEC metadata', metadata=True)
        
    def data(self, duration='H'):
        """
        Retrieve the hourly data from CDEC. Build a list of the URL's that 
        need to be fetched and use grequests to fetch the data. Then the
        data retrieval will be significantly sped up.
        """
        
        self.db.db_connect()
        
        # deteremine the client/s for processing
        client = self.config['client']
        self._logger.info('Client for CDEC data collection: {}'.format(client))
         
        # query to get the mesowest stations for the given client
        qry = "SELECT primary_id FROM tbl_stations_view WHERE source='cdec' AND client='{}'"
        cursor = self.db.cnx.cursor()
         
        # get the current local time
        endTime = utils.get_end_time(self.config['timezone'], self.config['end_time'], self.timezone)
        
        # create timezone objects
        mnt = pytz.timezone(self.config['timezone'])
        pst = pytz.timezone(self.timezone)
        
        # if a start time is specified localize it and convert to UTC
        if self.config['start_time'] is not None:
            startTime = pd.to_datetime(self.config['start_time'])
            startTime = mnt.localize(startTime)
            startTime = startTime.tz_convert(self.timezone)
        
        # go through each client and get the stations
        for cl in client:
            self._logger.info('Building URLs for client {}'.format(cl))
            
            cursor.execute(qry.format(cl))
            stations = cursor.fetchall()
            stations = [s[0] for s in stations]
            
            # go through each and get the data
            req = []
            for stid in stations:
                        
                if self.config['start_time'] is None:        
                    # determine the last value for the station
                    qry = "SELECT max(date_time) + INTERVAL 1 MINUTE AS d FROM tbl_level0 WHERE station_id='%s'" % stid
                    cursor.execute(qry)
                    startTime = cursor.fetchone()[0]                 
                
                    if startTime is not None:
                        startTime = pd.to_datetime(startTime, utc=True)
                    else:             
                        # start of the water year, do a PST time       
                        wy = utils.water_day(endTime, self.timezone)
                        startTime = pd.to_datetime(datetime(wy-1, 10, 1), utc=False)
                        startTime = pst.localize(startTime)
                 
                # determine what sensors to retreive and filter to duration
                sens = self.single_station_info(stid)
                sens = sens[sens.DUR_CODE == duration]
                 
                self._logger.debug('Building url for station {} between {} and {}'.format(
                    stid, startTime.strftime('%Y-%m-%d'), endTime.strftime('%Y-%m-%d'))) 
                
                # build the url's for each sensor
                for s in self.sensor_metadata.keys():
                    if sens.SENS_LONG_NAME.str.contains(s).any():
                        p = {}
                        p['station_id'] = stid
                        p['sensor_num'] = self.sensor_metadata[s]['num'] 
                        p['dur_code'] = duration
                        p['start_date'] = startTime.strftime('%Y-%m-%d')
                        p['end_date'] = endTime.strftime('%Y-%m-%d')
                        
                        req.append(grequests.get(self.data_csv_url, params=p))
                        
        # close the db connection since the data retrieval might take a while 
        self.db.db_close()
            
        # send the requests to CDEC
        self._logger.info('Sending {} requests to CDEC'.format(len(req)))
        res = grequests.map(req)
        
        # parse the responses
        data, av = self.cdec2df(res, stations) 

        # insert into the database
        for stid in stations:
            if data[stid] is not None:
                self.db.insert_data(data[stid], 'data', description='CDEC data for {}'.format(stid))
            if av[stid] is not None:
                self.db.insert_data(av[stid], 'avg_del', description='CDEC data for {} averaged'.format(stid))

        
    def cdec2df(self, res, stations):
        """
        Parse a list of responses from CDEC into dataframes
        
        Args:
            res: list of grequest responses
            
        Returns:
            dict of dataframes, one for each site
        """
        
        data = {}
        for stid in stations:
            data[stid] = []
        
        count = 0
        for rs in res:
            if rs:
                if rs.status_code == 200:
                    try:
                        
                        # determine the station and sensor from the url
                        stid, sens_name = self.parse_url(rs.url)
                        
                        # read in the response
                        df = pd.read_csv(StringIO(rs.text), skiprows=2, header=None, parse_dates=[[0,1]], index_col=None, na_values='m')
                        df.columns = ['date_time', sens_name]
                        df.set_index('date_time', inplace=True)
                        
                        if sens_name is not None:
                            data[stid].append(df)

                        self._logger.debug('Got data for {} - {}'.format(stid, sens_name))
                        count += 1
                    except Exception:
                        pass
                    
        self._logger.info('Retrieved {} good responses form CDEC'.format(count))
                    
        # merge the data frames together and get ready for the database
        av = {}
        for stid in stations:
            if len(data[stid]) > 0:
                data[stid] = pd.concat(data[stid], axis=1)
                data[stid].dropna(axis=0, how='all', inplace=True)
                
                # convert the units
                data[stid] = utils.convert_units(data[stid], self.units)
                
                # truncate and add fields
                data[stid] = data[stid].truncate(data[stid].first_valid_index().ceil('H'),
                                                 data[stid].last_valid_index().floor('H'))
                data[stid]['station_id'] = stid
                data[stid]['date_time'] = data[stid].index.strftime('%Y-%m-%d %H:%M')
                
                # average the dataframe
                av[stid] = utils.average_df(data[stid], stid)
        
            else:
                data[stid] = None
                av[stid] = None
            
        return data, av
        
    
    def parse_url(self, url):
        """
        Parse the url that was passed to CDEC. Pull out the station
        id and the sensor name
        
        Args:
            url: url string with parameters
            
        Returns:
            station id and sensor name from sensor_metadata, None if not found
        """
        
        o = urlparse(url)
        query = parse_qs(o.query)
        stid = query['station_id'][0]
        sens_num = int(query['sensor_num'][0])
        
        sens_name = None
        for v in self.sensor_metadata.values():
            if sens_num == v['num']:
                sens_name = v['col']
                break
            
        return stid, sens_name
        

  
    

