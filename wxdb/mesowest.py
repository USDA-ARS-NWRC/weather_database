"""
Interact with the Mesowest API to get metadata and timeseries data
"""

import logging
from MesoPy import Meso
import pandas

__author__ = "Scott Havens"
__maintainer__ = "Scott Havens"
__email__ = "scott.havens@ars.usda.gov"
__date__ = "2017-07-27"


class Mesowest():
    """
    Mesowest class to interact with the Mesowest API. The objective is to
    use MesoPy to get the metadata and timeseries station data given the stations
    in the database
    """
    
    token = 'e505002015c74fa6850b2fc13f70d2da'
    states = ['CA', 'ID']
    
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
    
    def __init__(self, db):
        self._logger = logging.getLogger(__name__)
        
        self.db = db
        
        self._logger.debug('Initialized Mesowest')
        
    def metadata(self):
        """
        Retrieve the metadata from Mesowest
        """
        
        self._logger.info('Obtaining metadata form Mesowest')
        m = Meso(token=self.token)
        
        networks = m.networks()
        
        for state in self.states:
            self._logger.debug('Obtaining metadata for {}'.format(state))
            meta = m.metadata(state=state)
        
        
    


