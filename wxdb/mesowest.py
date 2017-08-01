"""
Interact with the Mesowest API to get metadata and timeseries data
"""

import logging
from MesoPy import Meso
import pandas as pd
import chunk
from docutils.nodes import description

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
        DF['source'] = 'Mesowest'
        
        DF = DF.where((pd.notnull(DF)), None)
        
        # insert the dataframe into the database
        self.db.insert_data(DF, description='Mesowest metadata', metadata=True)
        
        
        
        
        
        
    


