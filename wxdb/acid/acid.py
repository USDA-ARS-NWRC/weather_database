import logging
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from acid.autocleanfft import AutoCleanFFT

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
        
        # flag options, remove, cap, fill
        
        
        self.config = config
        
        # database placeholder, if 'write_to' given then write
        # the dataframe to the desired table, if not just return
        # the dataframe
        self.db = db
        
    def run(self):
        """
        Perform the quality control measures
        """
        
        # go client at a time and load the data
        
        
        
        if self.db:
            # write out to the database
            self.db.insert_data(data, 
                                loc='auto',
                                description='ACID data for {}'.
                                format(data.iloc[0].station_id))
        
        
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
                
        
        
        
        
        
        