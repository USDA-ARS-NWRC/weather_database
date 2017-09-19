import logging
import pandas as pd
import numpy as np

__author__ = "Scott Havens"
__maintainer__ = "Scott Havens"
__email__ = "scott.havens@ars.usda.gov"
__date__ = "2017-07-27"


class QC():
    """
    Quality control class that will implemented qualtiy control measures.
    The database has a flag column that will get filled in using codes contained
    in the QC class. The codes will be applied broadly for the timestep and will
    not say which value is bad. Multiple codes can be applied to each row.
    
    The codes are as follows:
    
    `0` - no flag
        The row data contains no values that raised a flag
    
    `m` - missing data
        The row contains a missing value
        
    `r` - data out of range
        The row contains data that is out of range, see `QC.ranges` for values
        
    `p` - persistence check
        Check if the variable is changing enough over a time period. Check the
        difference between minimum and maximum value of a time period which should
        have changed by some value. (not implemented yet)
    
    `c` - consistency check failed
        The row contains data that is not consistent with closeby stations (not
        implemented yet)
        
    Args:
        config: configuration section for the quality control
        data: DataFrame with columns mataching the ranges keys
        
    Returns:
        DataFrame with the data flagged in 'qc_flag' column
    
    """
    
    codes = {
            0: 'No flag',
            'm': 'Missing data',
            'r': 'Data out of range',
            'p': 'Persistence check',
            'c': 'Consistency check failed'
        }
    
    # all ranges are metric Celcius, millimeters, w/m**2, Pascal
    ranges = {
        'air_temp': {'min': -50, 'max': 50},
        'dew_point_temperature': {'min': -50, 'max': 35},
        'relative_humidity': {'min': 0, 'max': 100},
        'wind_speed': {'min': 0, 'max': 35},
        'wind_direction': {'min': 0, 'max': 360},
        'wind_gust': {'min': 0, 'max': 50},
        'solar_radiation': {'min': 0, 'max': 1350},
        'snow_smoothed': {'min': 0, 'max': 5000},
        'precip_accum': {'min': 0, 'max': 5000},
        'precip_intensity': {'min': 0, 'max': 100},
        'snow_depth': {'min': 0, 'max': 5000},
        'snow_interval': {'min': 0, 'max': 1000},
        'snow_water_equiv': {'min': 0, 'max': 5000},
        'vapor_pressure': {'min': 0, 'max': 3500},
        'cloud_factor': {'min': 0, 'max': 1}
        }
    
    table_from = 'tbl_level1'
    table_to = 'tbl_level_auto'
    
    def __init__(self, config, db=None):
        self._logger = logging.getLogger(__name__)
        
        # flag options, remove, cap, fill
        if 'flag' in config:
            if not any(config['flag'] in s for s in ['remove', 'cap', 'fill']):
                raise Exception("quality control flag must be 'remove', 'cap', or 'fill')")
        else:
            config['flag'] = 'remove'
        
        self.config = config
        
        # database placeholder, if 'write_to' given then write
        # the dataframe to the desired table, if not just return
        # the dataframe
        self.db = db
        
    def run(self, data):
        """
        Perform the quality control measures
        """
        stid = data.station_id[0]
        cols = data.columns
        r = pd.DataFrame(index=data.index)
        m = pd.DataFrame(index=data.index)
        for key, val in self.ranges.items():
            if key in cols:
                # check for missing data
                # the station_id and date_time column *should* always have something in them
                m_idx = data[key].isnull()
        
                # check the range
                r_idx = ~data[key].between(val['min'], val['max'])
                
                # value that is NaN is out of range so fix the flag
                r_idx[m_idx] = False
                
                # deal with the data
                if self.config['flag'] == 'remove':
                    data.loc[r_idx, key] = np.nan
                
                m[key] = m_idx
                r[key] = r_idx
                
        r_idx = r.any(axis=1)
        m_idx = m.any(axis=1)
        
        flg = (r_idx | m_idx).sum()
        if flg > 0:
            self._logger.warn('Flagged {} rows for {}'.format(flg, stid))
                
        # add the flag column
        data['qc_flag'] = None
        
        # add the flags
        data.loc[m_idx, 'qc_flag'] = 'm'
        data.loc[r_idx, 'qc_flag'] = data.loc[r_idx, 'qc_flag'] + 'r'
        
        if self.db:
            # write out to the database
            self.db.insert_data(data, 
                                loc='auto',
                                description='Auto QC data for {}'.
                                format(data.iloc[0].station_id))
        
        # return the flagged dataframe
        return data
        
        
        
        
        
        