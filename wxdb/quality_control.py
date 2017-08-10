import logging
import coloredlogs

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
        The row contains data that is not consistent with closby stations (not
        implemented yet)
        
    Args:
        config: configuration section for the quality control
        data: DataFrame with columns mataching the ranges keys
        
    Returns:
        DataFrame with the data flagged
    
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
    
    def __init__(self, config, data):
        self._logger = logging.getLogger(__name__)
        
        self.config = config
        data
        
        
        
        
        
        