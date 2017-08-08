"""
Collection of utilities that are needed for more than one module
"""

import pandas as pd
from datetime import datetime
import pytz

__author__ = "Scott Havens"
__maintainer__ = "Scott Havens"
__email__ = "scott.havens@ars.usda.gov"
__date__ = "2017-08-03"

def water_day(indate, timezone):
    """
    Determine the decimal day in the water year
    
    Args:
        indate: datetime object
        
    Returns:
        dd: decimal day from start of water year
    
    20160105 Scott Havens
    """
    tp = indate.timetuple()
    mnt = pytz.timezone(timezone)
    
    # create a test start of the water year    
    test_date = datetime(tp.tm_year, 10, 1, 0, 0, 0)
    test_date = mnt.localize(test_date)
    test_date = pd.to_datetime(test_date,utc=True)
    
    # check to see if it makes sense
    if indate < test_date:
        wy = tp.tm_year
    else:
        wy = tp.tm_year + 1

    return wy

def get_end_time(timezone, end_time=None, to_timezone='UTC'):
    """
    Get the current time for the given timezone, returned in UTC
    
    Args:
        timezone: time zone that pytz can parse
        
    Returns:
        pandas datetime object with timezone
    """
    
    mnt = pytz.timezone(timezone)
    if end_time is None:
        endTime = pd.Timestamp('now')
    else:
        endTime = pd.to_datetime(end_time)
    endTime = mnt.localize(endTime)
    return endTime.tz_convert(to_timezone)


def convert_units(r, units):
    """
    Convert the units of a dataframe from english
    to metric. The dictionary provides the column name
    with the current unit
    
    Args:
        r: dataframe to convert
        units: dict for {col_name: unit}
        
    Returns:
        dataframe r converted
    """
    
    # columns of data frame
    col = r.columns;
    
    # functions to apply
    f2c = lambda x: (x - 32) * 5 / 9
    in2mm = lambda x: x * 25.4
    mph2ms = lambda x: x * 0.44704
    
    for c in col:
        
        if c in units.keys():
            
            if units[c] == 'deg_f':
                func = f2c
            elif units[c] == 'inches':
                func = in2mm
            elif units[c] == 'mph':
                func = mph2ms
            else:
                func = None
        
        if func is not None:
            r[c] = r[c].map(func)

    return r

    
    