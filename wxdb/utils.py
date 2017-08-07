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

def get_end_time(timezone, end_time=None):
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
    return endTime.tz_convert('UTC')
    
    