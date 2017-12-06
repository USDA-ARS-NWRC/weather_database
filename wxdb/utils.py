"""
Collection of utilities that are needed for more than one module
"""

import pandas as pd
import numpy as np
from datetime import datetime
import pytz
import utm

__author__ = "Scott Havens"
__maintainer__ = "Scott Havens"
__email__ = "scott.havens@ars.usda.gov"
__date__ = "2017-08-03"

FREEZE = 273.16         # freezing temp K
BOIL = 373.15           # boiling temperature K
SEA_LEVEL = 1.013246e5  # sea level pressure

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

def average_df(r, stid):
    """
    Average the dataframe. Performs a copy of the original dataframe then
    resample 'H' will take the mean of the values that fall within the hour.
    
    Args:
        r: DataFrame with 'date_time' as index
        stid: station id, will be entered as a columns
        
    Returns:
        DataFrame that is resampled with a 'date_time' and 'station_id' columns
        added back in
    """
    
    df = r.copy()
    df = df.resample('H').mean()
    df.dropna(axis=0, how='all', inplace=True)
    df['date_time'] = df.index.strftime('%Y-%m-%d %H:%M')
    df['station_id'] = stid
    
    return df

def df_utm(row):
    """
    Calculate the UTM coordinates for a dataframe row
    
    Args:
        row: DataFrame row with 'latitude' and 'longitude'
        
        tuple with utm_x, utm_y, utm_zone
    """
    
    try:
        r = utm.from_latlon(float(row['latitude']), float(row['longitude']))
        ret = r[0], r[1], str(r[2]) + r[3]
    except Exception:
        ret = (None, None, None)
    return ret

def satw(tk):
    """
    Saturation vapor pressure of water. from IPW satw

    Args:
        tk: temperature in Kelvin

    Returns:
        saturated vapor pressure over water

    20151027 Scott Havens
    """

    # remove bad values
    tk[tk < 0] = np.nan

    l10 = np.log(10.0)

    btk = BOIL/tk
    x = -7.90298*(btk - 1.0) + 5.02808*np.log(btk)/l10 - \
        1.3816e-7*(np.power(10.0, 1.1344e1*(1.0 - tk/BOIL))-1.) + \
        8.1328e-3*(np.power(10.0, -3.49149*(btk - 1.0)) - 1.0) + \
        np.log(SEA_LEVEL)/l10

    x = np.power(10.0, x)

    return x


def sati(tk):
    """
    saturation vapor pressure over ice. From IPW sati

    Args:
        tk: temperature in Kelvin

    Returns:
        saturated vapor pressure over ice

    20151027 Scott Havens
    """

    # remove bad values
    tk[tk < 0] = np.nan

    # preallocate
    x = np.empty(tk.shape)

    # vapor above freezing
    ind = tk > FREEZE
    x[ind] = satw(tk[ind])

    # vapor below freezing
    l10 = np.log(10.0)
    x[~ind] = 100.0 * np.power(10.0, -9.09718*((FREEZE/tk[~ind]) - 1.0) -
                               3.56654*np.log(FREEZE/tk[~ind])/l10 +
                               8.76793e-1*(1.0 - (tk[~ind]/FREEZE)) +
                               np.log(6.1071)/l10)

    return x
   
def rh2vp(ta, rh):
    """
    Calculate the vapor pressure from the air temperature and 
    relative humidity
    
    Args:
        ta: air temperature in Celcius
        rh: relative humidity, preferabilty on the scale 0-1.0
        
    Returns:
        vapor pressure in Pascals
    """ 
    
    if np.max(rh) >= 1.0:
        rh = rh / 100.0
    
    vp = sati(ta + FREEZE)
    
    return vp * rh
    
    
    