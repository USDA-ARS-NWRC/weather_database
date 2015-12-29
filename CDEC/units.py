'''
20151228 Scott Havens

Convert units from english to metric

The values currently handled
['precip_accum', 'snow_water_equiv', 'air_temp', 'wind_speed', 'wind_dir',
                     'relative_humidity', 'snow_depth', 'solar_radiation']
                     
'''

# import pandas as pd


def convertUnits(r):
    
    # columns of data frame
    col = r.columns;
    
    # functions to apply
    f2c = lambda x: (x - 32) * 5 / 9
    in2mm = lambda x: x * 25.4
    in2cm = lambda x: x * 2.54
    mph2ms = lambda x: x * 0.44704
    
    for c in col:
        
        if c == 'air_temp':
            func = f2c
        elif c == 'precip_accum':
            func = in2mm
        elif c in ['snow_depth', 'snow_water_equiv']:
            func = in2mm
        elif c == 'wind_speed':
            func = mph2ms
        else:
            func = None
        
        if func is not None:
            r[c] = r[c].map(func)

    return r
