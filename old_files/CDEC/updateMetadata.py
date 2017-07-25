'''
20151221 Scott Havens

Download the stations metadata and add to the database tbl_metadata

'''


import numpy as np
import re, os
from ulmo import cdec


# connect to db, returns connection cnx
execfile(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'database_connect.py'))
cursor = cnx.cursor()

# grab the metadata, returns pandas structure
metadata = cdec.historical.get_stations()

N = len(metadata)


# loop through each station
fields = ['primary_id', 'station_name', 'latitude', 'longitude', 'elevation', 'source', 'variables']
provider = 'CDEC'
for sta in metadata.index:
    
    # pull out the station information
    name = metadata.ix[sta]['name']
    num = metadata.ix[sta].num
    lat = metadata.ix[sta].lat
    lon = metadata.ix[sta].lon
    junk = metadata.ix[sta].junk
    
    # check if there
    badData = re.findall(r'\n',name)
    if len(badData) == 0:
    
        # process the crappy csv file
        # check for values that aren't really values, but end up with strings again
        try:
            lat = str(np.float(lat))
            lon = str(-1*np.float(lon))
            name = cnx.converter.escape(name)
            
            # get the variables
            s = cdec.historical.get_station_sensors(station_ids=[sta], resolutions='hourly')
            variables = []
            for v in s[sta]['variable']:
                variables.append(str(v.replace(',', '')))
            variables = ', '.join(variables)
        
            if isinstance(junk, str):
                junk = str(np.float(junk.rstrip()))
                values = [sta, name, lon, junk, str(np.float(lat)*0.3048), provider, variables]
            else:
                num = str(np.float(num))
                values = [sta, name, lat, lon, str(np.float(num)*0.3048), provider, variables]
                
            print values
            
            # insert into the database
            val = []
            for i,f in enumerate(fields):
                val.append("%s='%s'" % (f, values[i]))
            
            add_data = "INSERT INTO tbl_metadata (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s" % \
                (",".join(fields), ','.join(['%s' for v in values]), ','.join(val))
            
            cursor.execute(add_data, values)
            
                
        except ValueError:
            print '%s bad data' % sta

    
cnx.close()