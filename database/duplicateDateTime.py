"""
Sometimes we need to duplicate all the values for a given day to another datet time.
Well lets do it!

20160315 Scott Havens
"""


import os
from datetime import datetime, timedelta
import progressbar

# Date time to get and duplicate into
dateFrom = '2016-03-10 06:00:00'
dateTo = '2016-03-10 07:00:00'
tbl_from = 'tbl_level2'
db_name = 'weather_v2'

#------------------------------------------------------------------------------ 
execfile(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'database_connect.py'))
cnx.autocommit = False
cursor = cnx.cursor()

#------------------------------------------------------------------------------ 
# get all the stations
qry_station = "SELECT station_id from tbl_stations WHERE client='BRB'"
cursor.execute(qry_station)
stations = cursor.fetchall()

#------------------------------------------------------------------------------ 
# get the columns
qry = "SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA`='%s' AND `TABLE_NAME`='%s'" %\
    (db_name, tbl_from)
cursor.execute(qry)
columns = cursor.fetchall()
columns = [i[0] for i in columns]
ind = columns.index('date_time')

pbar = progressbar.ProgressBar(max_value=len(stations))
for j,sta in enumerate(stations):
    
    #------------------------------------------------------------------------------ 
    # get the current dateFrom
    
    qry = "SELECT * FROM %s WHERE station_id='%s' and date_time='%s'" % \
        (tbl_from, sta[0], dateFrom)
    cursor.execute(qry)
    d = cursor.fetchall()

    if d:
        d = list(d[0])
        # replace the time
        d[ind] = dateTo
        
        v = []
        for i,val in enumerate(d):
            if val is None:
                v.append('NULL')
            else:
                v.append("'" + str(val) + "'")
                
    
        # put back into the database
        qry = "REPLACE INTO %s VALUES(" % tbl_from + ','.join(v) +")"
        cursor.execute(qry)
    
    pbar.update(j)
    
pbar.finish()
cnx.close()


