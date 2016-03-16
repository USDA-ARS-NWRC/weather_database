"""
Take the raw data from tbl_raw_data and average/delete values to 
ensure that all values are on the hour

Loop through one station at a time, look for duplicate values or those
that are not on the hour 


2015-01-20 Scott Havens
"""

import os
from datetime import datetime, timedelta
import pandas as pd
import progressbar

# number of seconds for rounding
round_val = 3600

execfile(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'database_connect.py'))
cnx.autocommit = False
cursor = cnx.cursor()

#------------------------------------------------------------------------------ 
# get all the stations
qry_station = "SELECT station_id from tbl_stations"
cursor.execute(qry_station)
stations = cursor.fetchall()

#pbar = progressbar.ProgressBar(max_value=len(stations))
for j,sta in enumerate(stations):

    #------------------------------------------------------------------------------ 
    # determine the whole hours and how many values are present
    
    qry = "SELECT COUNT(*) AS cnt, station_id, date_time,FROM_UNIXTIME(FLOOR((UNIX_TIMESTAMP(date_time) + %i-1) / %i) * %i) AS round_date \
            FROM tbl_level1 WHERE station_id='%s' GROUP BY station_id,round_date" % (round_val, round_val, round_val, sta[0])
    cursor.execute(qry)
    d = cursor.fetchall()
    
    # create a dataframe for easy viewing/manipulating
    data = pd.DataFrame(d, columns=['cnt', 'station_id', 'date_time', 'round_time'])
    
    # make sure that all the round_values are unique
    if len(data) != len(data.round_time.unique()):
        raise Exception('Times are not unique for station %s' % sta[0])
    
    flag = False
    if len(data) == data.cnt.sum():
        flag = True
    
    # loop through each row
    for i,r in data.iterrows():
        
            if r.cnt == 1:
                try:
                    # only one time step for the rounding
                    # check to see if the date_time is equal to round_time
                    if r.date_time != r.round_time:
                        cnx.start_transaction() # start a transaction
                        
                        if flag:
                            # then all values are on the hour but need to be adjusted
                            # so this takes a little different approach and rounds to the nearest
                            # time step value
                            t = timedelta(minutes=r.date_time.minute, seconds=r.date_time.second)
                            
                            date_time = r.date_time - t
                            if t >= timedelta(minutes=round_val/60/2):
                                date_time += timedelta(minutes=round_val/60)
                            
                            qry = "UPDATE tbl_level1 SET date_time='%s' where date_time='%s' and station_id='%s'" % (date_time, r.date_time, r.station_id)
                            
                        else:  
                            qry = "UPDATE tbl_level1 SET date_time='%s' where date_time='%s' and station_id='%s'" % (r.round_time, r.date_time, r.station_id)
                        cursor.execute(qry)
                        cnx.commit()
                        
                except mysql.connector.Error as err:
                    cnx.rollback()
                    print 'Error updating date_time %s - %s (%s): ' % (sta[0], r.round_time, err)
            
            else:
                try:
                # there are multiple values for each time step
                    cnx.start_transaction() # start a transaction
                    
                    # determine the time step start point
                    date_start = r.round_time - timedelta(seconds=round_val-1)
                    
                    # create a query to get all the values
                    qry = "SELECT * FROM tbl_level1 WHERE station_id='%s' AND date_time BETWEEN '%s' AND '%s'" % (sta[0], date_start, r.round_time)
                    dup = pd.read_sql(qry, cnx, index_col='date_time')
                    
                    # delete all those values from the table
                    delete_qry = "DELETE FROM tbl_level1 WHERE station_id='%s' AND date_time BETWEEN '%s' AND '%s'" % (sta[0], date_start, r.round_time)
                    cursor.execute(delete_qry)
                    
                    # get the average and reinsert into table
                    m = dup.mean()
                    m['station_id'] = sta[0]
                    m['date_time'] = None
                    m['date_time'] = pd.Timestamp(r.round_time)
                    
                    # create a query to insert the average values
                    cols = m.keys().tolist()
                    val = []
                    for c in cols:
                        val.append(str(m[c]))
                    
                    insert_qry = "INSERT INTO tbl_level1 (%s) VALUES ('%s')" % (",".join(cols), "','".join(val))
                    cursor.execute(insert_qry)
                    
                    # commit the tranaction
                    cnx.commit()
    
                except mysql.connector.Error as err:
                    cnx.rollback()
                    print 'Error averaging data from database for %s - %s (%s): ' % (sta[0], r.round_time, err)
    
#    pbar.update(j)
    
#pbar.finish()
cnx.close()
