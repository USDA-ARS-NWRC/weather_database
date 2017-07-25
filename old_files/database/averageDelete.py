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


def get_stations():
    
    #------------------------------------------------------------------------------ 
    # get all the stations
    #qry_station = "SELECT station_id from tbl_stations"
    qry_station = "SELECT DISTINCT(station_id) from tbl_level1 where av_del=0"
    cursor.execute(qry_station)
    stations = cursor.fetchall()
    
    # remove stations without a name
    stations = [s[0] for s in stations]
    stations = filter(None, stations)

    return stations


def get_station_data(sta, round_val):
    """
    determine the whole hours and how many values are present
    """
    
    qry = "SELECT COUNT(*) AS cnt, station_id, date_time,FROM_UNIXTIME(FLOOR((UNIX_TIMESTAMP(date_time) + %i-1) / %i) * %i) AS round_date \
                FROM tbl_level1 WHERE station_id='%s' and av_del=0 GROUP BY station_id,round_date" % (round_val, round_val, round_val, sta)
    
    cursor.execute(qry)
    d = cursor.fetchall()
    
    # create a dataframe for easy viewing/manipulating
    data = pd.DataFrame(d, columns=['cnt', 'station_id', 'date_time', 'round_time'])
    
    # make sure that all the round_values are unique
    if len(data) != len(data.round_time.unique()):
        raise Exception('Times are not unique for station %s' % sta)
    
    flag = False
    if len(data) == data.cnt.sum():
        flag = True
        
    return data, flag


def get_values(sta, date_start, date_end, table):
    """
    get values from a table between a time range
    """
    
    # create a query to get all the values
    qry = "SELECT * FROM %s WHERE station_id='%s' AND date_time BETWEEN '%s' AND '%s'" % (table, sta, date_start, date_end)
    dup = pd.read_sql(qry, cnx, index_col='date_time')
    
    return dup


def delete_values(sta, date_start, date_end, table):
    """
    Delete values from a table between a time range
    """
    
    delete_qry = "DELETE FROM %s WHERE station_id='%s' AND date_time BETWEEN '%s' AND '%s'" % (table, sta, date_start, date_end)
    return cursor.execute(delete_qry)


def average_reinsert(sta, data, date_time, table='tbl_level1'):
    """
    Average the data and reinsert into the table
    """
    
    m = data.mean()
    m['station_id'] = sta
    m['date_time'] = None
    m['date_time'] = pd.Timestamp(date_time)
    m['av_del'] = 1
    
    # create a query to insert the average values
    cols = m.keys().tolist()
    val = []
    for c in cols:
        val.append(str(m[c]))
    
    insert_qry = "INSERT INTO %s (%s) VALUES ('%s')" % (table, ",".join(cols), "','".join(val))
    cursor.execute(insert_qry)
    
    
def single_timestep(sta, date_time, round_time, flag, round_val):
    """
    Create a query for a given time
    """
    
    # only one time step for the rounding
    # check to see if the date_time is equal to round_time
    if date_time != round_time:
        
        if flag:
            # then all values are on the hour but need to be adjusted
            # so this takes a little different approach and rounds to the nearest
            # time step value
            t = timedelta(minutes=date_time.minute, seconds=date_time.second)
            
            dt = date_time - t
            if t >= timedelta(minutes=round_val/60/2):
                dt += timedelta(minutes=round_val/60)
            
            qry = "UPDATE tbl_level1 SET date_time='%s', av_del=1 where date_time='%s' and station_id='%s'" % (dt, date_time, sta)
            
        else:  
            qry = "UPDATE tbl_level1 SET date_time='%s', av_del=1 where date_time='%s' and station_id='%s'" % (round_time, date_time, sta)
                    
    else:
        # change the av_del column to ignore next time
        qry = "UPDATE tbl_level1 SET av_del=1 where date_time='%s' and station_id='%s'" % (date_time, sta)
                    
    return qry            



def check_raw(sta, date_start, round_time):
    """
    Go back and load all the data from tbl_raw_data between date_start and round_time
    Then delete from tbl_level1 and insert the averaged values
    """
    
    try:
        cnx.start_transaction() # start a transaction
        
        
        # check the data from tbl_raw_data
        raw = get_values(sta, date_start, round_time, 'tbl_raw_data')
        
        # delete all those values from the table
        result = delete_values(sta, date_start, round_time, 'tbl_level1')
        
        # get the average and reinsert into table
        average_reinsert(sta, raw, round_time, 'tbl_level1')
                        
        cnx.commit()
                        
    except mysql.connector.Error as err:
        cnx.rollback()
        print 'Error averaging data from tbl_raw_data for %s - %s (%s): ' % (sta, round_time, err)



def avg_delete(stations):
    """
    Average and delete from tbl_level1
    """
    
    print("%i stations to process.." % len(stations))

    pbar = progressbar.ProgressBar(max_value=len(stations))
    for j,sta in enumerate(stations):
    
        #------------------------------------------------------------------------------ 
        # determine the whole hours and how many values are present
        
        data, flag = get_station_data(sta, round_val)
        
        # loop through each row
        for i,r in data.iterrows():
            
            # determine the time step start point
            date_start = r.round_time - timedelta(seconds=round_val-1)
            
            if r.cnt == 1:
                try:
                    cnx.start_transaction() # start a transaction
                    
                    # create query for single time step
                    qry = single_timestep(sta, r.date_time, r.round_time, flag, round_val)
                    
                    cursor.execute(qry)
                    cnx.commit()
                        
                except mysql.connector.Error as err:
                    cnx.rollback()
                    
                    if err.errno == errorcode.ER_DUP_ENTRY:
                        # means that there was a duplicate key error, so go back to 
                        # the raw data and retry
                        
                        check_raw(sta, date_start, r.round_time)
                            
                        
                    else:
                        print 'Error updating date_time %s - %s (%s): ' % (sta, r.round_time, err)
            
            else:
                try:
                # there are multiple values for each time step
                    cnx.start_transaction() # start a transaction
                    
                    # get all the values from the table
                    dup = get_values(sta, date_start, r.round_time, 'tbl_level1')
                    
                    # delete all those values from the table
                    result = delete_values(sta, date_start, r.round_time, 'tbl_level1')
                    
                    # get the average and reinsert into table
                    average_reinsert(sta, dup, r.round_time, 'tbl_level1')
                    
                    # commit the tranaction
                    cnx.commit()
    
                except mysql.connector.Error as err:
                    cnx.rollback()
                    print 'Error averaging data from database for %s - %s (%s): ' % (sta, r.round_time, err)
        
        pbar.update(j)
        
    pbar.finish()


if __name__ == "__main__":
    
    # get the stations
    stations = get_stations()
    
    # average delete the data
    avg_delete(stations)

    cnx.close()
