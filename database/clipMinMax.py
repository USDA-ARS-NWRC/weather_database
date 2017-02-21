"""
Take the raw data from tbl_level1 and clip the min and max values to
ensure that all values are on the hour and have top and low end limits as
form of automatic data cleaning

Loop through one station at a time, look for values greater the the applied thresholds


2015-01-20 Scott Havens
"""

import os
from datetime import datetime
import pandas as pd
import progressbar
from averageDelete import get_stations, get_station_data, get_values,reinsert
# number of seconds for rounding
round_val = 3600

execfile(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'database_connect.py'))
cnx.autocommit = False
cursor = cnx.cursor()


def threshold_column(data, column_name, min_val, max_val):
    """
    thresh data. The col_data is assigned to max_val when any data is greater than max_val.
    The same is applied to min_val when the col_data is less than min_val
    """
    col_data = data[column_name]

    for i in range(len(col_data)):
        if col_data[i] > max_val:
            col_data[i] = max_val
        elif col_data[i] < min_val:
            col_data[i] = min_val

    #Assign the data back
    data[col_name] = col_data

def get_entry_date(tbl,sta,timing='last'):
    """
    Returns the date time of the last entry for a station in a table.
    timing can be one of the following:
            first - will return the earliest entry.
            last - will return the date of the last
    """
    #attempt to read the station data
    try:
        if timing == 'last':
            qry = "SELECT MAX(date_time) FROM %s where station_id='%s';" % (tbl,sta)

        elif timing == 'first':
            qry = "SELECT MIN(date_time) FROM %s where station_id='%s';" % (tbl,sta)

        else:
            raise ValueError("Method get_entry_date expects either first or last for the timing argument.")

    #likely a missing station in table 1.5
    except mysql.connector.Error as err:
        print "exception caught"
        print err
        insert_qry = "INSERT INTO %s (station_id) VALUES (%s);" % (tbl,sta)
        cursor.execute(insert_qry)
        cursor.commit()

    cursor.execute(qry)
    date_time = cursor.fetchall()
    date_time = date_time[0][0]

    if date_time == None:
        #Set it to early
        date_time = pd.Timestamp(datetime(1900,01,01))


    return date_time.isoformat(' ')


def column_compare(base_tbl,compare_tbl, column):
    """
    Compares at two tables and checks to see if they have the same entries for a given
    column, returns list of missing entries.
    It is expected that Basis is more complete than basis.
    """

    basis_qry = 'SELECT DISTINCT {0} FROM {1}'.format(column, base_tbl)
    cursor.execute(basis_qry)
    basis = cursor.fetchall()

    compare_qry = 'SELECT DISTINCT {0} FROM {1}'.format(column, compare_tbl)
    cursor.execute(compare_qry)
    compare = cursor.fetchall()
    result =  list(set(basis) - set(compare))

    return result

def ensure_field_continuity(base_tbl,compare_tbl, column):
    """
    Looks at column in base and adds any missing fields to the column in compare using the
    min date available
    """
    missing_data = column_compare(base_tbl,compare_tbl,column)

    for j, sta in enumerate(missing_data):
        sta = sta[0]
        startdate = get_entry_date(base_tbl,sta,"first")
        insert_qry = "INSERT INTO %s (%s, date_time) VALUES ('%s','%s');" % (compare_tbl,column,sta,startdate)
        cursor.execute(insert_qry)
        cnx.commit()

def clip_min_max(stations, clip_dict):
    """
     threashold data from tbl_level1 and place it in tbl_level1_5
    """

    print("%i stations to process.." % len(stations))

    #pbar = progressbar.ProgressBar(max_value=len(stations))
    ensure_field_continuity("tbl_level1", "tbl_level1_5", "station_id")
    # for j,sta in enumerate(stations):
    #
    #     try:
    #         #Grab the last entry dates from both tables
    #         print "Getting dates "
    #
    #         tbl1_enddate = get_entry_date("tbl_level1",sta,"last")
    #         tbl1_5_enddate = get_entry_date("tbl_level1_5",sta,"last")
    #         print tbl1_enddate
    #         print tbl1_5_enddate
    #         if tbl1_enddate > tbl1_5_enddate:
    #             cnx.start_transaction()
    #
    #             #Grab the missing data
    #             print("grabbing values...")
    #
    #             data = get_values(sta,tbl1_5_enddate,tbl1_enddate,"tbl_level1")
    #
    #             #Cycle through the columns the user wants to clip
    #             print "Clipping data..."
    #             for col,limits in clip_dict.items():
    #                 threshold_column(data,col,limits[0],limits[1])
    #             print("reinserting the data...")
    #             reinsert(sta,data,tbl1_enddate, 'tbl_level1_5')
    #             cnx.commit()
    #         else:
    #             print "Date time mismatch"
    #
    #     except mysql.connector.Error as err:
    #         cnx.rollback()
    #         print(err)


        #pbar.update(j)

    #pbar.finish()

if __name__ == "__main__":
    #Assign values low to hi
    clipping_dict ={
    "wind_speed":(0.0,40.0),
    "air_temp":(-30.0,50.0),
    "solar_rad":(0.0,1500.0)
    }
    # get the stations
    print("Acquiring stations...")
    missing_stations = column_compare('tbl_level1','tbl_level1_5','station_id')
    stations = get_stations("tbl_level1","av_del=1")

    # clip the data the data
    clip_min_max(stations,clipping_dict)

    cnx.close()
