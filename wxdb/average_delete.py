
from datetime import timedelta
import pandas as pd
import logging
import mysql.connector
from mysql.connector import errorcode

__author__ = "Scott Havens"
__maintainer__ = "Scott Havens"
__email__ = "scott.havens@ars.usda.gov"
__date__ = "2017-08-10"


class AD():
    # number of seconds for rounding
    round_val = 3600
    
    def __init__(self, df):
        self.df = df
        
        

class AvgDel():
    """
    The raw data from stations can be difficult to work with when
    timestamps are not on the hour or there are multiple measurements
    per hour. This will go through the data and apply the following
    corrections:
    
    1. If the measurements are hourly, adjust to the nearest hour if needed
    2. If the measurements are not hourly, average the values for the hour
    3. Delete values not on the hour
    """
    
    # number of seconds for rounding
    round_val = 3600
    
    def __init__(self, db):
        self._logger = logging.getLogger(__name__)
        self._logger.info('Starting average delete of tbl_leve1')
        
        self.db = db
        self.db.db_connect()
        self.cursor = self.db.cnx.cursor()
        
        # get the stations
        stations = self.get_stations()
     
        # average delete the data
        self.avg_delete(stations)
        
        self.cursor.close()
        self.db.db_close()
    
    def get_stations(self):
                
        # get all the stations
        qry_station = "SELECT DISTINCT(station_id) from tbl_level1 where avg_del=0"
        self.cursor.execute(qry_station)
        stations = self.cursor.fetchall()
        
        # remove stations without a name
        stations = [s[0] for s in stations]
        stations = list(filter(None, stations))
                
        self._logger.debug('Found {} stations to process'.format(len(stations)))
    
        return stations
    
    
    def get_station_data(self, sta, round_val):
        """
        determine the whole hours and how many values are present
        """
        
        qry = ("SELECT COUNT(*) AS cnt, MIN(id), station_id, "
               "DATE_ADD(DATE_FORMAT(date_time, \'%Y-%m-%d %H:00:00\'), "
               "INTERVAL IF(MINUTE(date_time) < {}/60/2, 0, 1) HOUR) AS round_date "
               "FROM tbl_level1 WHERE station_id='{}' and avg_del=0 GROUP BY "
               "station_id,round_date").format(round_val, sta)
        self.cursor.execute(qry)
        d = self.cursor.fetchall()
        
        # create a dataframe for easy viewing/manipulating
        data = pd.DataFrame(d, columns=['cnt', 'id', 'station_id', 'round_time'])
        data['round_time']  = pd.to_datetime(data['round_time'])
        
        # make sure that all the round_values are unique
        if len(data) != len(data.round_time.unique()):
            raise Exception('Times are not unique for station {}'.format(sta))
        
        flag = False
        if len(data) == data.cnt.sum():
            flag = True
        
        return data, flag
    
    
    def get_values(self, sta, date_start, date_end, table):
        """
        get values from a table between a time range
        """
        
        # create a query to get all the values
        qry = "SELECT * FROM {} WHERE station_id='{}' AND date_time BETWEEN '{}' AND '{}'" .format(table, sta, date_start, date_end)
        dup = pd.read_sql(qry, self.db.cnx, index_col='date_time')
        
        return dup
    
    
    def delete_values(self, ids, table='tbl_level1'):
        """
        Delete values from a table between a time range
        """
        
        wildcards = ','.join(['%s'] * len(ids))
        delete_qry = "DELETE FROM {} WHERE id IN ({})".format(table, wildcards)
        return self.cursor.execute(delete_qry, ids)
    
    
    def average_reinsert(self, sta, data, date_time, table='tbl_level1'):
        """
        Average the data and reinsert into the table
        """
        
        m = data.mean()
        m['station_id'] = sta
        m['date_time'] = pd.Timestamp(date_time).strftime('%Y-%m-%d %H:%M')
        m['avg_del'] = 1
        m = m.where((pd.notnull(m)), None)
        
        # create a query to insert the average values
        cols = m.keys().tolist()
        
        wildcards = ','.join(['%s'] * len(cols))
        colnames = ','.join(cols)
        insert_qry = 'INSERT INTO {0} ({1}) VALUES ({2})'.format(
            table, colnames, wildcards)
        
        in_data = tuple(m.values)
                
        self.cursor.execute(insert_qry, in_data)
        
        
    def single_timestep(self, sta, id, round_time, flag, round_val):
        """
        Create a query for a given time
        """
 
        qry = "UPDATE tbl_level1 SET date_time='{}', avg_del=1 where id='{}'".format(round_time, id)
                     
        return qry            
    
    
    
    def check_raw(self, sta, date_start, round_time):
        """
        Go back and load all the data from tbl_raw_data between date_start and round_time
        Then delete from tbl_level1 and insert the averaged values
        """
         
        try:
#             self.db.cnx.start_transaction() # start a transaction
             
             
            # check the data from tbl_raw_data
            raw = self.get_values(sta, date_start, round_time, 'tbl_raw_data')
             
            # delete all those values from the table
            result = self.delete_values(sta, date_start, round_time, 'tbl_level1')
             
            # get the average and reinsert into table
            self.average_reinsert(sta, raw, round_time, 'tbl_level1')
                             
            self.db.cnx.commit()
                             
        except mysql.connector.Error as err:
            self.db.cnx.rollback()
            self._logger.warn('Error averaging data from tbl_raw_data for {} - {} ({})'.format(sta, round_time, err))
     
     
     
    def avg_delete(self, stations):
        """
        Average and delete from tbl_level1
        """
              
        for j,sta in enumerate(stations):
         
            #------------------------------------------------------------------------------ 
            # determine the whole hours and how many values are present
             
            data, flag = self.get_station_data(sta, self.round_val)
            
            self._logger.debug('{} - {} times to process'.format(sta, len(data)))
             
            # loop through each row
            for i,r in data.iterrows():
                 
                # determine the time step start point
                date_start = r.round_time - timedelta(seconds=self.round_val/2-1)
                date_end = r.round_time + timedelta(seconds=self.round_val/2)
                 
                if r.cnt == 1:
                    try:
#                         self.db.cnx.start_transaction() # start a transaction
                         
                        # create query for single time step
                        qry = self.single_timestep(sta, r.id, r.round_time, flag, self.round_val)
                         
                        self.cursor.execute(qry)
                        self.db.cnx.commit()
                             
                    except mysql.connector.Error as err:
                        self.db.cnx.rollback()
                         
                        if err.errno == errorcode.ER_DUP_ENTRY:
                            # means that there was a duplicate key error, so go back to 
                            # the raw data and retry
                             
                            self.check_raw(sta, date_start, r.round_time)
                                 
                             
                        else:
                            self._logger.warn('Error updating date_time for {} - {} ({})'.format(sta, r.round_time, err))
                 
                else:
                    try:
                    # there are multiple values for each time step
#                         self.db.cnx.start_transaction() # start a transaction
                         
                        # get all the values from the table
                        dup = self.get_values(sta, date_start, date_end, 'tbl_level1')
                         
                        # delete all those values from the table
                        result = self.delete_values(dup['id'].values.tolist(), 'tbl_level1')
                         
                        # get the average and reinsert into table
                        self.average_reinsert(sta, dup, r.round_time, 'tbl_level1')
                         
                        # commit the tranaction
                        self.db.cnx.commit()
         
                    except mysql.connector.Error as err:
                        self.db.cnx.rollback()
                        self._logger.warn('Error averaging data from database for {} - {} ({})'.format(sta, r.round_time, err))
             
