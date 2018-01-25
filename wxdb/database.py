"""
Database instance
"""

import mysql.connector
from mysql.connector import errorcode
import logging
import pandas as pd
import numpy as np

__author__ = "Scott Havens"
__maintainer__ = "Scott Havens"
__email__ = "scott.havens@ars.usda.gov"
__date__ = "2017-07-27"

class Database():
    """
    Database class to create a connection and interact with a MySQL database.
    
    Args:
        config: The [mysql] section of the cofiguration file. Should have at a
            minimum:
            user: user name
            password: user passowrd
            host: the host to connect to
            database: which database to use
            metadata: metadata data to insert into
            data: data table to insert into
        
    """
    
    fields = ['user', 'password', 'host', 'database']
    chunk_size = 75000
    
    def __init__(self, config):
        """
        Initialize the db instance by connecting to the database
        """
        
        self._logger = logging.getLogger(__name__)
        
        # check for the config
        k = config.keys()      
        for f in self.fields:
            if f not in k:
                self._logger.error('[mysql] section requires {}'.format(f))
        
        # determine if the port parameter was passed
        config['port'] = config['port'] if 'port' in k else 3306
        self.config = config
        
        self.metadata_table = None
        if 'metadata' in k:
            self.metadata_tables = config['metadata'].split(',')
        
        self.data_table = None
        if 'level0' in k:
            self.data_tables = config['level0'].split(',')
            
        self.avg_del_tables = None
        if 'level1' in k:
            self.avg_del_tables = config['level1'].split(',')
        
        self.auto_tables = None
        if 'auto' in k:
            self.auto_tables = config['auto'].split(',')
            
        self.conneted = False
        
    def db_connect(self):
        """
        Connect to the database
        """
        try:
            cnx = mysql.connector.connect(user=self.config['user'],
                                          password=self.config['password'],
                                          host=self.config['host'],
                                          database=self.config['database'],
                                          port=self.config['port'])
            cnx.set_converter_class(NumpyMySQLConverter)

        except mysql.connector.Error as err:
            raise err
            
        self.cnx = cnx
        self.conneted = True
        self._logger.debug('Connected to MySQL database -- {}'.format(self.config['database']))
        
    def db_close(self):
        """
        Ensure that the database connection is closed
        """
        self.cnx.close()
        self.conneted = False
        self._logger.debug('Disconnected from MySQL database -- {}'.format(self.config['database']))
        
        
        
    def insert_data(self, df, loc, description=''):
        """
        Insert data into the database for the given table and dataframe
        """
        
        table = self.get_table(loc)
                
        self.db_connect()
        
        for tbl in table:
            self._logger.info('Adding/updating {} ({} values) to the database table {}'.format(
                description, len(df), tbl))
            
            try:
                # replace Null with None
                df = df.where((pd.notnull(df)), None)
            
                # create a bulk insert for the data        
                wildcards = ','.join(['%s'] * len(df.columns))
                colnames = ','.join(df.columns)
                update = ','.join(['{}=VALUES({})'.format(c,c) for c in df.columns])
                insert_sql = 'INSERT INTO {0} ({1}) VALUES ({2}) ON DUPLICATE KEY UPDATE {3}'.format(
                    tbl, colnames, wildcards, update)
                
                data = [tuple(rw) for rw in df.values]
                cur = self.cnx.cursor()
                
                for d in chunks(data, self.chunk_size):
                    cur.executemany(insert_sql, d)
                    self.cnx.commit()
                
            except Exception as err:
                    self._logger.error(err)
                    
        self.db_close()
        
    def update_data(self, df, loc, where, description=''):
        """
        Update data into the database for the given table and dataframe
        """
        
        table = self.get_table(loc)
        
        for tbl in table:
            self._logger.info('Adding/updating {} ({} values) to the database table {}'.format(
                description, len(df), tbl))
            
            try:
                # replace Null with None
                df = df.where((pd.notnull(df)), None)
            
                # create a bulk insert for the data        
#                 wildcards = ','.join(['%s'] * len(df.columns))
#                 colnames = ','.join(df.columns)

                for i,row in df.iterrows():
                    
                    row.dropna(inplace=True)
                    w = row[where]
                    del row[where]
                    
                    update = ','.join(['{}=%s'.format(c) for c in row.index])
                    update_sql = 'UPDATE {0} SET {1} WHERE {2}="{3}"'.format(tbl, update, where, w)
                
                    data = [rw for rw in row.values]
                    cur = self.cnx.cursor()
                    
                    cur.execute(update_sql, data)
                    self.cnx.commit()
                    
            except mysql.connector.Error as err:
                    self._logger.error(err)
                    
        self.db_close()
        
    def retrieve_station_data(self, stid, start_date, end_date, loc='level1'):
        """
        Retrieve all the data from the database for the given station between
        the start and end dates.
        
        Args:
            stid: string for the primary_id
            start_date: timestamp for the start date to pull
            end_date: timestampe for the end date to pull
            loc: table location to get from
            
        Returns:
            Dataframe for the retquested data
        """
        
        table = self.get_table(loc)[0]
        
        if not self.conneted:
            self.db_connect()
        
        try:
            qry = "SELECT * FROM {0} WHERE station_id='{1}' AND date_time BETWEEN '{2}' AND '{3}'".format(
                table, stid, start_date.isoformat(), end_date.isoformat())

            df = pd.read_sql(qry, self.cnx, index_col='date_time')
            
            if df is None:
                self._logger.warn('Could not find data for {}'.format(stid))
                
            else:
                # replace None with NaN and drop columns without any data
                df.fillna(value=np.nan, inplace=True)
                df.dropna(axis=1, how='all', inplace=True)
                
        except mysql.connector.Error as err:
                self._logger.error(err)
                
        return df

        
    def get_table(self, loc):
        """
        Determine the tables to insert into based on loc
        
        Args:
            loc: string location, valid values are 
                ['metadata', 'data', 'avg_del', 'auto']
                
        Returns
            tables for the cooresponding loc
        """
        
        if loc == 'metadata':
            table = self.metadata_tables
        elif loc == 'level0':
            table = self.data_tables
        elif loc == 'level1':
            table = self.avg_del_tables
        elif loc == 'auto':
            table = self.auto_tables
        else:
            raise ValueError('valid values for loc are "metadata", "data", "avg_del", "auto"')
        
        return table
 
class NumpyMySQLConverter(mysql.connector.conversion.MySQLConverter):
    """ A mysql.connector Converter that handles Numpy types """

    def _float32_to_mysql(self, value):
        return float(value)

    def _float64_to_mysql(self, value):
        return float(value)

    def _int32_to_mysql(self, value):
        return int(value)

    def _int64_to_mysql(self, value):
        return int(value)
                
def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]       
            