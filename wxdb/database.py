"""
Database instance
"""

import mysql.connector
from mysql.connector import errorcode
import logging
import pandas as pd

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
    
    def __init__(self, config):
        """
        Initialize the db instance by connecting to the database
        """
        
        self.config = config
        self._logger = logging.getLogger(__name__)
        
        # check for the config
        k = config.keys()      
        for f in self.fields:
            if f not in k:
                self._logger.error('[mysql] section requires {}'.format(f))
        
        # determine if the port parameter was passed
        port = config['port'] if 'port' in k else 3306
        
        self.metadata_table = None
        if 'metadata' in k:
            self.metadata_table = config['metadata']
        
        self.data_table = None
        if 'data' in k:
            self.data_table = config['data']
        
        try:
            cnx = mysql.connector.connect(user=config['user'],
                                          password=config['password'],
                                          host=config['host'],
                                          database=config['database'],
                                          port=port)

        except mysql.connector.Error as err:
#             if err.errno == 1045:  # errorcode.ER_ACCESS_DENIED_ERROR:
#                 self._logger.error('''Something is wrong with your user name or password''')
#             elif err.errno == 1049:  # errorcode.ER_BAD_DB_ERROR:
#                 self._logger.error("Database does not exist")
#             self._logger.error(err)
            raise err
            

        self.cnx = cnx
                
        self._logger.info('Connected to MySQL database -- {}'.format(config['database']))
        
        
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Ensure that the database connection is closed
        """
        self.cnx.close()
        self._logger.info('Disconnected from MySQL database -- {}'.format(self.config['database']))
        
        
        
    def insert_data(self, df, description='', metadata=False, data=False):
        """
        Insert data into the database for the given table and dataframe
        """
        
        if (not metadata) & (not data):
            self._logger.error('Must specify either metadata or data')
        if metadata & data:
            self._logger.error('Metadata and data cannot be set to True at the same time')
        
        self._logger.info('Adding {} to the database'.format(description))
        
        if metadata:
            table = self.metadata_table
        
        
        try:
            # replace Null with None
            df = df.where((pd.notnull(df)), None)
        
            # create a bulk insert for the data        
            wildcards = ','.join(['%s'] * len(df.columns))
            colnames = ','.join(df.columns)
            update = ','.join(['{}=VALUES({})'.format(c,c) for c in df.columns])
            insert_sql = 'INSERT INTO {0} ({1}) VALUES ({2}) ON DUPLICATE KEY UPDATE {3}'.format(
                table, colnames, wildcards, update)
            
            data = [tuple(rw) for rw in df.values]
            
            cur = self.cnx.cursor()
            cur.executemany(insert_sql, data)
            self.cnx.commit()
            
        except mysql.connector.Error as err:
            self.cnx.rollback()
            self._logger.debug('Error loading {} into database'.format(description))
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                self._logger.error("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                self._logger.error("Database does not exist")
            else:
                self._logger.error(err)
                
            
            