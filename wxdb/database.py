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
        user: database username
        password: use password
        
    """
    
    def __init__(self, user, password, host, db):
        """
        Initialize the db instance by connecting to the database
        """
        
        self._logger = logging.getLogger(__name__)
        self.db = db
        
        try:
            cnx = mysql.connector.connect(user=user,
                                          password=password,
                                          host=host,
                                          database=db)

        except mysql.connector.Error as err:
            if err.errno == 1045:  # errorcode.ER_ACCESS_DENIED_ERROR:
                self._logger.error('''Something is wrong with your
                                user name or password''')
            elif err.errno == 1049:  # errorcode.ER_BAD_DB_ERROR:
                self._logger.error("Database does not exist")
            else:
                self._logger.error(err)

        self.cnx = cnx
                
        self._logger.info('Connected to MySQL database -- {}'.format(db))
        
        
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Ensure that the database connection is closed
        """
        self.cnx.close()
        self._logger.info('Disconnected from MySQL database -- {}'.format(self.db))
        
        
        
    def insert_data(self, table, df, description=''):
        """
        Insert data into the database for the given table and dataframe
        """
        
        self._logger.info('Adding {} to the database'.format(description))
        
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
                
            
            