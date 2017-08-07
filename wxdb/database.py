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
    chunk_size = 100000
    
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
        if 'data' in k:
            self.data_tables = config['data'].split(',')
        
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
        self._logger.info('Connected to MySQL database -- {}'.format(self.config['database']))
        
    def db_close(self):
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
        
        if metadata:
            table = self.metadata_tables
        
        if data:
            table = self.data_tables
        
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
                
            except mysql.connector.Error as err:
                    self._logger.error(err)
                    
        self.db_close()
 
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
            