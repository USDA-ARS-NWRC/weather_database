# -*- coding: utf-8 -*-

import os
import argparse
import logging
import coloredlogs
from datetime import datetime
import traceback
import pandas as pd

try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser
    
from database import Database
from mesowest import Mesowest
from cdec import CDEC
from quality_control import QC
from acid import ACID

__author__ = "Scott Havens"
__maintainer__ = "Scott Havens"
__email__ = "scott.havens@ars.usda.gov"
__date__ = "2017-07-27"

def strip_string(s):
    return [x.strip() for x in s.split(',')]

class Weather():
    """
    wxdb interacts with the weather database to ingest
    various data sources.
    
    Args:
        config_file: configuration file for the weather class.
        
    Returns:
        weather class instance
    
    """
    
    def __init__(self, config_file):
        """
        Initialization
        """
        
        # read the config file and store
        if not os.path.isfile(config_file):
            raise Exception('Configuration file does not exist --> {}'
                            .format(config_file))
        
        f = MyParser()
        f.read(config_file)
        self.config = f.as_dict()
        
            
        # start logging
        loglevel = 'DEBUG'
        logfile = None
        if 'logging' in self.config.keys():
            if 'log_level' in self.config['logging']:
                loglevel = self.config['logging']['log_level'].upper()
            if 'log_file' in self.config['logging']:
                logfile = self.config['logging']['log_file']
                    
        numeric_level = getattr(logging, loglevel, None)
        if not isinstance(numeric_level, int):
            raise ValueError('Invalid log level: %s' % loglevel)

        fmt = '%(asctime)s::%(levelname)s:%(name)s:%(message)s'
        if logfile is not None:
            logging.basicConfig(filename=logfile,
                                filemode='a',
                                datefmt='%Y-%m-%d %H:%M:%S',
                                level=numeric_level,
                                format=fmt)
        else:
            logging.basicConfig(level=numeric_level)
            coloredlogs.install(level=numeric_level, fmt=fmt, datefmt='%Y-%m-%d %H:%M:%S')

        self._loglevel = numeric_level
        self._logger = logging.getLogger(__name__)
        
        # add the title
        title = self.title()
        for line in title:
            self._logger.info(line)
        self._logger.info('wxdb started {}'.format(datetime.now()))
        
        # check to see if metadata or data are in the config file
        self.load_metadata = False
        self.load_data = False
        
        # process the metadata section
        if 'metadata' in self.config.keys():
            self.config['metadata']['sources'] = strip_string(self.config['metadata']['sources'])
            self._logger.info('Metadata section found, will load metadata from sources {}'
                              .format(self.config['metadata']['sources']))
            self.load_metadata = True
            
        # process the data section
        if 'data' in self.config.keys():
            self.config['data'] = self.process_config(self.config['data'])
            self.load_data = True

#         if (not self.load_metadata) & (not self.load_data):
#             raise Exception('[metadata] or [data] are not specified in the config file')
        
        # quality control data
        if 'quality_control' in self.config.keys():
            # parse the options
            self.perform_qc = True
        else:
            self.perform_qc = False
#             self.config['quality_control'] = False
    
        # automated cleaning of input data
        if 'acid' in self.config.keys():
            # can't do qc and acid at the same time
            self.perform_acid = True
            self.perform_qc = False
            
            self.config['acid'] = self.process_config(self.config['acid'])
        else:
            self.perform_acid = False
            
    def process_config(self, d):
        """
        Process the config settings that will be the same between data and acid. This
        will be replace with inicheck at some point
        """
        
        k = d.keys()
        
        if  'sources' in k:
            d['sources'] =  strip_string(d['sources'])
            self._logger.info('data section found, will load data from sources {}'
                              .format(d['sources']))
        
        if 'client' in k:
            d['client'] = strip_string(d['client'])
        else:
            raise Exception('client must be specified in the [data] config section')
        
        if 'timezone' not in k:
            d['timezone'] = 'UTC'
            
        if 'start_time' not in k:
            d['start_time'] = None
        
        if 'end_time' not in k:
            d['end_time'] = None
            
        return d
     
    def get_metadata(self):
        """
        Get the metadata from the sources
        """
        
        for s in self.config['metadata']['sources']:
            if s == 'mesowest':
                Mesowest(self.db, self.config['mesowest_metadata']).metadata()
            elif s == 'cdec':
                CDEC(self.db).metadata()
                
        # update the station from tbl_station_update
        self.update_stations()
                
    def get_data(self):
        """
        Get the current data from the sources
        
        TODO probably don't need the sources but can use the tbl_stations_view to
        get everything needed
        """
        
        if self.perform_qc:
            qc = QC(self.config['quality_control'], self.db)
        else:
            qc = False
        
        for s in self.config['data']['sources']:
            if s == 'mesowest':
                Mesowest(self.db, self.config['data'], qc).data()
            elif s == 'cdec':
                CDEC(self.db, self.config['data'], qc).data()
                
    def qc_data(self):
        """
        The Automatic Cleaning of Input Data (ACID) requires longer timeseries
        of data than is typical for the interval that the data is retireved. So
        ACID must be performed after the data is loaded into level1 data then
        pulled back out
        """
        
        ACID(self.config['acid'], self.db).run()      
                
    def update_stations(self):
        """
        Update the station metadata from the table tbl_station_update. This
        table contains corrected metadata mainly for station coordinates.
        Perform a join of the two tables by updating the fields provided
        in tbl_station_update
        """
        
        self.db.db_connect()
        cursor = self.db.cnx.cursor()
        
        qry = "SELECT * FROM tbl_station_update"
        
        cursor.execute(qry)
        su = cursor.fetchall()
        self.db.db_close()
        
        # convert to a dataframe
        df = pd.DataFrame(su, columns=['id', 'primary_id', 'station_name',
                                       'latitude','longitude', 'elevation',
                                       'utm_x', 'utm_y', 'utm_zone'])
        del df['id']
        
        self.db.update_data(df, 'metadata', where='primary_id', description='updating station metadata')
        
        
    def run(self):
        """
        Run the collection for metadata and data
        """   
        
        startTime = datetime.now()
        
        # connect to the database
        self.db = Database(self.config['mysql'])
        
        if self.load_metadata:
            self.get_metadata()
            
        if self.load_data:
            self.get_data()
            
        if self.perform_acid:
            self.qc_data()
            
#         if self.perform_qc:
#             QC(self.config['quality_control']).run()
                    
        self._logger.info('Elapsed time: {}'.format(datetime.now() - startTime))
        self._logger.info('Done')    
        
    def title(self):                                                                            
        return ['                                                                        dddddddd bbbbbbbb',       
        '                                                                        d::::::d b::::::b ',   
        '                                                                        d::::::d b::::::b  ',          
        '                                                                        d::::::d b::::::b   ',         
        '                                                                        d:::::d   b:::::b    ',        
        'wwwwwww           wwwww           wwwwwww xxxxxxx      xxxxxxx  ddddddddd:::::d   b:::::bbbbbbbbb',    
        ' w:::::w         w:::::w         w:::::w  x:::::x    x:::::x dd::::::::::::::d   b::::::::::::::bb',  
        '  w:::::w       w:::::::w       w:::::w    x:::::x  x:::::x d::::::::::::::::d   b::::::::::::::::b', 
        '   w:::::w     w:::::::::w     w:::::w      x:::::xx:::::x d:::::::ddddd:::::d   b:::::bbbbb:::::::b',
        '    w:::::w   w:::::w:::::w   w:::::w        x::::::::::x  d::::::d    d:::::d   b:::::b    b::::::b',
        '     w:::::w w:::::w w:::::w w:::::w          x::::::::x   d:::::d     d:::::d   b:::::b     b:::::b',
        '      w:::::w:::::w   w:::::w:::::w           x::::::::x   d:::::d     d:::::d   b:::::b     b:::::b',
        '       w:::::::::w     w:::::::::w           x::::::::::x  d:::::d     d:::::d   b:::::b     b:::::b',
        '        w:::::::w       w:::::::w           x:::::xx:::::x d::::::ddddd::::::dd  b:::::bbbbbb::::::b',
        '         w:::::w         w:::::w           x:::::x  x:::::x d:::::::::::::::::d  b::::::::::::::::b ',
        '          w:::w           w:::w           x:::::x    x:::::x d:::::::::ddd::::d  b:::::::::::::::b  ',
        '           www             www           xxxxxxx      xxxxxxx ddddddddd   ddddd  bbbbbbbbbbbbbbbb   ']
                                                                                                  
                                                                                                  
                                                                                                  
                                                                                                  
                                                                                                  
                                                                                                  
                                                                                                  

    
class MyParser(ConfigParser):
    def as_dict(self):
        d = dict(self._sections)
        for k in d:
            d[k] = dict(self._defaults, **d[k])
            d[k].pop('__name__', None)
        return d
    

    
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description="""Quick way to work with the weather database 
                                                to add metadata or station data""")
    
    parser.add_argument('config_file', type=str)
    
    args = parser.parse_args()
    
    try:
        w = Weather(args.config_file)
        w.run()
    except Exception as e:
        traceback.print_exc()

        
        
        
        
    
    
    