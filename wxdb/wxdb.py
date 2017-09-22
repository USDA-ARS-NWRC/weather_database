# -*- coding: utf-8 -*-

import os
import argparse
import logging
import coloredlogs
from datetime import datetime
import traceback

try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser
    
from database import Database
from mesowest import Mesowest
from cdec import CDEC
from quality_control import QC

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

        fmt = '%(levelname)s:%(name)s:%(message)s'
        if logfile is not None:
            logging.basicConfig(filename=logfile,
                                filemode='w',
                                level=numeric_level,
                                format=fmt)
        else:
            logging.basicConfig(level=numeric_level)
            coloredlogs.install(level=numeric_level, fmt=fmt)

        self._loglevel = numeric_level
        self._logger = logging.getLogger(__name__)
        
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
            
            self.config['data']['sources'] =  strip_string(self.config['data']['sources'])
            self._logger.info('data section found, will load data from sources {}'
                              .format(self.config['data']['sources']))
            self.load_data = True
            
            k = self.config['data'].keys()
            if 'client' in k:
                self.config['data']['client'] = strip_string(self.config['data']['client'])
            else:
                raise Exception('client must be specified in the [data] config section')
            
            if 'timezone' not in k:
                self.config['data']['timezone'] = 'UTC'
                
            if 'start_time' not in k:
                self.config['data']['start_time'] = None
            
            if 'end_time' not in k:
                self.config['data']['end_time'] = None
            
            
        if (not self.load_metadata) & (not self.load_data):
            raise Exception('[metadata] or [data] are not specified in the config file')
        
        # quality control data
        if 'quality_control' in self.config.keys():
            # parse the options
            self.perform_qc = True
            pass
        else:
            self.perform_qc = False
#             self.config['quality_control'] = False
            
     
    def get_metadata(self):
        """
        Get the metadata from the sources
        """
        
        for s in self.config['metadata']['sources']:
            if s == 'mesowest':
                Mesowest(self.db, self.config['mesowest_metadata']).metadata()
            elif s == 'cdec':
                CDEC(self.db).metadata()
                
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
            
#         if self.perform_qc:
#             QC(self.config['quality_control']).run()
                    
        self._logger.info('Elapsed time: {}'.format(datetime.now() - startTime))
        self._logger.info('Done')    
    
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

        
        
        
        
    
    
    