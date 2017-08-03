# -*- coding: utf-8 -*-

import os
import argparse
import logging
import coloredlogs
from datetime import datetime

try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser
    
from database import Database
from mesowest import Mesowest

__author__ = "Scott Havens"
__maintainer__ = "Scott Havens"
__email__ = "scott.havens@ars.usda.gov"
__date__ = "2017-07-27"

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
        self.load_current = False
        
        # process the metadata section
        if 'metadata' in self.config.keys():
            self._logger.info('Metadata section found, will load metadata from sources {}'
                              .format(self.config['metadata']['sources']))
            self.load_metadata = True
            self.config['metadata']['sources'] = self.config['metadata']['sources'].split(',')
            
        # process the current section
        if 'current' in self.config.keys():
            self._logger.info('Current section found, will load current data from sources {}'
                              .format(self.config['current']['sources']))
            self.load_current = True
            self.config['current']['sources'] = self.config['current']['sources'].split(',')
            
            if 'client' in self.config['current'].keys():
                self.config['current']['client'] = self.config['current']['client'].split(',')
            else:
                raise Exception('client must be specified in the [current] config section')
            
            if 'timezone' not in self.config['current'].keys():
                self.config['current']['timezone'] = 'US/Mountain'
            
        if (not self.load_metadata) & (not self.load_current):
            raise Exception('[metadata] or [current] are not specified in the config file')
            
     
    def get_metadata(self):
        """
        Get the metadata from the sources
        """
        
        for s in self.config['metadata']['sources']:
            if s == 'mesowest':
                m = Mesowest(self.db, self.config['mesowest_metadata']).metadata()
                
    def get_current(self):
        """
        Get the current data from the sources
        """
        
        for s in self.config['current']['sources']:
            if s == 'mesowest':
                m = Mesowest(self.db, self.config['current']).current_data()
        
    def run(self):
        """
        Run the collection for metadata and data
        """   
        
        startTime = datetime.now()
        
        # connect to the database
        self.db = Database(self.config['mysql'])
        
        if self.load_metadata:
            self.get_metadata()
            
        if self.load_current:
            self.get_current()
            
        self.db.cnx.close()
            
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
        print(e)

        
        
        
        
    
    
    