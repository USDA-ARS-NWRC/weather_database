"""
2017-06-30
author: Micah Johnson
"""
import os
import sys
import logging as out
import ConfigParser
import mysql.connector

out.basicConfig(level=out.DEBUG)

def get_connection(config = None):
    #Pass in the config file via the args of method
    if config != None:
        config_file = config
    #pass in the config file via the sys.argv
    elif len(sys.argv) > 1:
        config_file = sys.argv[1]
    else:
        out.error("Config specifying the users credentials not found.\n Pleas specify a file path for the config file.")

    cfg = ConfigParser.ConfigParser()
    cfg.read(config_file)
    cred = cfg._sections['connection']
    #Remove the parser added __name__ key
    del cred['__name__']

    try:
        cnx = mysql.connector.connect(**cred)

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            out.info("Something is wrong with your user name or password")

        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            out.info("Database does not exist")

        else:
            out.info(err)
            cnx.close()

    return cnx



class DBStation():
    """
    DBStation is designed to access and manipulate single station data that is
    already inputted into the ARS DB.
    """
    def __init__(self,cnx,name):
        self.cursor = cnx.cursor()
        self.check_station_existence(name)
        #Use tbl_meta_data to retrieve generic info on station
        self.x = 0
        self.y = 0

    def check_station_existence(self, name):
        """
        Determine if the user's inputted station name exists, if not recommend
        an alternative and exit program.
        """
        cursor = self.cursor
        qry = "SELECT primary_id from tbl_metadata"
        cursor.execute(qry)
        stations = cursor.fetchall()
        exists = False
        matches = []
        #Search all the stations
        for s in stations:
            lname = name.lower()

            #Assign the correct name
            if s[0].lower() == lname:
                self.name = s[0]
                exists = True

        if not exists:
            out.info("Station name {0} does not exist in database.".format(name))
            for s in stations:
                char_match = 0
                exact_match = 0
                sta = s[0].lower()
                #cycle through the shortest name to avoid over index
                if len(sta) > len(lname):
                    comparator = len(lname)

                else:
                    comparator = len(sta)
                #look for matches where order matters
                for i in range(comparator):
                    if sta[i] == lname[i]:
                        exact_match+=1.0

                if len(sta) > 0:
                    if exact_match/len(sta) >= 0.70:
                        matches.append(s[0])
            if len(matches) >0:
                out.info("Did you mean station:")
                for s in matches:
                    out.info('\t{0}'.format(s))

                #
                #     else:
                #         for i,l in emumerate(comparator:
                #             if l in lname:
                #                 char_match +=1.0
                #
                # if len(s[0]) > 0:
                #     if char_match/len(s[0]) > 0.80:
                #         out.info("Station name = {0}, match = {1}".format(s,char_match/len(s[0])))


            raise ValueError("Station name {0} was not found in database.".format(name))

    def get_variable(name,start,end):
        pass

if __name__ == '__main__':
    cnx = get_connection('./user_config.ini')
    sta = DBStation(cnx,'VNN21')
