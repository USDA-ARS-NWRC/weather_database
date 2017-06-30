"""
2017-06-30
author: Micah Johnson
"""
import os
import sys
import logging as out
import ConfigParser
import mysql.connector
from datetime import datetime
from matplotlib import pyplot as plt
import pandas as pd
out.basicConfig(level=out.DEBUG)

def get_connection(config = None):
    """
    Retrieve the connection to the mysql database
    """

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

def verify_name(name,options):
    """
    Checks to see if the value provided is in the list of options. if not looks
    for possible suggestions.
    args:
        name - string value of option being searched for
        options - list of string values that value is being looked for.

    returns:
        correct_name - if the name was different in terms of case, but matched
            on a lower case basis, the original name in the options is returned
    """
    exists = False
    matches = []
    #Search all the option
    lname = name.lower()

    for s in options:
        #Assign the correct name
        if s.lower() == lname:
            correct_name = s
            exists = True

    if not exists:
        out.info("Name {0} does not appear to be an option.".format(name))
        for s in options:
            exact_match = 0
            sta = s.lower()
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
                    matches.append(s)

        #If no matches were found looking at options where order matters, try
        #without looking at order
        if len(matches) == 0:
            for s in options:
                l_s = len(s)
                char_match = 0
                sta = s.lower()

                for c in sta:
                    if c in lname:
                        char_match+=1.0

                if l_s > 0:
                    if char_match/l_s >= 0.80:
                        matches.append(s)

        if len(matches) > 0:
            out.info("Did you mean:")
            for s in matches:
                out.info('\t{0}'.format(s))
        out.info("\n")
        raise ValueError("Station name {0} was not found in database.".format(name))

    return correct_name


class DBStation():
    """
    DBStation is designed to access and manipulate single station data that is
    already inputted into the ARS DB.
    """
    def __init__(self,cnx,name):
        self.cnx = cnx
        self.cursor = cnx.cursor()
        self.set_station(name)
        #Use tbl_meta_data to retrieve generic info on station
        self.x = 0
        self.y = 0

        qry = "SELECT table_name FROM information_schema.tables";
        tables = self.send(qry)
        self.available_tables = [t[0] for t in tables]


    def send(self,qry):
        cursor = self.cursor
        try:
            cursor.execute(qry)
            result = cursor.fetchall()
        except Exception as e:
            raise Exception(e)

        return result


    def set_station(self, name):
        """
        sets the attributes of a station
        args:
            name - string value of the primary id of a weather station

        returns:
            None

        """
        qry = "SELECT primary_id from tbl_metadata"
        stations = self.send(qry)

        correct_stations = [s[0] for s in stations]

        #Set station name to the recognized station name
        self.name = verify_name(name,correct_stations)


    def get_variable(self,field_name,start,end, table_level = 2):
        if table_level == 1:
            table = "tbl_level1"
        elif table_level == 1.5:
            table = "tbl_level1_5"
        elif table_level == 2:
            table = "tbl_level2"
        else:
            table = table_level

        #Check table existence and recommend if not.
        table = verify_name(table,self.available_tables)

        #check field_name existence and recommend if not.
        qry = "SELECT column_name FROM information_schema.columns WHERE table_name='{0}'".format(table)
        available_fields = self.send(qry)
        available_fields = [f[0] for f in available_fields]
        field_name = verify_name(field_name,available_fields)

        #Select the request variable and date confined by table and date range
        qry = "SELECT date_time,{0} FROM {1} WHERE (station_id='{2}' AND date_time BETWEEN '{3}' AND '{4}') ".format(field_name, table, self.name, start, end)
        data = pd.read_sql(qry, self.cnx, index_col='date_time')

        return data


if __name__ == '__main__':
    cnx = get_connection('./user_config.ini')
    sta = DBStation(cnx,'PRAI1')

    data = sta.get_variable('air_temp','2000-12-01--15:00','2005-01-01--16:00')
    plt.plot(data)
    plt.show()
