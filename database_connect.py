'''
20151221 Scott Havens

Connect to the database
'''

import mysql.connector
from mysql.connector import errorcode

host = '10.200.28.203'
# host = 'localhost'
user = 'wxuser_v2'
password = 'x340hm4h980r'

db = 'weather_v2'

try:
    cnx = mysql.connector.connect(user=user, password=password,
                              host=host,
                              database=db)
    
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
        
# else:
#     cnx.close()
