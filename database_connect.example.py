'''
20151221 Scott Havens

Connect to the database
'''

import mysql.connector
from mysql.connector import errorcode

host = 'host'
user = 'user'
password = 'password'
db = 'database'

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
