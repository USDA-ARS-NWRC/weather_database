'''
20151228 Scott Havens

Update the station coordinates from the stationCoords.csv file
'''

import csv

print 'Updating station coordinates from stationCoords.csv...'

# connect to db, returns connection cnx
execfile('database_connect.py')
cursor = cnx.cursor()


# stationCoords.csv
with open('stationCoords.csv', 'rU') as f:
    
    qstr = "INSERT INTO tbl_metadata (primary_id,latitude,longitude,elevation,X,Y) VALUES ('%s',%s,%s,%s,%s,%s)" \
        " ON DUPLICATE KEY UPDATE latitude='%s',longitude='%s',elevation='%s',X='%s',Y='%s'";

    for row in csv.DictReader(f):

        row['X'] = row.get('X','NULL') or 'NULL'
        row['Y'] = row.get('Y','NULL') or 'NULL'
        qry = qstr % (row['primary_id'], row['latitude'], row['longitude'], row['elevation'], row['X'], row['Y'],\
                      row['latitude'], row['longitude'], row['elevation'], row['X'], row['Y'])

        cursor.execute(qry)

    f.close()

cnx.close()

print 'Done.'
