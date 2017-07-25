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
    
    qstr = "INSERT INTO tbl_metadata (primary_id,station_name,latitude,longitude,elevation,X,Y) VALUES ('%s','%s',%s,%s,%s,%s,%s)" \
        " ON DUPLICATE KEY UPDATE latitude=%s,longitude=%s,elevation=%s,X=%s,Y=%s"
    
    qstr = "UPDATE tbl_metadata SET station_name='%s',latitude=%s,longitude=%s,elevation=%s,X=%s,Y=%s WHERE primary_id='%s'"

    for row in csv.DictReader(f):

        row['X'] = row.get('X','NULL') or 'NULL'
        row['Y'] = row.get('Y','NULL') or 'NULL'
#        qry = qstr % (row['primary_id'], row['station_name'], row['latitude'], row['longitude'], row['elevation'], row['X'], row['Y'],\
#                      row['latitude'], row['longitude'], row['elevation'], row['X'], row['Y'])
        qry = qstr % (row['station_name'], row['latitude'], row['longitude'], row['elevation'], row['X'], row['Y'], row['primary_id'])
        cursor.execute(qry)

    f.close()
    
# remove one of the overlaping but uneeded CDEC stations
cursor.execute("DELETE FROM tbl_metadata WHERE primary_id='SCR' AND source='CDEC'")
cursor.execute("DELETE FROM tbl_metadata WHERE primary_id='SVT' AND source='CDEC'")

cnx.close()

print 'Done.'
