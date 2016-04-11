#!/bin/bash         
# 20141130 Scott Havens
#
# MySQL login info

#host=localhost
#user=wxuser_v2
#pass=x340hm4h980r

host=10.200.28.203
user=scott
pass=avalanche
db=weather_v2

# get the time
DATE=`date +%Y_%m_%d`

# create the backup file name
FILENAME=backup_$DATE.sql.gz
echo $FILENAME

# create the backup
mysqldump -u $user -p$pass -h$host $db tbl_stations | gzip -9 > $FILENAME

# move this file to SHARE3
# somehow

#smb://arsidboi6na0001/SHARE3/Mac_Files/Snowserver/db_backup
