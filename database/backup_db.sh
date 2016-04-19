#!/bin/bash         
# 20141130 Scott Havens
#
# MySQL login info

host=localhost
user=wxuser_v2
pass=x340hm4h980r
db=weather_v2

snap_user=snowserver
snap_pass=85bhTcEBSFeyvmqk84rmxa67
# get the time
DATE=`date +%Y_%m_%d`

printf '%50s\n' | tr ' ' -
printf ' *** DATABASE BACKUP ***\n'
printf '%50s\n' | tr ' ' -

# create the backup file name
FILENAME=backup_$DATE.sql.gz
echo $FILENAME

# create the backup
/usr/local/mysql/bin/mysqldump -u $user -p$pass -h$host $db | gzip -9 > $FILENAME

# move this file to SHARE3
#smb://arsidboi6na0001/SHARE3/Mac_Files/Snowserver/db_backup

/sbin/mount_smbfs //$snap_user:$snap_pass@10.200.28.211/SHARE3 ~/smb_mount/
mv $FILENAME /Users/scott/smb_mount/Mac_Files/Snowserver/db_backup/$FILENAME
umount /Users/scott/smb_mount