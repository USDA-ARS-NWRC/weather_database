#!/bin/bash         
# 20151228 Scott Havens
#
# Download the data for all the sources

BASEDIR=$(dirname $0)
echo $BASEDIR

printf '%50s\n' | tr ' ' -
printf ' *** CDEC ***\n'
printf '%50s\n' | tr ' ' -

# download the CDEC
python $BASEDIR/CDEC/download_CDEC.py


printf '%50s\n' | tr ' ' -
printf ' *** MESOWEST ***\n'
printf '%50s\n' | tr ' ' -

# download the Mesowest and call AverageDelete.sql for tbl_level1
php $BASEDIR/Mesowest/getData.php