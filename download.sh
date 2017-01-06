#!/bin/sh
# 20151228 Scott Havens
#
# Download the data for all the sources

cd /home/public/WxDataDownload
BASEDIR=$(dirname $0)
echo $BASEDIR

. $BASEDIR/../virturalenv/wxdb/bin/activate

printf '%50s\n' | tr ' ' -
printf ' *** CDEC ***\n'
printf '%50s\n' | tr ' ' -

# download the CDEC
python $BASEDIR/CDEC/download_CDEC.py


printf '%50s\n' | tr ' ' -
printf ' *** MESOWEST ***\n'
printf '%50s\n' | tr ' ' -

# download the Mesowest for tbl_level1
python $BASEDIR/Mesowest/download_Mesowest_historic.py --current db_config.json


printf '%50s\n' | tr ' ' -
printf ' *** Average and delete from tbl_level1 ***\n'
printf '%50s\n' | tr ' ' -

# call database/averageDelete.py to average and delete to the hour timestep
python $BASEDIR/database/averageDelete.py

deactivate
