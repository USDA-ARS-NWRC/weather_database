# 20160204 Andrew Hedrick
#
# Download 2014 and 2015 CDEC data and put on database

python CDEC/download_CDEC_historic14.py

python CDEC/download_CDEC_historic15.py

python database/averageDelete.py