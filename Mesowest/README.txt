README.txt
MesowestAPI_v2

20151210 Scott Havens

The 2nd version of the API which will hopefully clean up some of the tables in the database as not all 100 variables are needed.  This will make it fasters and more manageable.

Create the database weather_v2

** Station metadata **

1. tbl_metadata - create a metadata table to store station infromation, query/CreateMetadataTable.sql
2. Add station data to tbl_metadata, ‘sh metadata.sh’
2.1 This calls ‘query/LoadMetadata.sql’


** Create the data tables **

tbl_raw_data - stores the raw data as obtained by Mesowest
tbl_level1 - automatic QC that occurs when data is downloaded
tbl_level2 - user correct data, extra column date_fixed,user to show the last user/time when a data point was updated


** Create the stations table **

tbl_stations - Track what stations should be downloaded and to where they belong

source query/CreateStationTable.sql


** Load stored procedures into MySQL ***
Procedure to remove duplicates and average over the hour

1. open MySQL
2. source query/AverageDelete.sql


** getData.php **

1. Get data from Mesowest, load into tbl_raw_data & tbl_level1
2. Average and remove duplicates from tbl_level1

** Add index to tables **
For faster lookup and queries, add index in query/AddIndex.sql





