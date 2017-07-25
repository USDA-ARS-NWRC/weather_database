/*
20141205 Scott Havens

Load the metadata table
*/

-- LOAD DATA LOCAL INFILE '/home/scott/LocalHost/WxDataDownload/Mesowest/data/mesowest_metadata.awk'
LOAD DATA LOCAL INFILE '/Users/scott/Documents/Projects/Snowserver/WxDataDownload/Mesowest/data/mesowest_metadata.awk'
REPLACE INTO TABLE tbl_metadata
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n' 
IGNORE 3 LINES
(primary_id,secondary_id,station_name,state,country,latitude,longitude,elevation,mesowest_network_id,network_name,status,primary_provider_id,primary_provider,secondary_provider_id,secondary_provider,tertiary_provider_id,tertiary_provider,wims_id,source)
-- SET DATE = STR_TO_DATE(@dtime , '%Y%m%d/%H%i');

-- SELECT ROW_COUNT();

-- (STATION, LAT, LON, ELEV, SOMETHING, @dtime, TMPF, RELH, SKNT, GUST, DRCT, QFLG, DWPF, PRES, PMSL, ALTI, P03D, SOLR, WNUM, VSBY, CHC1, CHC2, CHC3, CIG, TLKE, FT, FM, HI6, LO6, PEAK, HI24, LO24, PREC, P01I, P03I, P06I, P24I, P05I, P10I, P15I, SNOW, PACM, SACM, WEQS, P30I, PWVP, TSOI, MSOI, STEN, TSRD, EVAP, TRD1, TRD2, TRD3, TRD4, TFZ1, TFZ2, TFZ3, TFZ4, RSS1, RSS2, RSS3, RSS4)
-- how to deal with '' fields to null
-- can reformat original data to \N or use the SET to one = nullif(@vone,'')
-- awk -F, '{for(i=1;i<=NF;++i){if($i==""){printf "\N"}else{printf $i} if(i<NF)printf ","} printf "\n"}' mesowest_small.dat > mesowest_awk.dat
