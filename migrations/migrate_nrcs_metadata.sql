INSERT INTO weather_db.tbl_metadata (primary_id,station_name,latitude,longitude,timezone,elevation,state,source,primary_provider,network,reported_lat,reported_long)
SELECT 
	primary_id,
	station_name,
    latitude,
    longitude,
    'America/Boise' AS timezone,
    elevation,
    state,
    source,
    primary_provider,
    network_name AS network,
    latitude AS reported_lat,
    longitude AS reported_long
FROM 
	wxdb_import.tbl_metadata 
WHERE source='NRCS';
