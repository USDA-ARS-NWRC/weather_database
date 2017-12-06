INSERT INTO weather_db.tbl_stations (client,metadata_id) 
SELECT
    t1.client,
    t2.id as metadata_id
FROM
    wxdb_import.tbl_stations t1
LEFT OUTER JOIN
    weather_db.tbl_metadata t2 ON t1.station_id = t2.primary_id
WHERE t2.id is not null;
