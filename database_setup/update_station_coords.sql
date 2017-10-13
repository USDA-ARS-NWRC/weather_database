load data local infile 'stationCoords.csv' 
into table tbl_station_update
COLUMNS TERMINATED BY ','
ESCAPED BY '"'
LINES TERMINATED BY '\r'
IGNORE 1 LINES
(primary_id, @station_name, @latitude, @longitude, @elevation, @utm_x, @utm_y, @utm_zone)
SET
station_name = nullif(@station_name,''),
latitude = nullif(@latitude,''),
longitude = nullif(@longitude,''),
elevation = nullif(@elevation,''),
utm_x = nullif(@utm_x,''),
utm_y = nullif(@utm_y,''),
utm_zone = nullif(@utm_zone,'');