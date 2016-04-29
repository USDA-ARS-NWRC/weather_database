CREATE TABLE IF NOT EXISTS tbl_raw_data (
    station_id VARCHAR(10),
    date_time DATETIME,
    air_temp DECIMAL(10 , 3 ),
    dew_point_temperature DECIMAL(10 , 3 ),
    relative_humidity DECIMAL(10 , 3 ),
    wind_speed DECIMAL(10 , 3 ),
    wind_direction DECIMAL(10 , 3 ),
    wind_gust DECIMAL(10 , 3 ),
    solar_radiation DECIMAL(10 , 3 ),
    snow_smoothed DECIMAL(10 , 3 ),
    precip_accum DECIMAL(10 , 3 ),
    snow_depth DECIMAL(10 , 3 ),
    snow_accum DECIMAL(10 , 3 ),
    precip_storm DECIMAL(10 , 3 ),
    snow_interval DECIMAL(10 , 3 ),
    snow_water_equiv DECIMAL(10 , 3 ),
	vapor_pressure DECIMAL (10 , 3),
	cloud_factor DECIMAL(10 , 3),
    PRIMARY KEY(station_id,date_time)
);

-- tbl_level1 is the automatic data QC and organization
CREATE TABLE IF NOT EXISTS tbl_level1 LIKE tbl_raw_data;
ALTER IGNORE TABLE tbl_level1 ADD COLUMN av_del TINYINT(1) DEFAULT 0;

-- tbl_level2 is the users accessing the db and changing the data
CREATE TABLE IF NOT EXISTS tbl_level2 LIKE tbl_raw_data;

ALTER IGNORE TABLE tbl_level2 ADD COLUMN user VARCHAR(20) FIRST,
ADD COLUMN date_fixed DATETIME AFTER user;

-- ALTER IGNORE TABLE tbl_level1 ADD COLUMN cloud_factor DECIMAL(10 , 3);
