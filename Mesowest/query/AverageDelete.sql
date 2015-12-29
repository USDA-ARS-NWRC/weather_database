/*
Go through the tbl_level1 table and average times with more than one measurement

20150213 Scott Havens
*/

DROP PROCEDURE IF EXISTS averageDelete;

delimiter //

CREATE PROCEDURE averageDelete ()
BEGIN

/* define variable that will be used */
declare c_station_id VARCHAR(10);
declare c_date_time DATETIME;
declare c_cnt INT;
declare done int default 0;
declare stations_updated VARCHAR(2048) DEFAULT 'Station List - ';
declare count int default 0;


/* create a cursor to iterate on all the multiple values */
DECLARE c1 CURSOR FOR 
SELECT 
    COUNT(*) AS cnt, station_id, date_time
FROM
    tbl_level1
GROUP BY station_id , date_time
HAVING COUNT(*) > 1;


/* ensure no data error */
DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET done = 1;

/* create a temporary table to work in */
DROP TEMPORARY TABLE IF EXISTS duplicates;
CREATE TEMPORARY TABLE duplicates LIKE tbl_level1;

/* execute the cursor */
OPEN c1;

/* loop over each duplicate times in cursor */
REPEAT
    
	/* fetch one row with table name in this_sta */
	FETCH c1 INTO c_cnt,c_station_id,c_date_time;
	-- select c_cnt,c_station_id,c_date_time;

	
	/* get the rows that match the current and insert into the temp table */
	TRUNCATE TABLE duplicates;
	INSERT INTO duplicates SELECT * FROM tbl_level1 WHERE station_id=c_station_id AND date_time=c_date_time;
	
	/* delete duplicates from tbl_level1 */
	DELETE FROM tbl_level1 WHERE station_id=c_station_id AND date_time=c_date_time;

	/* average the values in duplicates */
	INSERT INTO tbl_level1 SELECT station_id,date_time,AVG(air_temp),AVG(dew_point_temperature),AVG(relative_humidity),AVG(wind_speed),AVG(wind_direction),AVG(wind_gust),AVG(solar_radiation),AVG(snow_smoothed),AVG(precip_accum),AVG(snow_depth),AVG(snow_accum),AVG(precip_storm),AVG(snow_interval),AVG(snow_water_equiv),AVG(vapor_pressure) FROM duplicates;
	-- SELECT * from duplicates;


UNTIL done 
END REPEAT;

/* close the cursor */
CLOSE c1;

/* remove the temp table */
DROP TEMPORARY TABLE IF EXISTS duplicates;
        

END//

delimiter ;

-- CALL averageDelete();