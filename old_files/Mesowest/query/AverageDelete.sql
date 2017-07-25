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
declare c_round_date DATETIME;
declare c_cnt INT;
declare done int default 0;
declare stations_updated VARCHAR(2048) DEFAULT 'Station List - ';
declare count int default 0;
declare r int default 3600;

/* perform some clean up */
-- DELETE FROM tbl_level1 WHERE station_id=' ';

/* create a cursor to iterate on all the multiple values */
DECLARE c1 CURSOR FOR 
SELECT 
    COUNT(*) AS cnt,
    station_id,
	date_time,
    FROM_UNIXTIME(FLOOR((UNIX_TIMESTAMP(date_time) + r-1) / r) * r) AS round_date
FROM
    tbl_level1
GROUP BY station_id , round_date;
LIMIT 20;

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
	FETCH c1 INTO c_cnt,c_station_id,c_date_time,c_round_date;
	-- select c_cnt,c_station_id,c_date_time;

	SELECT c_cnt,c_station_id,c_date_time,c_round_date;
	
	IF c_cnt > 1 THEN
		/* get the rows that match the current and insert into the temp table */
		TRUNCATE TABLE duplicates;
		INSERT INTO duplicates SELECT * FROM tbl_level1 WHERE station_id=c_station_id AND date_time BETWEEN DATE_SUB(c_round_date, INTERVAL r-1 SECOND) AND c_round_date;
		
		/* delete duplicates from tbl_level1 */
		-- DELETE FROM tbl_level1 WHERE station_id=c_station_id AND date_time BETWEEN DATE_SUB(c_date_time, INTERVAL r/2 SECOND) AND DATE_ADD(c_date_time, INTERVAL r/2-1 SECOND);
		DELETE FROM tbl_level1 WHERE date_time IN (SELECT date_time FROM duplicates);

		/* average the values in duplicates */
		INSERT INTO tbl_level1 SELECT station_id,c_round_date,AVG(air_temp),AVG(dew_point_temperature),AVG(relative_humidity),AVG(wind_speed),AVG(wind_direction),AVG(wind_gust),AVG(solar_radiation),AVG(snow_smoothed),AVG(precip_accum),AVG(snow_depth),AVG(snow_accum),AVG(precip_storm),AVG(snow_interval),AVG(snow_water_equiv),AVG(vapor_pressure),AVG(cloud_factor) FROM duplicates;
		-- SELECT * from duplicates;

	ELSE
		IF TIMEDIFF(c_date_time, c_round_date) != 0 THEN
			UPDATE tbl_level1 SET date_time=c_round_date where date_time=c_date_time and station_id=c_station_id;
		END IF;

	END IF;

	SELECT station_id,date_time from tbl_level1 where station_id=c_station_id and date_time=c_round_date;

UNTIL done 
END REPEAT;

/* close the cursor */
CLOSE c1;

/* remove the temp table */
DROP TEMPORARY TABLE IF EXISTS duplicates;
        

END//

delimiter ;

CALL averageDelete();