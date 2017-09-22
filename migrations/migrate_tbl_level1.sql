
drop procedure if exists migrate_raw;
delimiter #
create procedure migrate_raw()
begin

declare v_max int unsigned default 18892369;
declare v_counter int unsigned default 0;

  while v_counter < v_max do
	start transaction;
    	SELECT v_counter;
	INSERT INTO weather_db.tbl_level1 (station_id,date_time,air_temp,dew_point_temperature,relative_humidity,wind_speed,wind_direction,wind_gust,solar_radiation,snow_smoothed,precip_accum,snow_depth,snow_interval,snow_water_equiv,vapor_pressure,cloud_factor)
	SELECT 
		station_id,
		date_time,
		air_temp,
		dew_point_temperature,
		relative_humidity,
		wind_speed,
		wind_direction,
		wind_gust,
		solar_radiation,
		snow_smoothed,
		precip_accum,
		snow_depth,
		snow_interval,
		snow_water_equiv,
		vapor_pressure,
		cloud_factor
	FROM
		wxdb_import.tbl_level1
	WHERE
		station_id in (select distinct t2.primary_id from weather_db.tbl_stations t1 join weather_db.tbl_metadata t2 ON t1.metadata_id = t2.id)
	limit v_counter,1000000
	ON DUPLICATE KEY UPDATE
		station_id=values(station_id),
		date_time=values(date_time),
		air_temp=values(air_temp),
		dew_point_temperature=values(dew_point_temperature),
		relative_humidity=values(relative_humidity),
		wind_speed=values(wind_speed),
		wind_direction=values(wind_direction),
		wind_gust=values(wind_gust),
		solar_radiation=values(solar_radiation),
		snow_smoothed=values(snow_smoothed),
		precip_accum=values(precip_accum),
		snow_depth=values(snow_depth),
		snow_interval=values(snow_interval),
		snow_water_equiv=values(snow_water_equiv),
		vapor_pressure=values(vapor_pressure),
		cloud_factor=values(cloud_factor);
	commit;
    	SELECT ROW_COUNT();
    	set v_counter=v_counter+1000000;
  end while;

    
end #

delimiter ;

call migrate_raw();
    
drop procedure if exists migrate_raw;
