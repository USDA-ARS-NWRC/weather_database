DELIMITER $$

CREATE TRIGGER `weather_db`.`tbl_level2_AFTER_INSERT` AFTER INSERT ON `tbl_level2` FOR EACH ROW
BEGIN

	-- Find username of person performing the DELETE into table
    DECLARE vUser varchar(50);
	SELECT USER() INTO vUser;

	if new.air_temp is not null then
		INSERT INTO `tbl_level2_audit` VALUES (null,'insert',CURRENT_USER(),CURRENT_TIMESTAMP(),new.id,'air_temp',new.air_temp);
	end if;

	if new.dew_point_temperature is not null then
		INSERT INTO `tbl_level2_audit` VALUES (null,'insert',CURRENT_USER(),CURRENT_TIMESTAMP(),new.id,'dew_point_temperature',new.dew_point_temperature);
	end if;

	if new.relative_humidity is not null then
		INSERT INTO `tbl_level2_audit` VALUES (null,'insert',CURRENT_USER(),CURRENT_TIMESTAMP(),new.id,'relative_humidity',new.relative_humidity);
	end if;

	if new.wind_speed is not null then
		INSERT INTO `tbl_level2_audit` VALUES (null,'insert',CURRENT_USER(),CURRENT_TIMESTAMP(),new.id,'wind_speed',new.wind_speed);
	end if;

	if new.wind_direction is not null then
		INSERT INTO `tbl_level2_audit` VALUES (null,'insert',CURRENT_USER(),CURRENT_TIMESTAMP(),new.id,'wind_direction',new.wind_direction);
	end if;

	if new.wind_gust is not null then
		INSERT INTO `tbl_level2_audit` VALUES (null,'insert',CURRENT_USER(),CURRENT_TIMESTAMP(),new.id,'wind_gust',new.wind_gust);
	end if;

	if new.solar_radiation is not null then
		INSERT INTO `tbl_level2_audit` VALUES (null,'insert',CURRENT_USER(),CURRENT_TIMESTAMP(),new.id,'solar_radiation',new.solar_radiation);
	end if;

	if new.snow_smoothed is not null then
		INSERT INTO `tbl_level2_audit` VALUES (null,'insert',CURRENT_USER(),CURRENT_TIMESTAMP(),new.id,'snow_smoothed',new.snow_smoothed);
	end if;

	if new.precip_accum is not null then
		INSERT INTO `tbl_level2_audit` VALUES (null,'insert',CURRENT_USER(),CURRENT_TIMESTAMP(),new.id,'precip_accum',new.precip_accum);
	end if;

	if new.precip_intensity is not null then
		INSERT INTO `tbl_level2_audit` VALUES (null,'insert',CURRENT_USER(),CURRENT_TIMESTAMP(),new.id,'precip_intensity',new.precip_intensity);
	end if;

	if new.snow_depth is not null then
		INSERT INTO `tbl_level2_audit` VALUES (null,'insert',CURRENT_USER(),CURRENT_TIMESTAMP(),new.id,'snow_depth',new.snow_depth);
	end if;

	if new.snow_interval is not null then
		INSERT INTO `tbl_level2_audit` VALUES (null,'insert',CURRENT_USER(),CURRENT_TIMESTAMP(),new.id,'snow_interval',new.snow_interval);
	end if;

	if new.snow_water_equiv is not null then
		INSERT INTO `tbl_level2_audit` VALUES (null,'insert',CURRENT_USER(),CURRENT_TIMESTAMP(),new.id,'snow_water_equiv',new.snow_water_equiv);
	end if;

	if new.vapor_pressure is not null then
		INSERT INTO `tbl_level2_audit` VALUES (null,'insert',CURRENT_USER(),CURRENT_TIMESTAMP(),new.id,'vapor_pressure',new.vapor_pressure);
	end if;

	if new.cloud_factor is not null then
		INSERT INTO `tbl_level2_audit` VALUES (null,'insert',CURRENT_USER(),CURRENT_TIMESTAMP(),new.id,'cloud_factor',new.cloud_factor);
	end if;

END;$$

DELIMITER ;