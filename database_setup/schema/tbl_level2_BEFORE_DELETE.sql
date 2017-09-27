DELIMITER $$

CREATE DEFINER = CURRENT_USER TRIGGER `weather_db`.`tbl_level2_BEFORE_DELETE` BEFORE DELETE ON `tbl_level2` FOR EACH ROW
BEGIN
if old.air_temp is not null then
	INSERT INTO `tbl_level2_audit` VALUES (null,'delete',CURRENT_USER(),CURRENT_TIMESTAMP(),old.id,'air_temp',old.air_temp);
end if;

if old.dew_point_temperature is not null then
	INSERT INTO `tbl_level2_audit` VALUES (null,'delete',CURRENT_USER(),CURRENT_TIMESTAMP(),old.id,'dew_point_temperature',old.dew_point_temperature);
end if;

if old.relative_humidity is not null then
	INSERT INTO `tbl_level2_audit` VALUES (null,'delete',CURRENT_USER(),CURRENT_TIMESTAMP(),old.id,'relative_humidity',old.relative_humidity);
end if;

if old.wind_speed is not null then
	INSERT INTO `tbl_level2_audit` VALUES (null,'delete',CURRENT_USER(),CURRENT_TIMESTAMP(),old.id,'wind_speed',old.wind_speed);
end if;

if old.wind_direction is not null then
	INSERT INTO `tbl_level2_audit` VALUES (null,'delete',CURRENT_USER(),CURRENT_TIMESTAMP(),old.id,'wind_direction',old.wind_direction);
end if;

if old.wind_gust is not null then
	INSERT INTO `tbl_level2_audit` VALUES (null,'delete',CURRENT_USER(),CURRENT_TIMESTAMP(),old.id,'wind_gust',old.wind_gust);
end if;

if old.solar_radiation is not null then
	INSERT INTO `tbl_level2_audit` VALUES (null,'delete',CURRENT_USER(),CURRENT_TIMESTAMP(),old.id,'solar_radiation',old.solar_radiation);
end if;

if old.snow_smoothed is not null then
	INSERT INTO `tbl_level2_audit` VALUES (null,'delete',CURRENT_USER(),CURRENT_TIMESTAMP(),old.id,'snow_smoothed',old.snow_smoothed);
end if;

if old.precip_accum is not null then
	INSERT INTO `tbl_level2_audit` VALUES (null,'delete',CURRENT_USER(),CURRENT_TIMESTAMP(),old.id,'precip_accum',old.precip_accum);
end if;

if old.precip_intensity is not null then
	INSERT INTO `tbl_level2_audit` VALUES (null,'delete',CURRENT_USER(),CURRENT_TIMESTAMP(),old.id,'precip_intensity',old.precip_intensity);
end if;

if old.snow_depth is not null then
	INSERT INTO `tbl_level2_audit` VALUES (null,'delete',CURRENT_USER(),CURRENT_TIMESTAMP(),old.id,'snow_depth',old.snow_depth);
end if;

if old.snow_interval is not null then
	INSERT INTO `tbl_level2_audit` VALUES (null,'delete',CURRENT_USER(),CURRENT_TIMESTAMP(),old.id,'snow_interval',old.snow_interval);
end if;

if old.snow_water_equiv is not null then
	INSERT INTO `tbl_level2_audit` VALUES (null,'delete',CURRENT_USER(),CURRENT_TIMESTAMP(),old.id,'snow_water_equiv',old.snow_water_equiv);
end if;

if old.vapor_pressure is not null then
	INSERT INTO `tbl_level2_audit` VALUES (null,'delete',CURRENT_USER(),CURRENT_TIMESTAMP(),old.id,'vapor_pressure',old.vapor_pressure);
end if;

if old.cloud_factor is not null then
	INSERT INTO `tbl_level2_audit` VALUES (null,'delete',CURRENT_USER(),CURRENT_TIMESTAMP(),old.id,'cloud_factor',old.cloud_factor);
end if;

END;$$

DELIMITER ;
