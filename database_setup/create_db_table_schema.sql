-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL,ALLOW_INVALID_DATES';

-- -----------------------------------------------------
-- Schema weather_db
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema weather_db
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `weather_db` DEFAULT CHARACTER SET utf8 ;
USE `weather_db` ;

-- -----------------------------------------------------
-- Table `weather_db`.`tbl_metadata`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `weather_db`.`tbl_metadata` ;

CREATE TABLE IF NOT EXISTS `weather_db`.`tbl_metadata` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `primary_id` VARCHAR(10) NOT NULL,
  `station_name` VARCHAR(256) NULL,
  `latitude` DECIMAL(15,11) NULL,
  `longitude` DECIMAL(15,11) NULL,
  `elevation` DECIMAL(10,3) NULL,
  `state` VARCHAR(5) NULL,
  `timezone` VARCHAR(45) NULL,
  `source` VARCHAR(45) NOT NULL,
  `primary_provider` VARCHAR(256) NULL,
  `network` VARCHAR(45) NULL,
  `reported_lat` DECIMAL(15,11) NULL,
  `reported_long` DECIMAL(15,11) NULL,
  `utm_x` DECIMAL(10,3) NULL,
  `utm_y` DECIMAL(10,3) NULL,
  `utm_zone` VARCHAR(15) NULL,
  PRIMARY KEY (`id`),
  INDEX `idx_primary_id` (`primary_id` ASC),
  UNIQUE INDEX `primary_id_UNIQUE` (`primary_id` ASC))
ENGINE = InnoDB
PACK_KEYS = Default
ROW_FORMAT = Default;


-- -----------------------------------------------------
-- Table `weather_db`.`tbl_variables`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `weather_db`.`tbl_variables` ;

CREATE TABLE IF NOT EXISTS `weather_db`.`tbl_variables` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `metadata_id` INT NOT NULL,
  `variables` VARCHAR(256) NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_metadata_id_idx` (`metadata_id` ASC),
  CONSTRAINT `fk_variable_metadata_id`
    FOREIGN KEY (`metadata_id`)
    REFERENCES `weather_db`.`tbl_metadata` (`id`)
    ON DELETE CASCADE
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `weather_db`.`tbl_station_photos`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `weather_db`.`tbl_station_photos` ;

CREATE TABLE IF NOT EXISTS `weather_db`.`tbl_station_photos` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `metadata_id` INT NOT NULL,
  `img_path` VARCHAR(1024) NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_metadata_id_idx` (`metadata_id` ASC),
  CONSTRAINT `fk_photos_metadata_id`
    FOREIGN KEY (`metadata_id`)
    REFERENCES `weather_db`.`tbl_metadata` (`id`)
    ON DELETE CASCADE
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `weather_db`.`tbl_stations`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `weather_db`.`tbl_stations` ;

CREATE TABLE IF NOT EXISTS `weather_db`.`tbl_stations` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `client` VARCHAR(15) NOT NULL,
  `metadata_id` INT NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_station_metadata_id_idx` (`metadata_id` ASC),
  INDEX `client_INDEX` (`client` ASC),
  CONSTRAINT `fk_station_metadata_id`
    FOREIGN KEY (`metadata_id`)
    REFERENCES `weather_db`.`tbl_metadata` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `weather_db`.`tbl_level0`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `weather_db`.`tbl_level0` ;

CREATE TABLE IF NOT EXISTS `weather_db`.`tbl_level0` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `station_id` VARCHAR(10) NOT NULL,
  `date_time` DATETIME NOT NULL,
  `air_temp` DECIMAL(10,3) NULL,
  `dew_point_temperature` DECIMAL(10,3) NULL,
  `relative_humidity` DECIMAL(10,3) NULL,
  `wind_speed` DECIMAL(10,3) NULL,
  `wind_direction` DECIMAL(10,3) NULL,
  `wind_gust` DECIMAL(10,3) NULL,
  `solar_radiation` DECIMAL(10,3) NULL,
  `snow_smoothed` DECIMAL(10,3) NULL,
  `precip_accum` DECIMAL(10,3) NULL,
  `snow_depth` DECIMAL(10,3) NULL,
  `snow_interval` DECIMAL(10,3) NULL,
  `snow_water_equiv` DECIMAL(10,3) NULL,
  `vapor_pressure` DECIMAL(10,3) NULL,
  `cloud_factor` DECIMAL(10,3) NULL,
  PRIMARY KEY (`id`),
  INDEX `idx_station_id` (`station_id` ASC),
  INDEX `idx_date_time` (`date_time` ASC),
  UNIQUE INDEX `station_date_UNIQUE` (`station_id` ASC, `date_time` ASC),
  CONSTRAINT `fk_level0_station_id`
    FOREIGN KEY (`station_id`)
    REFERENCES `weather_db`.`tbl_metadata` (`primary_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `weather_db`.`tbl_level1`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `weather_db`.`tbl_level1` ;

CREATE TABLE IF NOT EXISTS `weather_db`.`tbl_level1` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `station_id` VARCHAR(10) NOT NULL,
  `date_time` DATETIME NOT NULL,
  `air_temp` DECIMAL(10,3) NULL,
  `dew_point_temperature` DECIMAL(10,3) NULL,
  `relative_humidity` DECIMAL(10,3) NULL,
  `wind_speed` DECIMAL(10,3) NULL,
  `wind_direction` DECIMAL(10,3) NULL,
  `wind_gust` DECIMAL(10,3) NULL,
  `solar_radiation` DECIMAL(10,3) NULL,
  `snow_smoothed` DECIMAL(10,3) NULL,
  `precip_accum` DECIMAL(10,3) NULL,
  `precip_intensity` DECIMAL(10,3) NULL,
  `snow_depth` DECIMAL(10,3) NULL,
  `snow_interval` DECIMAL(10,3) NULL,
  `snow_water_equiv` DECIMAL(10,3) NULL,
  `vapor_pressure` DECIMAL(10,3) NULL,
  `cloud_factor` DECIMAL(10,3) NULL,
  `qc_flag` VARCHAR(100) NULL,
  PRIMARY KEY (`id`),
  INDEX `idx_station_id` (`station_id` ASC),
  INDEX `idx_date_time` (`date_time` ASC),
  UNIQUE INDEX `station_date_UNIQUE` (`station_id` ASC, `date_time` ASC),
  CONSTRAINT `fk_level1_station_id`
    FOREIGN KEY (`station_id`)
    REFERENCES `weather_db`.`tbl_metadata` (`primary_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `weather_db`.`tbl_level2`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `weather_db`.`tbl_level2` ;

CREATE TABLE IF NOT EXISTS `weather_db`.`tbl_level2` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `station_id` VARCHAR(10) NOT NULL,
  `date_time` DATETIME NOT NULL,
  `air_temp` DECIMAL(10,3) NULL,
  `dew_point_temperature` DECIMAL(10,3) NULL,
  `relative_humidity` DECIMAL(10,3) NULL,
  `wind_speed` DECIMAL(10,3) NULL,
  `wind_direction` DECIMAL(10,3) NULL,
  `wind_gust` DECIMAL(10,3) NULL,
  `solar_radiation` DECIMAL(10,3) NULL,
  `snow_smoothed` DECIMAL(10,3) NULL,
  `precip_accum` DECIMAL(10,3) NULL,
  `precip_intensity` DECIMAL(10,3) NULL,
  `snow_depth` DECIMAL(10,3) NULL,
  `snow_interval` DECIMAL(10,3) NULL,
  `snow_water_equiv` DECIMAL(10,3) NULL,
  `vapor_pressure` DECIMAL(10,3) NULL,
  `cloud_factor` DECIMAL(10,3) NULL,
  PRIMARY KEY (`id`),
  INDEX `idx_station_id` (`station_id` ASC),
  INDEX `idx_date_time` (`date_time` ASC),
  UNIQUE INDEX `station_date_UNIQUE` (`station_id` ASC, `date_time` ASC),
  CONSTRAINT `fk_level2_station_id`
    FOREIGN KEY (`station_id`)
    REFERENCES `weather_db`.`tbl_metadata` (`primary_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `weather_db`.`tbl_level2_audit`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `weather_db`.`tbl_level2_audit` ;

CREATE TABLE IF NOT EXISTS `weather_db`.`tbl_level2_audit` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `action` VARCHAR(15) NOT NULL,
  `user` VARCHAR(45) NOT NULL,
  `timestamp` DATETIME NOT NULL,
  `row_id` INT NOT NULL,
  `variable` VARCHAR(45) NOT NULL,
  `value` DECIMAL(10,3) NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_row_id_idx` (`row_id` ASC),
  CONSTRAINT `fk_level2_row_id`
    FOREIGN KEY (`row_id`)
    REFERENCES `weather_db`.`tbl_level2` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `weather_db`.`tbl_level_auto`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `weather_db`.`tbl_level_auto` ;

CREATE TABLE IF NOT EXISTS `weather_db`.`tbl_level_auto` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `station_id` VARCHAR(10) NOT NULL,
  `date_time` DATETIME NOT NULL,
  `air_temp` DECIMAL(10,3) NULL,
  `dew_point_temperature` DECIMAL(10,3) NULL,
  `relative_humidity` DECIMAL(10,3) NULL,
  `wind_speed` DECIMAL(10,3) NULL,
  `wind_direction` DECIMAL(10,3) NULL,
  `wind_gust` DECIMAL(10,3) NULL,
  `solar_radiation` DECIMAL(10,3) NULL,
  `snow_smoothed` DECIMAL(10,3) NULL,
  `precip_accum` DECIMAL(10,3) NULL,
  `precip_intensity` DECIMAL(10,3) NULL,
  `snow_depth` DECIMAL(10,3) NULL,
  `snow_interval` DECIMAL(10,3) NULL,
  `snow_water_equiv` DECIMAL(10,3) NULL,
  `vapor_pressure` DECIMAL(10,3) NULL,
  `cloud_factor` DECIMAL(10,3) NULL,
  PRIMARY KEY (`id`),
  INDEX `idx_station_id` (`station_id` ASC),
  INDEX `idx_date_time` (`date_time` ASC),
  UNIQUE INDEX `station_date_UNIQUE` (`station_id` ASC, `date_time` ASC),
  CONSTRAINT `fk_auto_station_id`
    FOREIGN KEY (`station_id`)
    REFERENCES `weather_db`.`tbl_metadata` (`primary_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;

USE `weather_db` ;

-- -----------------------------------------------------
-- Placeholder table for view `weather_db`.`tbl_stations_view`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `weather_db`.`tbl_stations_view` (`id` INT, `client` INT, `metadata_id` INT, `primary_id` INT, `source` INT);

-- -----------------------------------------------------
-- View `weather_db`.`tbl_stations_view`
-- -----------------------------------------------------
DROP VIEW IF EXISTS `weather_db`.`tbl_stations_view` ;
DROP TABLE IF EXISTS `weather_db`.`tbl_stations_view`;
USE `weather_db`;
CREATE  OR REPLACE VIEW `tbl_stations_view` AS
    SELECT 
        s.*, m.primary_id, m.source
    FROM
        tbl_stations s
            INNER JOIN
        tbl_metadata m ON s.metadata_id = m.id;

SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
