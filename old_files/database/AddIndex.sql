/*
* Add index to data and current for STATION, date
*/ 

ALTER TABLE tbl_raw_data ADD INDEX station_id (station_id);
SELECT ROW_COUNT();
ALTER TABLE tbl_raw_data ADD INDEX date_time (date_time);
SELECT ROW_COUNT();

ALTER TABLE tbl_level1 ADD INDEX station_id (station_id);
SELECT ROW_COUNT();
ALTER TABLE tbl_level1 ADD INDEX date_time (date_time);
SELECT ROW_COUNT();
ALTER TABLE tbl_level1 ADD INDEX av_del (av_del);
SELECT ROW_COUNT();

ALTER TABLE tbl_level2 ADD INDEX station_id (station_id);
SELECT ROW_COUNT();
ALTER TABLE tbl_level2 ADD INDEX date_time (date_time);
SELECT ROW_COUNT();

