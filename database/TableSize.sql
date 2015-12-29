/*
20141207 Scott Havens

Get the size of the database tables
*/

SELECT table_name AS "Tables", 
round(((data_length + index_length) / 1024 / 1024), 2) "Size in MB" 
FROM information_schema.TABLES 
WHERE table_schema = "weather_v2"
ORDER BY (data_length + index_length) DESC;

-- return the size of current
-- SELECT
-- round(((data_length + index_length) / 1024 / 1024), 2) AS "Size in MB" 
-- FROM information_schema.TABLES 
-- WHERE table_schema = "weather_v2"
--  AND table_name = "tbl_level1";