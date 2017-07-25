/*
20141205 Scott Havens

Create the metadata table from mesowest_csv.tbl
*/

CREATE TABLE IF NOT EXISTS tbl_metadata (
	primary_id VARCHAR(100),
    secondary_id VARCHAR(10000),
    station_name VARCHAR(256),
    state VARCHAR(10),
    country	VARCHAR(10),
    latitude DECIMAL(15 , 11),
    longitude DECIMAL(15 , 11),
    elevation DECIMAL(10 , 3),
    mesowest_network_id	INT,
    network_name VARCHAR(256),
    status	VARCHAR(256),
    primary_provider_id	INT,
    primary_provider VARCHAR(256),
    secondary_provider_id INT,
    secondary_provider VARCHAR(256),
    tertiary_provider_id INT,
    tertiary_provider VARCHAR(256),
    wims_id VARCHAR(256),
	X DECIMAL(15 , 3),
    Y DECIMAL(15 , 3),
	source VARCHAR(20),
	variables VARCHAR(10000),
	PRIMARY KEY (primary_id, source)
    );
