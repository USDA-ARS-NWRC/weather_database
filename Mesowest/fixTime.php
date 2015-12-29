<?php
/*
 * Round the times from raw_data to the nearest hour
 *
 * 20150212 Scott Havens
 */

/*
 * PARAMETERS
 */
$wy = 2015; // current water year only used to initialize db

// set query parameters, add additional as needed
date_default_timezone_set ( 'UTC' );
$start = date ( 'Y-m-d H:i', mktime ( 0, 0, 0, 10, 1, $wy - 1 ) ); // start time default to WY start
// $start = date ( 'Y-m-d H:i', mktime ( 0, 0, 0, 2, 10, $wy ) ); // start time default to WY start
$endDate = date ( 'Y-m-d H:i' );	// end date now
                                                             


echo "------------------------------------------------------------------------------\n";
echo date ( 'Y-m-d H:i' ) . "\n";

include ('database_connect.php');
include ('wx_DateTime.php');

// map function to escape null values
function mapfun($value) {
	if ($value) {
		$value = "'" . $value . "'";
	} else {
		$value = "NULL"; // in the SQL query "NULL" will NOT be quoted
	}
	return $value;
}

/*
 * Load the stations from the database
 */

// echo "Seeing if there is recent data ...\n";

// Get all the stations
$sqry = 'SELECT station_id from stations';
$stations = $conn->query ( $sqry );
// print_r($stations->fetch);

echo "About to get data from raw_data ...\n";

if ($stations) {
	$idx = 0;
	while ( $row = $stations->fetch_assoc () ) {
		
		// get the station id
		$station_id = $row ['station_id'];
// 		echo $station_id . "\n";
		
		// determine the latest time in the database
		$qry = "SELECT max(date_time) AS d FROM fixed WHERE station_id='$station_id'";
		$dt = $conn->query ( $qry );
		$t = $dt->fetch_assoc ();
		// print_r($t);
		if (! empty ( $t ['d'] )) {
			$phpdate = strtotime ( $t ['d'] );
			$startDate = date ( "Y-m-d H:i", $phpdate );
		} else {
			$startDate = $start;
		}
		$dt->free();
		
		// get the data from raw_data
		// determine the latest time in the database
		$qry = "SELECT * FROM raw_data WHERE station_id='$station_id' AND date_time BETWEEN '$startDate' AND '$endDate'";
// 		echo $qry;
		$dt = $conn->query ( $qry );
// 		print_r($dt);
			
		// get the column names
		$finfo = $dt->fetch_fields ();
		$col = array();
		foreach ( $finfo as $val ) {
			$col[] = $val->name;
		}
		$col[] = 'user';
		$col[] = 'date_fixed';
// 		print_r($col);
		
		// get the data in the same order as the columns and add NULL values
		$ivals = array ();
		while ( $r = $dt->fetch_assoc ()) {
			
			// get the date time and round
			$wtm = new wx_DateTime( $r['date_time']);
			$r['date_time'] = $wtm->round();
			
			// add the user and the time changed
			$r['user'] = $user;
			$r['date_fixed'] = date ( 'Y-m-d H:i:s' );
			
			// get all the data and reinsert into table 'fixed'
			// build an array for the current obs with array(index=>'value') that matches col
			$aval = array ();
			foreach ( $col as $key => $v ) {
				
				if (empty ( $r [$v] ))
					$aval [$key] = NULL;
				else
					$aval [$key] = $r [$v];
			}
						
			$values = array_map ( 'mapfun', array_values ( $aval ) );	// add the NULL
			$ivals [] = '(' . implode ( ',', $values ) . ')';			// create a string for inserting
			
		}
		
		$dt->free();
		
		// prep for mass insertion
		$cnt = count($ivals);
		$toinsert = implode ( ",", $ivals );
		$col = '(' . implode ( ',', array_values ( $col ) ) . ')';
		
// 		print_r($toinsert);
		// insertion!
		$qstr = 'INSERT IGNORE INTO fixed ' . $col . ' VALUES ' . $toinsert;
// 		echo $qstr;
		$ret = $conn->query ( $qstr );
		
		if ($conn->errno) {
			echo $station_id . " -- " . $conn->error . "\n";
		} else {
			echo $station_id . " -- " . date ( 'Y-m-d H:i' ) . " -- " . $cnt . " updated \n";
		}
// 		print_r($ivals);
// 		break;
		
	}
} else {
	echo mysql_error ();
}

// close the connection
$conn->close;

?>
