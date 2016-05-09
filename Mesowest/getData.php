<?php
    /*
     * Grab station data from MesowestAPI and insert into the database
     *
     * Just use a simple script that will look for the most recent date value in the database
     * and get the data from that date until the present. All the data will be inserted into
     * the database.
     *
     * 20150210 Scott Havens
     */

    /*
     * PARAMETERS
     */
    
    $wy = 2016; // current water year only used to initialize db
                
    // set query parameters, add additional as needed
    date_default_timezone_set ( 'UTC' );
    $start = date ( 'YmdHi', mktime ( 0, 0, 0, 10, 1, $wy-1 ) ); // start time default to WY start
    $p ['stid'] = '';
    $p ['start'] = $start;
    $p ['end'] = date ( 'YmdHi' ); // end time, now
    $p ['obstimezone'] = 'utc'; // observation time zone
    $p ['units'] = 'metric'; // units
    $p ['vars'] = 'air_temp,dew_point_temperature,relative_humidity,wind_speed,wind_direction,wind_gust,solar_radiation,snow_smoothed,precip_accum,snow_depth,snow_accum,precip_storm,snow_interval,snow_water_equiv';   // variables
    $p ['token'] = 'e505002015c74fa6850b2fc13f70d2da'; // API token

    $url = 'http://api.mesowest.net/v2/stations/timeseries?';
    
    echo "------------------------------------------------------------------------------\n";
    echo date ( 'Y-m-d H:i' ) . "\n";

    include (dirname(dirname(__FILE__)) . '/database_connect.php');
    include ('wx_DateTime.php');


    // map function to escape null values
    function mapfun ($value) {
        
        if ($value) {
            $value = "'" .  $value . "'";
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
    $sqry = "SELECT station_id from tbl_stations WHERE source='Mesowest'";
    $stations = $conn->query ( $sqry );
//    var_dump($stations);

    
    
    // This query will return empty if there is no data in the database
    // $qry = 'SELECT r.station_id, r.date_time FROM raw_data AS r INNER JOIN stations ON r.station_id = stations.station_id' . 
    // 	' WHERE r.date_time = (SELECT MAX(t2.date_time) FROM raw_data AS t2 WHERE t2.station_id = r.station_id)';
    // $result = mysql_query ( $qry );
    // if ($result) {
    // 	$r = [ ];
    // 	while ( $row = mysql_fetch_array ( $result ) ) {
    // 		$phpdate = strtotime( $row ['date_time'] );
    // 		$r [$row ['station_id']] = date("YmdHi", $phpdate);
    // 	}
    // }

    echo "About to get data from Mesowest ...\n";

    if ($stations) {
        $idx = 0;
        while ( $row = $stations->fetch_assoc () ) {
            
            // get the station id
            $station_id = $row ['station_id'];
            $p ['stid'] = $station_id;
//     		echo $station_id . "\n";
            
            // determine the latest time in the database
            $qry = "SELECT max(date_time) + INTERVAL 1 MINUTE AS d FROM tbl_raw_data WHERE station_id='$station_id'";
            $dt = $conn->query ( $qry );
            $t = $dt->fetch_assoc();
    // 		print_r($t);
            if (!empty($t['d'])) {
                $phpdate = strtotime( $t['d']);
                $p['start'] = date ( "YmdHi", $phpdate );
            } else { 
                $p['start'] = $start; 
            }
    // 		print_r($p['start']."\n");
            
            // get the data from Mesowest
            $data = file_get_contents ( $url . http_build_query ( $p ) );
            $data = json_decode ( $data, 'true' );
            
//            var_dump($data['SUMMARY']);
            if (json_last_error() == JSON_ERROR_NONE & $data['SUMMARY']['RESPONSE_CODE'] == 1 & $data['SUMMARY']['NUMBER_OF_OBJECTS'] > 0) {
                        
                $obs = $data ['STATION'] [0] ['OBSERVATIONS']; // the actual data
                $cnt = count($obs['date_time']);
//     			echo $cnt ."\n";
                
                // variables returned
                $vars = $data ['STATION'] [0] ['SENSOR_VARIABLES'];
                $v = array();
                foreach ( $vars as $key => $value ) {
                    $k = array_keys ( $value );
                    $v [$k [0]] = $key;
                }
//     			print_r($v);
                
                /*
                 * Insert the data into the database
                 */
                $ivals = array();
                $fvals = array();
                $aval = array();
                
                // columns to insert
                $cols = array_values($v);
                $cols[] = 'station_id';
		//$fcols = $cols;
		//$fcols[] = 'av_del';

    // 			print_r($cols);
                foreach ($cols as $key => $value)
                    $aval[$value] = '';
                
//     			print_r($aval);
//                $fcols = $cols;
                $cols = '(' . implode (',', array_keys($aval) ) . ')';
                //$fcols = '(' . implode (',', array_values($fcols) ) . ')';
                
                // go through each measurement
                for ($i=0; $i<$cnt; $i++) {
                    // build an array for the current obs with array('column_name'=>'value')
                    foreach ($obs as $key => $value) {
                        
                        if ($key === 'date_time') {
                            $phpdate = strtotime( $value[$i] );
                            $aval[$v[$key]] = date("Y-m-d H:i:s", $phpdate);
                        } else {
                            if (empty($value[$i]))
                                $aval[$v[$key]] = NULL;
                            else
                                $aval[$v[$key]] = $value[$i];
                        }
                    }
                    $aval['station_id'] = $station_id;
                    
                    $values = array_map ( 'mapfun', array_values ( $aval ));
                    $ivals[] = '(' . implode ( ',', $values ) . ')';
                    
                    // add the av_del flag to tbl_level1
                    //$faval = $aval;
                    //$faval['av_del'] = 0;                    
                    //$values = array_map ( 'mapfun', array_values ( $faval ));
                    //$fvals[] = '(' . implode ( ',', $values ) . ')';
                    
                }
                
//     			print_r($cols);
//     			print_r($ivals);
                $toinsert = implode(",", $ivals);
                $sstr = $station_id . " -- " . date('Y-m-d H:i') . " -- ";
                
                $qstr = 'INSERT IGNORE INTO tbl_raw_data ' . $cols . ' VALUES ' . $toinsert;
                $ret = $conn->query($qstr);
                
                if ($conn->errno) {
                    echo $station_id . " -- " . $conn->error. "\n";
                } else {
                    $sstr = $sstr . $cnt . " updated to tbl_raw_data";
                }
                
                /*
                 * insert into tbl_level1 table
                 */
    // 			print_r($fcols);
    // 			print_r($fvals);
                //$toinsert = implode(",", $fvals);
                $qstr = 'INSERT IGNORE INTO tbl_level1 ' . $cols . ' VALUES ' . $toinsert;
                $ret = $conn->query($qstr);
                if ($conn->errno) {
                    echo $station_id . " -- " . $conn->error. "\n";
                } else {
                    $sstr .= " -- updated to tbl_level1";
                }
                
                echo $sstr . "\n";
                
            } else {
                echo 'No data for ' . $station_id . "\n";
                if (array_key_exists('SUMMARY', $data))
                    print_r($data['SUMMARY']);
            }
    // 		$idx++;
    // 		if ($idx === 5)
    // 			break;	
        }
    } else {
        echo mysql_error ();
    }

    /*
     * Remove any duplicate entries
     */
//    echo "Averaging and deleting duplicates from tbl_level1... \n";
//
//    $result = $conn->query('CALL averageDelete');
//    if ($conn->errno) {
//        $conn->error . "\n";
//    } else {
//        echo "Duplicate time values removed from tbl_level1 \n";
//    }

    // close the connection
    $conn->close();
        
?>
