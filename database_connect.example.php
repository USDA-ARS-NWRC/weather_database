<?php
$host = 'localhost';
$user = 'wxuser_v2';
$pass = 'x340hm4h980r';
$db = 'weather_v2';

$host = '10.200.28.203';
    
$conn = new mysqli($host,$user, $pass, $db);
if ($conn->connect_errno) {
	echo "Failed to connect to MySQL: (" . $conn->connect_errno . ") " . $conn->connect_error;
}
?>