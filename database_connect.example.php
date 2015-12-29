<?php
$host = 'host';
$user = 'user';
$pass = 'password';
$db = 'database';
    
$conn = new mysqli($host,$user, $pass, $db);
if ($conn->connect_errno) {
	echo "Failed to connect to MySQL: (" . $conn->connect_errno . ") " . $conn->connect_error;
}
?>