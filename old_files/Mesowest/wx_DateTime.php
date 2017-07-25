<?php
/**
 * This extends the DateTime object to round the date to the 
 * nearest hour.  Using DateTime ensures that all the leap
 * years, and other odd things are taken care of
 * 
 * $wt = new wx_DateTime('2016-02-29 23:59:00');
 * $wt->round()
 */
class wx_DateTime extends DateTime {

    /**
     * Return Date in MySQL format
     *
     * @return String
     */
    public function __toString() {
        return $this->format('Y-m-d H:i:s');
    }

    /**
     * Return rounded time value
     *
     * @return string
     */
    public function round() {
    	
    	$minute = $this->format('i');	// minute
    	$hour = $this->format('H');	// hour
    	
    	if ($minute >= 30)
    		$hour++;
    	$minute = 0;
    	
    	$this->setTime($hour, $minute);
    	
        return $this->__toString();
    }

}

?>
