

import numpy as np
import copy
from scipy.fftpack import fft
from scipy import signal
import pandas as pd

def auto_cloudfactor(solardata,window,scutoff,peakpm,peakfactor):
    """
    Args:
        solardata:       DataFrame of solar data 
        window:          [hours] number of hours to consider when calculating 'clear sky' integrated 
        scutoff:         low limit for daily peak, below which measurements are not calculated
        peakpm:          [hours] number of hours +- a peak to use for calculating daily total
        peakfactor:      this is a bit of a knob - factor for multiplying the integrated daily solar radiation
        
    Returns:
        cloud_factor:    DataFrame of auto-generated cloud factor
        
    Suggested use:
        auto_cloudfactor(solardata,24*10,50,1.05)

    """

    # Initialize 
    val             = copy.deepcopy(solardata.values.squeeze())
    cloud_factor    = copy.deepcopy(solardata)*np.nan  
    hrs             = np.array(range(0,(len(val))-24))
    iday            = 0
    
    # Calculate for every hour
    for h in hrs:
        try:
            # Get the section within specified window
            dx          = np.abs(h - hrs)
            idx         = dx < window
            section     = copy.deepcopy(val[np.where(idx)])
            
            # Calculate theoretical total from getting area underneath max days
            ipkall      = signal.find_peaks_cwt(section,np.arange(1, 10))
            
            # Get the two highest and sort descending
            ipks        = np.argsort(-section[ipkall])[0:2]
            
            # Only do the rest if there is a reasonable peak within the section
            if any(section[ipkall[ipks]] > scutoff):
        
                # This is the theoretical max for the section
                daily_max = np.array((0,0))
                for num,p in enumerate(ipks):
                    daily_max[num] = np.nansum(section[(ipkall[ipks[num]]-peakpm):(ipkall[ipks[num]]+peakpm)])
                 
                # Now compare to what we actually have for that hour
                sol     = np.nansum(val[iday:(iday + 24)])
                factor  = (sol/(np.nanmean(daily_max)))*peakfactor
                 
                # Also, only record if the daily max is over some limit  
                if sol > scutoff:    
                    if h == iday + 24:
                        cloud_factor[iday:(iday + 24)] = copy.copy(factor)
                        iday = iday + 24
                else:
                    if h == iday + 24:
                        cloud_factor[iday:(iday + 24)] = np.nan
                        iday = iday + 24
        
            else:
                if h == iday + 24:
                    cloud_factor[iday:(iday + 24)] = np.nan
                    iday = iday + 24

        
        except:
            if h == iday + 24:
                cloud_factor[iday:(iday + 24)] = np.nan
                iday = iday + 24
       
    # Set max to 1
    ih                  = cloud_factor > 1
    cloud_factor[ih]    = 1
    cloud_factor        = pd.DataFrame(cloud_factor,index = solardata.index)
    
    return cloud_factor

