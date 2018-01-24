
import numpy as np
import pandas as pd
from scipy import signal
import scipy.signal as signal
import scipy
from matplotlib import pyplot as plt
import copy
from scipy.fftpack import fft
import seaborn as sns
import numpy as np
import copy


def autocleanfft(raw,ninterp,scutoff,fcutoff,window,nfdays):
    """
    Args:
        raw:     DataFrame of raw data 
        ninterp: number of values to interpolate 
        scutoff: low limit for SNR at daily frequency, below which measurements are considered 'bad'
        fcutoff: frequency cutoff 
        window:  [hours] saves this many measurement at a time 
        nfdays:  [days] number of days to load in to calculate frequency content (tested 3-4)
        
    Returns:
        auto:     DataFrame of cleaned data
        
    Suggested use for
        air temp:              auto_airtemp(raw,12,0.8,0.5,24,3)
        relative humidity:     auto_airtemp(raw,12,1.8,0.5,24,3)
        solar radiation:       auto_airtemp(raw,12,15,0.5,24,3)
        wind speed:            auto_airtemp(raw,3,0.002,0.4,24,3)
        wind direction:        auto_airtemp(raw,6,0.00005,0.8,24,2) 

    """
    
    # Interpolate
    raw         = raw.interpolate(limit=ninterp)
    val         = copy.deepcopy(raw.values.squeeze())
    
    # Initialize some things
    auto        = val.copy()*np.nan  
    Fs          = 1.0/(60.0*60.0)   # sample frequency
    nf          = 1                 # filter order
    nhrs        = len(val)
    
    # Find the number of 'extra' hours, and add them to the first window
    rmdr        = nhrs%(window) 
    inds        = 0 + rmdr
    inde        = inds + window*nfdays 
    
    while inde <= nhrs:
        
        # This breaks the loop when we're done
        if inde == nhrs:
            nhrs = 0
        
        # Get only window values
        temp    = val[inds:inde]
        n       = len(temp)
        k       = np.arange(n)
        T       = n/Fs
        frq     = k/T               # two sides frequency range
        frq     = frq[range(n/2)]   # one side frequency range
        y       = copy.copy(temp)
        Y       = fft(y)/n
        Y       = Y[range(n/2)]
        
        # Apply a low-pass filter
        B, A    = signal.butter(nf, fcutoff, output='ba')
        tempf   = signal.filtfilt(B,A,temp) 
        
        # Hard coding values for Y to use for scutoff is not good...
        # Save only 0:window values
        if max(abs(Y[1:8])) > scutoff: 
            
            # If it's the first time through the loop, save everything
            if inde == (0 + rmdr + window*nfdays):
                auto[(0 + rmdr):inde]       = tempf[:]
                
            # Otherwise, consider the full window, but only save window size
            else:
                auto[(inde-window):inde]   = tempf[(len(tempf)-window):len(tempf)]

        # Move window along 
        if (inde + window) < nhrs:
            inds    = inds + window
            inde    = inde + window
         
        # This is the end of the window    
        elif (inde + window) == nhrs:
            inds    = inds + window
            inde    = nhrs
    
    auto        = pd.DataFrame(auto,index = raw.index)
                    
    return auto                    
