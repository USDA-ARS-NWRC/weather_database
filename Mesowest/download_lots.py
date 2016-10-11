"""
Download a whole bunch from Mesowest
"""

from download_Mesowest_historic import downloadData
from datetime import datetime, timedelta

start = datetime.now()

start_date = datetime(2001,10,1)
end_date = datetime(2012,10,1)
tstep = 7
bbox='-116.4,43.05,-114.45,44.44'     # BRB
bbox='-119.98,37.7,-119.19,38.35' # TUOL
# bbox='-116.241,43.56,-116.240,43.57' #KBOI
# bbox='-116.177,43.60,-116.176,43.601' #BOII

#------------------------------------------------------------------------------ 
increment = timedelta(days=tstep)

dates = []
nxt = start_date
while nxt <= end_date:
    dates.append(nxt)
    nxt += increment
dates.append(end_date)
# return np.array(result)

N = len(dates)

for i in range(1,N):
    
    print '##############################################'
    print '%i of %i' % (i-1, N-1)
    print '%s to %s' % (dates[i-1], dates[i])
    
    downloadData(dates[i-1], dates[i], bbox)

print datetime.now() - start
