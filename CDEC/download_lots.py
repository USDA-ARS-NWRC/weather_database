"""
Download a whole bunch from Mesowest
"""

from download_CDEC_bbox import downloadData
from datetime import datetime, timedelta

start_date = datetime(2000,12,28)
end_date = datetime(2012,10,1)
tstep = 7
# bbox='-116.4,43.05,-114.45,44.44' # BRB
bbox='-119.98,37.7,-119.19,38.35' # TUOL

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
