"""
Download a whole bunch from Mesowest
"""

from download_Mesowest_historic import downloadData
from datetime import datetime, timedelta

start_date = datetime(2011,10,1)
end_date = datetime(2015,10,1)
tstep = 7
bbox='-116.4,43.05,-114.45,44.44'

#------------------------------------------------------------------------------ 
increment = timedelta(days=tstep)

dates = []
nxt = start_date
while nxt <= end_date:
    dates.append(nxt)
    nxt += increment
# return np.array(result)

N = len(dates)

for i in range(1,N):
    
    print '##############################################'
    print '%i of %i' % (i-1, N-1)
    print '%s tp %s' % (dates[i-1], dates[i])
    
    downloadData(dates[i-1], dates[i], bbox)