'''
20151228 Scott Havens

Get the variables available for the stations from Mesowest
'''


import urllib, json, os
import progressbar

# if the flag is set, it will load the json from the website
# else it will just read it from the file
flag = 0


# connect to db, returns connection cnx
execfile(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'database_connect.py'))
cursor = cnx.cursor()

if flag == 1:
    
    print 'Loading Mesowest metadata from the web...'
    
    # set query parameters, add additional as needed
    p = {}
    p['sensorvars'] = '1'
    p['token'] = 'e505002015c74fa6850b2fc13f70d2da'
    
    url = 'http://api.mesowest.net/v2/stations/metadata?'
    
    arg = urllib.urlencode(p, doseq=True)
    
    f = urllib.urlopen(url + arg)
    data = json.loads(f.read())

else:
    print 'Loading Mesowest metadata from a file...'
    
    jfile = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data/mesowest_metadata.json')
    
    with open(jfile) as json_file:
        data = json.load(json_file)
        
        
# go through each station
pbar = progressbar.ProgressBar(max_value=len(data['STATION'])).start()
j = 0
for d in data['STATION']:
    sta = d['STID']
    
    # determine the variables
    v = d['SENSOR_VARIABLES']
    
    if not not v:
        vname = ', '.join(d['SENSOR_VARIABLES'].keys())
        
#        qry = "INSERT INTO tbl_metadata (primary_id, variables) VALUES ('%s','%s') ON DUPLICATE KEY UPDATE variables='%s'" % (sta, vname, vname);
        qry = "UPDATE tbl_metadata SET variables='%s' WHERE primary_id='%s'" % (vname, sta)
            
        cursor.execute(qry)
      
    j += 1
    pbar.update(j)
    
pbar.finish()  
cnx.close()