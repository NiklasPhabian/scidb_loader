import viirs
import scidbpy
import argparse
import scidb
import glob
import sys
import eta
#import subprocess
#import psutil
#import time


db = scidbpy.db.DB(scidb_url='http://schiss.duckdns.org:8080/')
#db = scidbpy.db.DB(scidb_url='http://127.0.0.1:8080/')

load_array = scidb.Array(name='load_array', db=db)
cldmsk = scidb.Cldmsk(db=db)
#cldmsk.remove()
cldmsk.create()


def load_file(nc_path):
    nc = viirs.CLDMSK(nc_path)
    nc.read()        
    nc.add_temporal_stare()
    np = nc.to_numpy()    
        
    
    load_array.remove()    
    load_array.from_numpy(np)     
    #load_array.add_stare_temporal() 
    load_array.add_stare_spatial(resolution=27) 
    load_array.insert_into(cldmsk)    
    print(cldmsk.head())
    

if __name__ == '__main__':
    #parser = argparse.ArgumentParser(description='Load files to scidb array')
    #parser.add_argument('--file', metavar='file', nargs='?', type=str, help='file to load')
    #parser.add_argument('--array', metavar='array', nargs='?', type=str, help='Destination array', default='.')
    #args = parser.parse_args()        
    #nc_path = args.file    

    #nc_path = '/home/griessbaum/CLDMSK_L2_VIIRS_SNPP.A2012062.0130.001.2019070194211.nc'    
    #nc_path = '/download/viirs/cldmsk/CLDMSK_L2_VIIRS_SNPP.A2016315.2306.001.2019071184303.nc'
    #load_file(nc_path)
    
    folder = '/download/viirs/cldmsk/'   
    files = sorted(glob.glob(folder+'*.nc'))
    n_start = 35832+714+714
    eta = eta.ETA(n_tot=len(files)-n_start)
    for nc_path in files[n_start:]:
        eta.display(step='{name}'.format(name=nc_path.split('/')[-1]))
        load_file(nc_path)
#        if psutil.virtual_memory().percent > 90:
#            subprocess.call(['scidbctl.py', '-c', '/opt/scidb/19.3/etc/config.ini', 'stop', 'earthdb'])
#            time.sleep(10)
#            subprocess.call(['scidbctl.py', '-c', '/opt/scidb/19.3/etc/config.ini', 'start', 'earthdb'])
#            time.sleep(10)


        


   
