import viirs
import scidbpy
import argparse
import scidb
import glob
import sys
import datetime
sys.path.insert(1, '/home/griessbaum/Dropbox/UCSB/STARE_Project/STARE_build/src/')
import pystare

db = scidbpy.db.DB(scidb_url='http://schiss.duckdns.org:8080/')
load_array = scidb.Array(name='load_array', db=db)
cldmsk = scidb.Cldmsk(db=db)
cldmsk.remove()
cldmsk.create()


def load_file(nc_path):
    nc = viirs.CLDMSK(nc_path)
    nc.read()        
    nc.add_temporal_stare()
    np = nc.to_numpy()    
    
    load_array.remove()    
    load_array.from_numpy(np)
    load_array.add_stare_spatial() 
    load_array.insert_into(cldmsk)    
    print(cldmsk.head())
    

if __name__ == '__main__':
    #parser = argparse.ArgumentParser(description='Load files to scidb array')
    #parser.add_argument('--file', metavar='file', nargs='?', type=str, help='file to load')
    #parser.add_argument('--array', metavar='array', nargs='?', type=str, help='Destination array', default='.')
    #args = parser.parse_args()        
    #nc_path = args.file    

    #nc_path = '/home/griessbaum/CLDMSK_L2_VIIRS_SNPP.A2019177.0318.001.2019177130739.nc'    
    #nc_path = '/download/viirs/cldmsk/CLDMSK_L2_VIIRS_SNPP.A2016315.2306.001.2019071184303.nc'
    
    folder = '/download/viirs/cldmsk/'        
    i = 0
    for nc_path in sorted(glob.glob(folder+'*.nc')):
        print(i, ':' + nc_path)
        load_file(nc_path)
        i += 1
        
        
        
    
    
    
    

