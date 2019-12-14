import viirs
import scidbpy
import argparse
import scidb
import glob
import sys
import eta
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
    print('loading array')
    load_array.from_numpy(np)
    print('Creating spatial index values')
    load_array.add_stare_spatial() 
    print('Creating temporal index values')
    #load_array.add_stare_temporal() 
    print('Redimensioning/Inserting to target')    
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
    #load_file(nc_path)
      
    
    folder = '/home/griessbaum/'   
    files = sorted(glob.glob(folder+'*.nc'))
    eta = eta.ETA(n_tot=len(files))
    for nc_path in files:
        eta.display(step='{name}'.format(name=nc_path.split('/')[-1]))
        load_file(nc_path)
        


    

