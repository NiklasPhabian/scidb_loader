import viirs
import scidbpy
import argparse
import scidb


db = scidbpy.db.DB(scidb_url='http://schiss.duckdns.org:8080/')
load_array = scidb.Array(name='load_array', db=db)
cldmsk = scidb.Cldmsk(db=db)
#cldmsk.create()


def load_file(nc_path):
    nc = viirs.CLDMSK(nc_path)        
    nc.read()    
    np = nc.to_numpy()
    
    load_array.remove()
    print('loading array')
    load_array.from_numpy(np)
    print('Creating spatial index values')
    load_array.add_stare_spatial() 
    print('Creating temporal index values')
    load_array.add_stare_temporal() 
    print('Redimensioning/Inserting to target')    
    load_array.insert_into(cldmsk)    
    print(cldmsk.head())
    load_array.remove()
    


if __name__ == '__main__':
    #parser = argparse.ArgumentParser(description='Load files to scidb array')
    #parser.add_argument('--file', metavar='file', nargs='?', type=str, help='file to load')
    #parser.add_argument('--array', metavar='array', nargs='?', type=str, help='Destination array', default='.')
    #args = parser.parse_args()        
    #nc_path = args.file    
    
    #nc_path = '/download/viirs/cldmsk/CLDMSK_L2_VIIRS_SNPP.A2013011.0312.001.2019071062408.nc'
    #nc_path = '/home/griessbaum/CLDMSK_L2_VIIRS_SNPP.A2019177.0318.001.2019177130739.nc'
        
    folder = '/download/viirs/cldmsk/'
    
    
    for nc_path in glob.glob(folder+'/.nc'):
        load_file(nc_path)
        
        
    
    
    
    

    
    
    

    
    

