import viirs
import subprocess
import argparse
import os


def nc2tsv(nv_file, tsv_file):
    nc = viirs.CLDMSK(nv_file)
    nc.read()
    df = nc.to_df()    
    df.to_csv(path_or_buf=tsv_file, sep='\t')
    
    
def tsv2array(tsv_file, array):
    iquery = "store(aio_input('{tsv_file}', num_attributes:3), {array})"
    iquery = "store(apply("\
	       "aio_input('{tsv_file}', num_attributes:5),"\
               "lat, dcast(a3, float(null)), " \
               "lon, dcast(a4, float(null)), " \
               "csc, dcast(a2, float(null))  " \
               "), {array});"
    iquery = iquery.format(tsv_file=tsv_file, array=array)
    print(iquery)
    subprocess.run(['iquery', '-anq', iquery])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Load files to scidb array')
    parser.add_argument('--file', metavar='file', nargs='?', type=str, help='file to load')
    parser.add_argument('--array', metavar='array', nargs='?', type=str, help='Destination array', default='.')
    args = parser.parse_args()    
    
    nc_file = args.file    
    tsv_file = '.'.join(nc_file.split('.')[:-1]) + '.tsv'
    
    #nc2tsv(nv_file=args.file, tsv_file=tsv_file)
    tsv2array(tsv_file=tsv_file, array=args.array)
    #ios.remove(tsv_file)    
