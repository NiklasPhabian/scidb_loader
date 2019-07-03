import viirs
import subprocess
import argparse


def nc2tsv(nv_file, tsv_file):
    nc = viirs.CLDMSK(nv_file)
    nc.read()
    df = nc.to_df()    
    df.to_csv(path_or_buf=tsv_file, sep='\t')
    
    
def tsv2array(tsv_file, array):
    iquery = "store(aio_input('{tsv_file}', num_attributes:3, '{array}')".format(tsv_file=tsv_file, array=array)
    print(iquery)
    subprocess.check_output(['iquery', '-anq', iquery])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Load files to scidb array')
    parser.add_argument('--file', metavar='file', nargs='?', type=str, help='file to load')
    parser.add_argument('--array', metavar='array', nargs='?', type=str, help='Destination array', default='.')
    args = parser.parse_args()    
    
    #nc2tsv(nv_file=args.file, tsv_file='tmp.tsv')
    tsv2array(tsv_file='tmp.tsv', array=args.array)
    
