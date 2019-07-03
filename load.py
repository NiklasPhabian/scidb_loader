import viirs
import subprocess

nc = viirs.CLDMSK('CLDMSK_L2_VIIRS_SNPP.A2015136.0100.001.2019071125009.nc')
nc.read()
df = nc.to_df()

tsv_name = 'poop.tsv'
df.to_csv(path_or_buf=tsv_name , sep='\t')

iquery = "store(aio_input('tsv_name', num_attributes:3, tmp)"

subprocess.check_output(['iquery', '-anq', iquery])

