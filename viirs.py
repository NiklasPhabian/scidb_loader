import netCDF4
import datetime
import numpy
import rasterio
import pytz
import pandas
import glob
import shapely
import geopandas

utc = pytz.timezone('UTC')


class VIIRSNC:
    def __init__(self, file_name, data_group, data_name):
        self.file_name = file_name
        self.file_name_geo = file_name
        self.data_name = data_name
        self.data_group = data_group        
        self.time_stamp = None
        self.data = None

    def make_mask(self, bbox=None, n_rows=None, n_cols=None):
        if bbox is not None:
            mask = numpy.zeros(self.lats.shape) == 0
            mask *= self.lats>bbox.lat_min
            mask *= self.lats<bbox.lat_max
            mask *= self.lons>bbox.lon_min
            mask *= self.lons<bbox.lon_max
        elif n_rows is not None:
            mask = numpy.zeros(self.lats.shape) == 1
            mask[:, 0:n_rows] = True
        elif n_cols is not None:
            mask = numpy.zeros(self.lats.shape) == 1
            mask[0:n_cols, :] = True
        else:
            mask = numpy.zeros(self.lats.shape) == 0
        return mask

    def read(self):
        self.read_data()
        self.read_geo()

    def read_data(self):
        netcdf = netCDF4.Dataset(self.file_name, 'r', format='NETCDF4')
        self.data = netcdf[self.data_group][self.data_name][:].data

    def read_geo(self):
        geo_netcdf = netCDF4.Dataset(self.file_name_geo, 'r', format='NETCDF4')
        self.lats = geo_netcdf['geolocation_data']['latitude'][:]
        self.lons = geo_netcdf['geolocation_data']['longitude'][:] 

    def read_timestamp(self):
        data_netcdf = netCDF4.Dataset(self.file_name, 'r', format='NETCDF4')
        timestamp_string = data_netcdf.time_coverage_start
        self.time_stamp =  datetime.datetime.strptime(timestamp_string, '%Y-%m-%dT%H:%M:%S.%fZ')
        self.time_stamp = utc.localize(self.time_stamp)
        return self.time_stamp

    def to_df(self, bbox=None, n_rows=None, n_cols=None):
        mask = self.make_mask(bbox, n_rows, n_cols)
        data_dict = {}
        data_dict[self.data_name] = self.data[mask]
        data_dict['time_stamp'] = self.time_stamp
        data_dict['lat'] = self.lats[mask]
        data_dict['lon'] = self.lons[mask]
        df = pandas.DataFrame(data_dict)
        return df

    def to_gpkg(self, file_name, bbox=None, n_rows=None, n_cols=None):
        df = self.to_df(bbox, bbox=bbox, n_rows=n_rows, n_cols=n_cols)
        df['geometry'] = df.apply(lambda x: shapely.geometry.Point((float(x.lon), float(x.lat))), axis=1)
        df['time_stamp'] = df['time_stamp'].astype(str)
        df = geopandas.GeoDataFrame(df, geometry='geometry')
        df.to_file(file_name, driver="GPKG")

    def to_tiff(self, tiff_name, bbox):
        mask = self.make_mask(bbox)
        clipped = mask * self.data
        clipped = clipped[~numpy.all(clipped==0, axis=1)]
        clipped = clipped.transpose()
        clipped = clipped[~numpy.all(clipped==0, axis=1)]
        clipped = clipped.transpose()
        meta = {}
        meta['driver'] = 'GTiff'
        meta['width'] = clipped.shape[0]
        meta['height'] = clipped.shape[1]
        meta['count'] = 1
        meta['dtype'] = 'float32'
        meta['crs'] = '+proj=latlong'
        with rasterio.open(tiff_name, 'w', **meta) as out_tiff:
            out_tiff.write(clipped, 1)


class CLDMSK(VIIRSNC):
    def __init__(self, file_name):
        data_group = 'geophysical_data'
        data_name = 'Clear_Sky_Confidence'
        super(CLDMSK, self).__init__(file_name, data_group, data_name)
        self.read_timestamp()


class DNB(VIIRSNC):
    def __init__(self, file_name):
        data_group = 'observation_data'
        data_name = 'DNB_observations'
        super(DNB, self).__init__(file_name, data_group, data_name)
        self.find_geo_filename()
        self.read_timestamp()

    def find_geo_filename(self):
        name_trunk = self.file_name.split('.')[0:-2]
        pattern = '.'.join(name_trunk).replace('VNP02DNB', 'VNP03DNB') + '*'
        self.file_name_geo = glob.glob(pattern)[0]


if __name__ == '__main__':
    pass
