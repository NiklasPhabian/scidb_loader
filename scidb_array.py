import scidbpy
import numpy
import configparser


class Database:
    def __init__(self, config_file, config_name):
        self.connection = None
        self.address = None
        self.load_config(config_file, config_name)
        self.connect()

    def connect(self):
        self.connection = scidbpy.connect("{address}".format(address=self.address))

    def load_config(self, config_file, config_name):
        config = configparser.ConfigParser(allow_no_value=True)
        config.optionxform = str
        config.read(config_file)
        self.address = config[config_name]['host']


class Array:
    def __init__(self, database):
        self.db_connection = database.connection
        self.name = None
        self.is_temp = False

    def create(self):
        if not self.exists():
            if self.is_temp:
                query = "CREATE TEMP ARRAY {array_name} {attributes} {dimensions}"
            else:
                query = "CREATE ARRAY {array_name} {attributes} {dimensions}"
            query = query.format(array_name=self.name, attributes=self.attributes, dimensions=self.dimensions)
            self.db_connection.iquery(query)

    def exists(self):
        arrays = self.db_connection.iquery("list('arrays')", fetch=True)
        names = arrays['name'].values
        exists = self.name in names
        return exists

    def remove(self):
        if self.exists():
            query = "remove({array_name})".format(array_name=self.name)
            self.db_connection.iquery(query)

    def upload_data(self, data):
        self.db_connection.input(upload_data=data).insert(self.name)

    def scan(self):
        return self.db_connection.scan(self.name)[:]

    def max_versions(self):
        query = "versions({array_name})"
        query = query.format(array_name=self.name)
        ret = self.db_connection.iquery(query, fetch=True)
        max_version = ret['VersionNo'].iloc[-1]
        return max_version

    def remove_old_versions(self):
        max_version = self.max_versions()
        query = "remove_versions({array_name}, {max_version})"
        query = query.format(array_name=self.name, max_version=max_version)
        self.db_connection.iquery(query)

    def redimension(self, destination_array):
        query = "insert(redimension({source}, {template}), {dest})"
        query = query.format(source=self.name, template=destination_array, dest=destination_array)
        self.db_connection.iquery(query)


class HstmArrayLoad(Array):
    def __init__(self, db):
        Array.__init__(self, db)
        self.is_temp = True
        self.name = 'reflectance_load'
        self.attributes = "<hid:int64, scan_group:int8, band1:int16, band2:int16, band3:int16, band4:int16>"
        self.dimensions = "[i={low}:{high}:{overlap}:{chunk_length}]".format(low=0, high='*', chunk_length=500000, overlap=0)


class HstmArray(Array):
    def __init__(self, db):
        Array.__init__(self, db)
        self.name = "reflectance"
        self.attributes = "<band1:int16, band2:int16, band3:int16, band4:int16>"
        self.dimensions = "[scan_group=0:203:0:203; hid=0:*:0:1000000]"

    def get_avg_rgb(self, hid_min, hid_max):
        query = "aggregate(between({array_name}, 0, {hid_min}, null, {hid_max}), avg(band1), avg(band4), avg(band3))"
        query = query.format(array_name=self.name, hid_min=hid_min, hid_max=hid_max)
        ret = self.db_connection.iquery(query, fetch=True)
        scale = 11000
        rgb = (ret[0][1][1] / scale, ret[0][2][1] / scale, ret[0][3][1] / scale)
        rgb = (max(rgb[0], 0), max(rgb[1], 0), max(rgb[2], 0))
        return rgb


class I2ArrayLoad(Array):
    def __init__(self, db):
        Array.__init__(self, db)
        self.is_temp = True
        self.name = 'i2_load'
        self.attributes = "<ilat:int64, ilon:int64, scan_group:int16, " \
                          "band1:int16, band2:int16, band3:int16>"
                          #"band4:int16, band5:int16, band6:int16, " \                          "band7:int16, "\
                          #"band8:int16, band9:int16, band10:int16, band11:int16, band12:int16, band13:int16, " \
                          #"band14:int16, band15:int16, band16:int16>"
        self.dimensions = "[i={low}:{high}:{overlap}:{chunk_length}]".format(low=0, high='*', chunk_length=100000, overlap=0)


class I2Array(Array):
    def __init__(self, db):
        Array.__init__(self, db)
        self.name = 'i2'
        self.attributes = "<band1:int16, band2:int16, band3:int16>"
                          #", band4:int16, band5:int16, band6:int16, band7:int16,"\
                          #"band8:int16, band9:int16, band10:int16, band11:int16, band12:int16, band13:int16, " \
                          #"band14:int16, band15:int16, band16:int16>"
        self.dimensions = "[ilat=-900000:900000:0:50000; ilon=-1800000:1800000:0:50000; scan_group=0:*:0:?]"

    def get_avg_rgb(self, lat_min, lat_max, lon_min, lon_max):
        query = "aggregate(between({array_name}, {lat_min}, {lon_min}, 0, {lat_max}, {lon_max}, null), avg(band1), avg(band4), avg(band3))"
        query = query.format(array_name=self.name, lat_min=lat_min, lon_min=lon_min, lat_max=lat_max, lon_max=lon_max)
        ret = self.db_connection.iquery(query, fetch=True)
        scale = 11000
        rgb = (ret[0][1][1] / scale, ret[0][2][1] / scale, ret[0][3][1] / scale)
        rgb = (max(rgb[0], 0), max(rgb[1], 0), max(rgb[2], 0))
        return rgb

    def get_regridded(self, n_lat, n_lon):
        query = "regrid(i2, {n_lat}, {n_lon}, 203, avg(band1), avg(band4), avg(band3));"


class LoadBandArray(Array):

    def __init__(self, db, n_band):
        Array.__init__(self, db)
        self.name = 'band{n_band}_load'.format(n_band=n_band)
        self.attributes = "<hid:int64, scan_group:int8, reflectance:double>"
        self.dimensions = "[i={low}:{high}:{overlap}:{chunk_length}]".format(low=0, high='*', chunk_length=500000, overlap=0)

    def redimension(self, destination_array):
        query = "insert(redimension({source}, {template}), {dest})"
        query = query.format(source=self.name, template=destination_array, dest=destination_array)
        self.db_connection.iquery(query)


class BandArray(Array):

    def __init__(self, db, n_band):
        Array.__init__(self, db)
        self.name = 'band{n_band}'.format(n_band=n_band)
        self.attributes = "<reflectance:double>"
        self.dimensions = "[hid=0:*:0:1000000; scan_group=0:203:0:203; synth=0:3]"
        self.min = None
        self.max = None
        if self.exists():
            self.max_value()
            self.min_value()

    def get_avg(self, hid_min, hid_max):
        query = "aggregate(between(filter({array_name}, reflectance>0), {hid_min}, 0,  0, {hid_max}, null, null), avg(reflectance))"
        query = query.format(array_name=self.name, hid_min=hid_min, hid_max=hid_max)
        ret = self.db_connection.iquery(query, fetch=True)
        return ret[0][1][1]

    def max_value(self):
        query = "aggregate({array_name}, max(reflectance))"
        query = query.format(array_name=self.name)
        ret = self.db_connection.iquery(query, fetch=True)
        self.max = float(ret[0][1][1])
        return self.max

    def min_value(self):
        query = "aggregate({array_name}, min(reflectance))"
        query = query.format(array_name=self.name)
        ret = self.db_connection.iquery(query, fetch=True)
        self.min = float(ret[0][1][1])
        return self.min

    def get_normalized_avg(self, hid_min, hid_max):
        avg = self.get_avg(hid_min, hid_max)
        return avg/(10000)


class BandArrayInterior(BandArray):
    def __init__(self, db, n_band):
        Array.__init__(self, db)
        self.name = 'bandInterior{n_band}'.format(n_band=n_band)
        self.attributes = "<reflectance:double>"
        self.dimensions = "[scan_group=0:203:0:203; hid=0:*:0:1000000]"

        self.min = None
        self.max = None
        if self.exists():
            self.max_value()
            self.min_value()

    def get_avg(self, hid_min, hid_max):
        query = "aggregate(between(filter({array_name}, reflectance>0), 0, {hid_min}, null,  {hid_max}), avg(reflectance))"
        query = query.format(array_name=self.name, hid_min=hid_min, hid_max=hid_max)
        ret = self.db_connection.iquery(query, fetch=True)
        return ret[0][1][1]


if __name__ == '__main__':
    load_array = LoadBandArray(0)
    load_array.remove()
    load_array.create()

    data = numpy.array([(0, 0,  15), (1, 0, 15)], dtype=[('hid', 'int64'), ('band_id', 'i8'), ('reflectance', 'double')])
    load_array.upload_data(data=data)
    load_array.redimension()
    load_array.remove()


