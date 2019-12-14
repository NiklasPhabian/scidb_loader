import scidbpy


class Array:
    def __init__(self, name, db, temp=False):
        self.db = db
        self.name = name
        self.temp = temp
        
    def exists(self):        
        arrays = self.db.iquery("list('arrays')", fetch=True)
        names = arrays['name'].values
        exists = self.name in names        
        return exists
    
    def max_version(self):
        query = "versions({array_name})"
        query = query.format(array_name=self.name)
        ret = self.db.iquery(query, fetch=True)
        max_version = ret['VersionNo'].iloc[-1]
        return max_version
    
    def create(self):
        if not self.exists():
            if self.temp:
                query = "CREATE TEMP ARRAY {name} {attributes} {dimensions}"
            else:
                query = "CREATE ARRAY {name} {attributes} {dimensions}"                
            query = query.format(name=self.name, attributes=self.attributes, dimensions=self.dimensions)
            self.db.iquery(query)

    def remove(self):
        if self.exists():
            query = "remove({array})".format(array=self.name)
            self.db.iquery(query)
        
    def remove_old_versions(self):
        max_version = self.max_versions()
        query = "remove_versions({array_name}, {max_version})"
        query = query.format(array_name=self.name, max_version=max_version)
        self.db.iquery(query)
            
    def scan(self):
        return self.db.scan(self.name)[:]
    
    def head(self):    
        return scidbpy.db.Array(db=self.db, name=self.name).head()
        
    def from_numpy(self, numpy_array):                 
        self.db.input(upload_data=numpy_array).store(self.name)
        
    def from_tsv_aio(self, tsv_path):   
        query = '''store(
                        aio_input(
                            '{tsv_file}', 
                            num_attributes:5, 
                            header:0, 
                            chunk_size:324800),
                        {array})'''
        query = query.format(array=self.name, tsv_file=tsv_path)  
        self.db.iquery(query) 
        
    def add_stare_spatial(self):
        query = '''store(
                        apply(
                            {array}, 
                            stare_spatial, stareFromResolutionLatLon(23, lat, lon)), 
                        tmp)'''
        query = query.format(array=self.name)
        self.db.iquery(query)
        self.db.iquery('remove({array})'.format(array=self.name))
        self.db.iquery('rename(tmp, {array})'.format(array=self.name))
        
    def add_stare_temporal(self):
        query = '''store(
                        apply(
                            {array}, 
                            stare_temporal, stareFromUTCDateTime(3, time_stamp)), 
                        tmp)'''
        query = query.format(array=self.name)
        self.db.iquery(query)
        self.db.iquery('remove({array})'.format(array=self.name))
        self.db.iquery('rename(tmp, {array})'.format(array=self.name))

    def redimension(self, attributes, dimensions):
        query = '''store(
                        redimension(
                            {array}, 
                            {attributes}{dimensions}), 
                        tmp)'''
        query=query.format(array=self.name, attributes=attributes, dimensions=dimensions)
        self.db.iquery(query)
        self.db.iquery('remove({array})'.format(array=self.name))
        self.db.iquery('rename(tmp, {array})'.format(array=self.name)) 
    
    def insert_into(self, target_array):
        query = '''insert(
                        redimension(
                            {array}, 
                            {attributes}{dimensions}), 
                        {target_name})'''
        query=query.format(array=self.name, 
                           target_name=target_array.name, 
                           attributes=target_array.attributes, 
                           dimensions=target_array.dimensions)
        self.db.iquery(query)
        
    def replace_attributes(self):
        query = '''store(
                        discard(discard(discard(
                            apply(
                                {array},
                                lat, dcast(a3, float(null)), 
                                lon, dcast(a4, float(null)), 
                                csc, dcast(a2, float(null))),
                            a0, a1), a2, a3), a4, error),
                        tmp)'''
        query=query.format(array=self.name)        
        self.db.iquery(query)
        self.db.iquery('remove({array})'.format(array=self.name))
        self.db.iquery('rename(tmp, {array})'.format(array=self.name))



class Cldmsk(Array):
    def __init__(self, db):
        name = 'cldmsk'
        Array.__init__(self, name=name, db=db)
        spatial_chunk = 1000*1000*1000*1000*1000
        

        temporal_chunk = '*'        
        self.attributes = "<time_stamp:datetime NOT NULL, Clear_Sky_Confidence:float NOT NULL, Integer_Cloud_Mask:int8 NOT NULL, lat:double NOT NULL, lon:double NOT NULL>"                
        self.attributes = '<Clear_Sky_Confidence:float NOT NULL, Integer_Cloud_Mask:int8 NOT NULL>'
        self.dimensions = '''[stare_spatial={low}:{high}:{overlap}:{spatial_chunk},
                              stare_temporal={low}:{high}:{overlap}:{temporal_chunk},
                              synth=0:3]'''        
        self.dimensions = self.dimensions.format(low=0, 
                                                 high='*', 
                                                 spatial_chunk=spatial_chunk,                                                  
                                                 temporal_chunk=temporal_chunk,                                                  
                                                 overlap=0)



#store(redimension(test, <lat:double NOT NULL, lon:double NOT NULL>[stare_spatial=0:*:0:1000000000000;synth=0:4]),c)
