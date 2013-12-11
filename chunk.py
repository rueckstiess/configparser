class Chunk(object):
    """ represents a chunk, contains all relevant information. """

    def __init__(self, doc=None, which=None):
        """ constructor for Chunk, extract info from doc if specified. The doc can either be a document from 
            the chunks collection, or a split event from the changelog collection. Both can be used to instantiate
            a Chunk. Split needs to specify `which`, either 'before', 'left' or 'right'.
        """

        # these fields are used when comparing two chunks for equality
        self.equality_fields = ['shard_version', 'shardkey_fields', 'range', 'shard', 'namespace']
        self.parent = []
        self.children = []

        if doc:
            # identify if split or chunk document
            if 'server' in doc:
                if which not in ['before', 'left', 'right', 'chunk']:
                    raise ValueError("can't parse document, `which` not specified. must be 'before', 'left', 'right' or 'chunk'.")
                self._from_split(doc, which)
            elif 'shard' in doc:
                self._from_chunk(doc)
            else:
                raise ValueError("can't parse document, neither split nor chunk.")
        else:
            self._source_doc = None
            self._source = None

            self.shard_version = None
            self.shardkey_fields = None
            self.range = None
            self.min = None
            self.max = None
            self.shard = None
            self.namespace = None


    def _from_chunk(self, chunk_doc):
        """ extracts information from a chunk document (from config.chunks) """
        
        self._source_doc = chunk_doc
        self._source = 'chunk'

        # store shard version
        self.shard_version = (chunk_doc['lastmod'].time, chunk_doc['lastmod'].inc)
        
        # shardkey fields
        self.shardkey_fields = chunk_doc['min'].keys()

        # store chunk range
        self.range = ( tuple(chunk_doc['min'].values()), tuple(chunk_doc['max'].values()) )
        self.min = self.range[0]
        self.max = self.range[1]

        # current shard 
        self.shard = chunk_doc['shard']

        # namespace
        self.namespace = chunk_doc['ns']

    
    def _from_split(self, split_doc, which):
        """ extracts information from a split document (from config.changelog). 
            specify `which` as any of 'before', 'left', 'right', 'chunk' (for multi-splits). 
        """

        self._source_doc = split_doc
        self._source = 'split'

        # store shard version
        self.shard_version = (split_doc['details'][which]['lastmod'].time, split_doc['details'][which]['lastmod'].inc)
        
        # shardkey fields
        self.shardkey_fields = split_doc['details'][which]['min'].keys()

        # store chunk range
        self.range = ( tuple(split_doc['details'][which]['min'].values()), tuple(split_doc['details'][which]['max'].values()) )
        self.min = self.range[0]
        self.max = self.range[1]

        # current shard not in split_doc
        self.shard = None

        # namespace
        self.namespace = split_doc['ns']



    def _is_equal(self, other, equality_fields=None):
        """ comparison function for equality. If equality_fields is given here, those are used. 
            Otherwise the global self.equality_fields are used. 
        """
        efields = equality_fields or self.equality_fields
        return all( getattr(self, f) == getattr(other, f) for f in efields )

    def __eq__(self, other):
        """ self == other """
        return self._is_equal(other)

    def __ne__(self, other):
        """ self != other """
        return not self._is_equal(other)

    def __repr__(self):
        """ representation of a chunk as string. """
        return 'Chunk( ns=%s, range=%s-->%s, shard_version=%s, shard=%s )' % (self.namespace, self.min, self.max, self.shard_version, self.shard)

    def all(self, fields=None):
        """ Useful to print all fields of a chunk. Usage:  print chunk.all() """
        s =  'Chunk:'
        fields = fields or self.equality_fields
        for f in fields:
            s += '    %s: %s' % (f, getattr(self, f))
        return s



if __name__ == '__main__':

    # Example on how to create a chunk from a "chunks" document and from a changelog "split" document
    
    from bson.min_key import MinKey
    from bson.max_key import MaxKey
    from bson import ObjectId, Timestamp
    import datetime

    c1 = Chunk({u'_id': u'mydb.mycoll_id_MinKey', u'min': {u'_id': MinKey()}, u'max': {u'_id': 0}, u'ns': u'mydb.mycoll', u'shard': u'shard0000', u'lastmodEpoch': ObjectId('52941cc0d0120f1f83928407'), u'lastmod': Timestamp(2, 1)})
    c2 = Chunk({u'what': u'split', u'ns': u'blab_store_timed.posts20131119T160000', u'clientAddr': u'10.238.206.52:38629', u'server': u'mongopostsshard1b', u'details': {u'left': {u'max': {u'_id': 7338642804404907374L}, u'lastmod': Timestamp(15, 118), u'lastmodEpoch': ObjectId('528b8b00e6d02724c1318ebc'), u'min': {u'_id': 7267532332871149567L}}, u'right': {u'max': {u'_id': 7552657837320303474L}, u'lastmod': Timestamp(15, 119), u'lastmodEpoch': ObjectId('528b8b00e6d02724c1318ebc'), u'min': {u'_id': 7338642804404907374L}}, u'before': {u'max': {u'_id': 7552657837320303474L}, u'lastmod': Timestamp(15, 55), u'lastmodEpoch': ObjectId('000000000000000000000000'), u'min': {u'_id': 7267532332871149567L}}}, u'time': datetime.datetime(2013, 11, 19, 16, 59, 32, 700000), u'_id': u'mongopostsshard1b-2013-11-19T16:59:32-528b98f4da0d63ea7e0ef1c8'}, 'before')
    
    print c1
    print c2


