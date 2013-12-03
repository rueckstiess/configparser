class Chunk(object):
    """ represents a chunk, contains all relevant information. """

    def __init__(self, chunk_doc=None):
        """ constructs chunk from chunk document (from the chunks collection) """
        if chunk_doc:
            self.chunk_doc = chunk_doc

            # store shard version
            self.shard_version = (chunk_doc['lastmod'].time, chunk_doc['lastmod'].inc)
            
            # choose shardkeyfct (identity or hashable dictionary for compound shard keys)
            self.shardkey_fields = chunk_doc['min'].keys()

            # store chunk range
            self.range = ( tuple(chunk_doc['min'].values()), tuple(chunk_doc['max'].values()) )
            self.min = self.range[0]
            self.max = self.range[1]

            # current shard 
            self.shard = chunk_doc['shard']

            # namespace
            self.namespace = chunk_doc['ns']

        else:
            # also allow for empty chunk construction
            self.chunk_doc = None
            self.shard_version = None
            self.shardkey_fields = None
            self.range = None
            self.min = None
            self.max = None
            self.shard = None
            self.namespace = None


    def _is_equal(self, other, required_fields=['shard_version', 'shardkey_fields', 'range', 'shard', 'namespace']):
        return all( getattr(self, rf) == getattr(other, rf) for rf in required_fields )

    def __eq__(self, other):
        return self._is_equal(other)

    def __ne__(self, other):
        return not self._is_equal(other)

    def __repr__(self):
        return 'Chunk( ns="%s", range=%s --> %s, shard_version=%s )' % (self.namespace, self.min, self.max, self.shard_version)




if __name__ == '__main__':

    from bson.min_key import MinKey
    from bson.max_key import MaxKey
    from bson import ObjectId, Timestamp

    c1 = Chunk({u'_id': u'mydb.mycoll_id_MinKey', u'min': {u'_id': MinKey()}, u'max': {u'_id': 0}, u'ns': u'mydb.mycoll', u'shard': u'shard0000', u'lastmodEpoch': ObjectId('52941cc0d0120f1f83928407'), u'lastmod': Timestamp(2, 1)})
    c2 = Chunk({u'_id': u'mydb.mycoll_id_0', u'min': {u'_id': 0}, u'max': {u'_id': MaxKey()}, u'ns': u'mydb.mycoll', u'shard': u'shard0001', u'lastmodEpoch': ObjectId('52941cc0d0120f1f83928407'), u'lastmod': Timestamp(2, 0)})
    
    print c1
    print c2


