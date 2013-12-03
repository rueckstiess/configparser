from pymongo import MongoClient

from bson.min_key import MinKey
from bson.max_key import MaxKey
from bson import ObjectId, Timestamp

from chunk import Chunk
from chunk_distribution import ChunkDistribution

from config_parser import ConfigParser


c1 = Chunk({u'_id': u'mydb.mycoll_id_MinKey', u'min': {u'_id': MinKey()}, u'max': {u'_id': 0}, u'ns': u'mydb.mycoll', u'shard': u'shard0000', u'lastmodEpoch': ObjectId('52941cc0d0120f1f83928407'), u'lastmod': Timestamp(2, 1)})
c2 = Chunk({u'_id': u'mydb.mycoll_id_0', u'min': {u'_id': 0}, u'max': {u'_id': MaxKey()}, u'ns': u'mydb.mycoll', u'shard': u'shard0001', u'lastmodEpoch': ObjectId('52941cc0d0120f1f83928407'), u'lastmod': Timestamp(2, 0)})

chunk_dist = ChunkDistribution()

chunk_dist.insert(c1)
chunk_dist.insert(c2)

print chunk_dist
print "check:", chunk_dist.check(verbose=True)

print "\n-------------------\n"

mc = MongoClient(port=30000)
config_db = mc['config1']

cfg_parser = ConfigParser(config_db)

# get all collections
collections = [c['_id'] for c in config_db['collections'].find({'dropped': {'$ne': True}})]

for namespace in collections:
    print namespace, 
    chunk_dist = cfg_parser.get_chunk_distribution(namespace)
    if chunk_dist.check(verbose=True):
        print '  ok'

print "\n-------------------\n"

collection = 'blab_store_timed.posts20131125T160000'
chunk_dist = cfg_parser.get_chunk_distribution(collection)
print chunk_dist

