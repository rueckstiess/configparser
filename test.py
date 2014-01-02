from pymongo import MongoClient

from bson.min_key import MinKey
from bson.max_key import MaxKey
from bson import ObjectId, Timestamp

from chunk import Chunk
from chunk_distribution import ChunkDistribution
from config_parser import ConfigParser

from copy import copy, deepcopy
from dateutil import parser

### CS-9785 config servers as example, although a bit boring because they don't have many splits

# create a MongoClient on the correct port
mc = MongoClient(port=27017)

# specify the config database ( here I imported all 3 config servers to 1 mongod, hence config[1,2,3] )
config_db = mc['config']

# create config parser object with the config_db as parameter
cfg_parser = ConfigParser(config_db)

# get all collections
collections = [c['_id'] for c in config_db['collections'].find({'dropped': {'$ne': True}})]

# validate that for each collection, the corresponding chunks form a distribution from 
# MinKey to MaxKey without gaps or overlaps.
for namespace in collections:
    print namespace, 
    chunk_dist = cfg_parser.get_chunk_distribution(namespace)
    if chunk_dist.check(verbose=True):
        print '  ok'

# pick first collection (arbitrary, change to specific namespace here)
namespace = "order_history.OrdersAudit"

# walk distributions backwards from chunks collection, each step applying one changelog event (move / split)
for chunk_dist in cfg_parser.walk_distributions(namespace):
    print chunk_dist.what, chunk_dist.time, len(chunk_dist), chunk_dist.max_shard_version()

# now build full history of ChunkDistribution objects over time (slow, expensive)
# history = cfg_parser.build_full_history(namespace)

# find the distribution as it was at a specific date and time, use SortedCollection's "find less than or equal": find_le()
# t = "2013-11-24 16:13"
# chunk_dist = history.find_le(parser.parse(t))
# print "last change was a %s at %s" % (chunk_dist.what, chunk_dist.time)
# print chunk_dist

