from chunk import Chunk
from chunk_distribution import ChunkDistribution
from sorted_coll import SortedCollection
from pymongo import DESCENDING
from datetime import datetime
from copy import copy


class ConfigParser(object):

    def __init__(self, config_db):
        self.config_db = config_db
        self.history = SortedCollection(key=lambda dist: dist.time)

    
    def get_chunk_distribution(self, namespace):        
        chunks = self.config_db['chunks'].find({'ns': namespace})
        chunk_dist = ChunkDistribution()

        for ch_doc in chunks:
            chunk = Chunk(ch_doc)
            chunk_dist.insert(chunk)  

        return chunk_dist          


    def process_changelog(self, namespace):

        chunk_dist = self.get_chunk_distribution(namespace)
        chunk_dist.time = datetime.max

        self.history.insert(chunk_dist)
        
        changelog = self.config_db['changelog'].find({'ns': namespace, 'what': {'$in': ['split']}}).sort([('time', DESCENDING)])

        for i, chl in enumerate(changelog):

            # process a chunk split
            if chl['what'] == 'split':
                new_dist = self._process_split(chl, self.history[0])
                self.history.insert(new_dist)

            # process a chunk move
            elif chl['what'].startswith('moveChunk.'):
                pass

    def _process_split(self, split_doc, chunk_dist):

        # create a shallow copy of the original chunk distribution
        new_dist = copy(chunk_dist)

        # extract the before, left, right details
        left_doc = split_doc['details']['left']
        right_doc = split_doc['details']['right']
        before_doc = split_doc['details']['before']
       
        # Chunk objects from the splits 
        left_split = Chunk(split_doc, 'left')
        right_split = Chunk(split_doc, 'right')
        before_split = Chunk(split_doc, 'before')

        # Chunk objects found in the distribution
        left_chunk = chunk_dist.find( left_split.range )
        right_chunk = chunk_dist.find( right_split.range )
        
        # some sanity checks: set shards to be equal (they are not in split_doc), then compare
        left_split.shard = left_chunk.shard
        right_split.shard = right_chunk.shard

        if left_split != left_chunk:
            raise ValueError("Error: left chunks not the same. %s <--> %s" % (left_split, left_chunk))
        if right_split != right_chunk:
            print ValueError("Error: right chunks not the same. %s <--> %s" % (right_split, right_chunk))

        # now remove these two chunks and insert a new one
        new_dist.remove(left_chunk)
        new_dist.remove(right_chunk)

        # link before chunk to the left and right (parent / children relationship)
        before_split.shard = left_chunk.shard
        left_chunk.parent = before_split
        right_chunk.parent = before_split
        before_split.children = [left_chunk, right_chunk]

        new_dist.insert(before_split)

        # update time of new distribution
        new_dist.time = split_doc['time']

        # another sanity check: make sure new chunk distribution is correct
        if not new_dist.check(verbose=True):
            raise ValueError('Error: resulting chunk distribution check failed.')
        
        return new_dist

