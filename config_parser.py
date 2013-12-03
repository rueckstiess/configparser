from chunk import Chunk
from chunk_distribution import ChunkDistribution
from sorted_coll import SortedCollection
from pymongo import DESCENDING
from datetime import datetime
from copy import copy, deepcopy


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
        chunk_dist.what = 'chunks'

        self.history.insert(chunk_dist)
        
        changelog = list( self.config_db['changelog'].find({'ns': namespace, 'what': {'$in': ['split', 'moveChunk.from', 'moveChunk.to', 'moveChunk.start', 'moveChunk.commit']}}).sort([('time', DESCENDING)]) ) 

        for i, chl in enumerate(changelog):

            # process a chunk split
            if chl['what'] == 'split':
                new_dist = self._process_split(chl, self.history[0])
                self.history.insert(new_dist)

            # process a chunk move if it wasn't aborted
            elif chl['what'] == 'moveChunk.from':
                new_dist = self._process_move(changelog[i:], self.history[0])
                if new_dist:
                    self.history.insert(new_dist)


    def _process_split(self, split_doc, chunk_dist):

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

        # update any shard versions if they are different (retrospectively, from moved chunks)
        if left_chunk.shard_version != left_split.shard_version:
            left_chunk.shard_version = left_split.shard_version
        if right_chunk.shard_version == right_split.shard_version:
            right_chunk.shard_version = right_split.shard_version

        if left_split != left_chunk:
            raise ValueError("Error: left chunks not the same. %s <--> %s" % (left_split, left_chunk))
        if right_split != right_chunk:
            print ValueError("Error: right chunks not the same. %s <--> %s" % (right_split, right_chunk))

        # create a shallow copy of the original chunk distribution
        new_dist = copy(chunk_dist)

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
        new_dist.what = 'split'

        # another sanity check: make sure new chunk distribution is correct
        if not new_dist.check(verbose=True):
            raise ValueError('Error: resulting chunk distribution check failed.')
        
        return new_dist


    def _process_move(self, changelog, chunk_dist):

        docs = {}

        # search for the relevant changelog events for a move
        docs['from'] = changelog[0]

        # skip aborted moves
        if 'note' in docs['from']['details'] and docs['from']['details']['note'] == 'abort':
            return False

        # search changelog from the `from` document backwards in time to find `commit`, `to`, `start`.
        for chl in changelog[1:]:
            what = chl['what']

            # only accept docs that start with moveChunk.
            if not what.startswith('moveChunk.'):
                return False
            what = what.split('.')[1]

            # only consider entries that match the range 
            if chl['details']['min'] != docs['from']['details']['min'] or chl['details']['max'] != docs['from']['details']['max']:
                return False

            # once another from is found, abort here (we need start, to, commit)
            if chl['what'] == 'from':
                return False

            # only find one single doc for each what
            if what in docs:
                return False
            else:
                docs[what] = chl

            if len(set(docs.keys())) == 4:
                break

        # not all 4 doc types (start, to, commit, from) found
        if len(set(docs.keys())) != 4:
            return 

        # we have a full set of all 4 doc types here, go ahead

        # find chunk that is being moved
        chunk_range = tuple(docs['from']['details']['min'].values()), tuple(docs['from']['details']['max'].values())
        chunk = chunk_dist.find( chunk_range )

        # duplicate chunk (deep copy) and update (remove shard version as it is unknown)
        new_chunk = deepcopy(chunk)
        new_chunk.shard_version = None
        new_chunk.shard = docs['start']['details']['from']
        new_chunk.children = [chunk]
        chunk.parent = new_chunk

        # create a shallow copy of the original chunk distribution
        new_dist = copy(chunk_dist)

        # delete old chunk and insert new chunk
        new_dist.remove(chunk)
        new_dist.insert(new_chunk)
        new_dist.time = docs['commit']['time']
        new_dist.what = 'move'

        return new_dist


