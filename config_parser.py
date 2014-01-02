from chunk import Chunk
from chunk_distribution import ChunkDistribution
from sorted_coll import SortedCollection
from pymongo import DESCENDING
from datetime import datetime
from copy import copy, deepcopy

from pprint import pprint

class ConfigParser(object):
    """ Config Parser offers some methods to convert a config database on a running mongod into
        useable and queryable objects in Python. It starts with the current distribution of 
        chunks, from the config.chunks collection, builds a ChunkDistribution from it, then 
        uses the changelog as delta changes. It walks the changelog backwards in time, applying
        each change to create a new ChunkDistribution. At the end, it returns a SortedCollection
        of ChunkDistributions, sorted by time.
    """

    def __init__(self, config_db):
        self.config_db = config_db
        self.processed_multisplits = set()
    

    def get_chunk_distribution(self, namespace): 
        """ returns a single ChunkDistribution object based on the current state of the
            cluster given by its config.chunks collection. 
        """       
        chunks = self.config_db['chunks'].find({'ns': namespace})
        chunk_dist = ChunkDistribution()

        for ch_doc in chunks:
            chunk = Chunk(ch_doc)
            chunk_dist.insert(chunk)  

        return chunk_dist          


    def walk_distributions(self, namespace):
        """ iterator over chunk distributions backwards in time. """

        # get original chunk distribution
        chunk_dist = self.get_chunk_distribution(namespace)
        self.processed_multisplits = set()
        
        # now get changelog ( only splits and moveChunk.* )
        changelog = list( self.config_db['changelog'].find({'ns': namespace, 'what': {'$in': ['multi-split', 'split', 'moveChunk.from', 'moveChunk.to', 'moveChunk.start', 'moveChunk.commit']}}).sort([('time', DESCENDING)]) ) 

        for i, chl in enumerate(changelog):
            # process a chunk split
            if chl['what'] == 'split':
                new_dist = self._process_split(chl, chunk_dist)

            # process a chunk multi-split
            elif chl['what'] == 'multi-split':
                new_dist = self._process_multi_split(changelog[i:], chunk_dist)

            # process a chunk move
            elif chl['what'] == 'moveChunk.from':
                new_dist = self._process_move(changelog[i:], chunk_dist)

            # none of that? go to next doc, no yield
            else:
                continue

            if new_dist:
                # attach changelog entry to chunk distribution
                chunk_dist.applied_change = chl

                # yield previous distribution
                yield chunk_dist
                chunk_dist = new_dist

        # yield final distribution
        chunk_dist.time = datetime.min
        yield chunk_dist



    def build_full_history(self, namespace):
        """ Builds an initial ChunkDistribution from the config.chunks collection, then walks
            the changelog backwards and creates a new ChunkDistribution for each step (either 
            a split or a move). All these ChunkDistributions are inserted into a SortedCollection
            and returned.
        """

        history = SortedCollection(key=lambda dist: dist.time)

        for chunk_dist in self.walk_distributions(namespace):
            history.insert(chunk_dist)

        return history



    def _process_split(self, split_doc, chunk_dist):
        """ Processes a single split event, transforming a given ChunkDistribution into a new one,
            where the two chunks are merged back into one original (split backwards).
        """

        # extract the before, left, right details
        left_doc = split_doc['details']['left']
        right_doc = split_doc['details']['right']
        before_doc = split_doc['details']['before']
       
        # Chunk objects from the splits 
        left_split = Chunk(split_doc, 'left')
        right_split = Chunk(split_doc, 'right')
        before_split = Chunk(split_doc, 'before')

        # Chunk objects found in the distribution
        try:
            left_chunk = chunk_dist.find( left_split.range )
        except ValueError:
            raise ValueError("Error processing split: can't find left chunk in distribution.")

        try: 
            right_chunk = chunk_dist.find( right_split.range )
        except ValueError:
            raise ValueError("Error processing split: can't find right chunk in distribution.")
        
        # set shards to be equal (they are not in split_doc), then compare
        left_split.shard = left_chunk.shard
        right_split.shard = right_chunk.shard

        # update any shard versions if they are different (retrospectively, from moved chunks)
        left_chunk.shard_version = left_split.shard_version
        right_chunk.shard_version = right_split.shard_version

        if left_split != left_chunk:
            raise ValueError("Error processing split: left chunks not the same. %s <--> %s" % (left_split, left_chunk))
        if right_split != right_chunk:
            print ValueError("Error processing split: right chunks not the same. %s <--> %s" % (right_split, right_chunk))

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
        chunk_dist.time = split_doc['time']
        chunk_dist.what = 'split'

        # another sanity check: make sure new chunk distribution is correct
        if not new_dist.check(verbose=True):
            raise ValueError('Error processing split: resulting chunk distribution check failed.')
        
        return new_dist


    def _process_multi_split(self, changelog, chunk_dist):
        """ Processes a multi-split event, transforming a given ChunkDistribution into a new one,
            where all the children chunks are merged back into one original (split backwards).
        """
        # check if this multi-split has already been processed (only process first one for each shard version)
        split_doc = changelog[0]
        lastmod = (split_doc['details']['before']['lastmod'].time, split_doc['details']['before']['lastmod'].inc)

        if lastmod in self.processed_multisplits:
            return False
        else:
            self.processed_multisplits.add(lastmod)

        # find all documents in the changelog belonging to this multi-split
        multi_split_docs = filter( lambda chl: chl['what'] == 'multi-split' and
                                               chl['details']['before']['lastmod'] == split_doc['details']['before']['lastmod'], changelog) 

        # "before" doc and its chunk
        before_doc = split_doc['details']['before']
        before_split = Chunk(split_doc, 'before')

        # Chunk objects found in the distribution
        chunks = []
        for doc in multi_split_docs:
            split = Chunk(doc, 'chunk')
            try:
                chunk = chunk_dist.find( split.range )
            except ValueError:
                raise ValueError("Error processing multi-split: can't find a chunk in distribution.")

            # set shards to be equal (they are not in split_doc), then compare
            split.shard = chunk.shard

            # update shard versions in chunks
            chunk.shard_version = split.shard_version

            if split != chunk:
                raise ValueError("Error processing multi-split: chunks not the same. %s <--> %s" % (split, chunk))

            chunks.append(chunk)

        # create a shallow copy of the original chunk distribution
        new_dist = copy(chunk_dist)

        # now remove all chunks and insert a new one
        for chunk in chunks:
            new_dist.remove(chunk)

        # link before chunk to all children chunks
        before_split.shard = chunks[0].shard
        for c, chunk in enumerate(chunks):
            chunks[c].parent = before_split
        before_split.children = chunks

        new_dist.insert(before_split)

        # update time of new distribution
        chunk_dist.time = split_doc['time']
        chunk_dist.what = 'multi-split'

        # another sanity check: make sure new chunk distribution is correct
        if not new_dist.check(verbose=True):
            raise ValueError('Error processing multi-split: resulting chunk distribution check failed.')
        
        return new_dist


    def _process_move(self, changelog, chunk_dist):
        """ Processes a single chunk move event, transforming a ChunkDistribution into a new ChunkDistribution,
            where the chunk that is moved is replaced by a chunk with same range, but the previous shard.
        """

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
        
        chunk_dist.time = docs['commit']['time']
        chunk_dist.what = 'move'

        return new_dist


