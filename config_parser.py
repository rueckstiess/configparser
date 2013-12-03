from chunk import Chunk
from chunk_distribution import ChunkDistribution


class ConfigParser(object):

    def __init__(self, config_db):
        self.config_db = config_db

    def get_chunk_distribution(self, namespace):        
        chunks = self.config_db['chunks'].find({'ns': namespace})
        chunk_dist = ChunkDistribution()

        for ch_doc in chunks:
            chunk = Chunk(ch_doc)
            chunk_dist.insert(chunk)  

        return chunk_dist          


    def process_changelog(self, namespace):

        chunk_dist = self.get_chunk_distribution(namespace)
        
        changelog = self.config_db['changelog'].find({'ns': namespace, 'what': {'$in': ['split', 'moveChunk.from', 'moveChunk.start']}}).sort([('time', DESCENDING)])

        for i, chl in enumerate(changelog):
            # print i, chl['what'], chl['time'], chl['details']
            if chl['what'] == 'split':
                self._process_split(chl, chunk_dist)

            elif chl['what'].startswith('moveChunk.'):
                pass

    def _process_split(self, split_doc, chunk_dist):
        left_doc = split_doc['details']['left']
        right_doc = split_doc['details']['right']
        before_doc = split_doc['details']['before']

        # find left and right chunk via shard version
        left = next( (ch for ch in chunk_dist if ch.shard_version == (left_doc['lastmod'].time, left_doc['lastmod'].inc)), None )
        right = next( (ch for ch in chunk_dist if ch.shard_version == (right_doc['lastmod'].time, right_doc['lastmod'].inc)), None )
       
        # range checks
        left_doc_range = (tuple(left_doc['min'].values()), tuple(left_doc['max'].values()))  
        if left.range != left_doc_range:
            print "Error: Chunk range not the same. %s <--> %s" % (left.range, left_doc_range)

        right_doc_range = (tuple(right_doc['min'].values()), tuple(right_doc['max'].values()))  
        if right.range != right_doc_range:
            print "Error: Chunk range not the same. %s <--> %s" % (right.range, right_doc_range)

        # now remove these two chunks and insert a new one
        # # new_dist = deepcopy(chunk_dist)
        # # new_dist.remove(left)
        # # new_dist.remove(right)

        # # new_dist.check(verbose=True)
        # raise SystemExit

    def _process_move(self, move_docs):
        pass

