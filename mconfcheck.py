from pymongo import MongoClient, ASCENDING

from bson.min_key import MinKey
from bson.max_key import MaxKey
from bson import ObjectId, Timestamp

from chunk import Chunk
from chunk_distribution import ChunkDistribution
from config_parser import ConfigParser

from copy import copy, deepcopy
from dateutil import parser
from operator import itemgetter
from itertools import izip_longest

import pprint
import argparse
import re
import time

from mtools.util.cmdlinetool import BaseCmdLineTool


def all_equal(l):
    return all( it == l[0] for it in l )

def argmax(l):
    return max(enumerate(l), key=itemgetter(1))[0]


class MConfCheckTool(BaseCmdLineTool):

    def __init__(self):
        """ Constructor: add description to argparser. """
        BaseCmdLineTool.__init__(self)

        self.argparser.description = 'Performs a health check on config servers and compares them for inconsistencies.'
        self.argparser.add_argument('config', action='store', nargs='*', metavar='URI', default=['mongodb://localhost:27017/config'], help='provide uri(s) to config server(s), default is mongodb://localhost:27017/config')

    def run(self, arguments=None):
        BaseCmdLineTool.run(self, arguments)

        # number of configs
        self.num_configs = len(self.args['config'])

        # parse config server URIs
        regex = re.compile(r'(?P<mongodb>mongodb://)?((?P<user>\w+):(?P<password>\w+)@)?(?P<host>\w+)(:(?P<port>\d+))?/(?P<database>\w+)')
        matches = [ regex.match(uri) for uri in self.args['config'] ]

        # verify that all config URIs are parsed correctly and contain a database
        # TODO check for empty config dbs
        if not all(matches) or not all(m.groupdict()['database'] for m in matches):
            raise SystemExit('Unable to parse config server URIs, please check syntax: mongodb://[username:password@]host[:port]/database')

        # for convenience mongodb:// can be omitted for this script, but MongoClient expects it
        for i, uri in enumerate(self.args['config']):
            if not uri.startswith('mongodb://'):
                self.args['config'][i] = 'mongodb://' + uri

        # add uri and short_uri to self.parsed_uri dicts
        self.parsed_uris = [m.groupdict() for m in matches]
        for i, (uri, puri) in enumerate(zip(self.args['config'], self.parsed_uris)):
            self.parsed_uris[i]['uri'] = uri
            if puri['port'] == None:
                self.parsed_uris[i]['port'] = '27017'
            self.parsed_uris[i]['short_uri'] = '%s:%s/%s' % (puri['host'], puri['port'], puri['database'])

        # connect to databases
        self.config_dbs = [ MongoClient(puri['uri'])[puri['database']] for puri in self.parsed_uris ]

        print "\n>> individual health checks on all config servers"
        print   "   (verifies that for each namespace, the chunk ranges reach from MinKey to MaxKey without gaps or overlaps)\n"
        for puri, database in zip(self.parsed_uris, self.config_dbs):
            print puri['short_uri']
            self._health_check(database)
            print 

        if len(self.config_dbs) > 1:
            print "\n>> comparing config.collections collection for each config server"
            print   "   (verifies that they agree on the state of each collection)\n"
            self._compare_collections()


            print "\n>> comparing config.chunks collection for each config server"
            print   "   (verifies that they agree on chunk ranges for each namespace and finds deviation point)\n"
            self._compare_chunks_and_reconstruct()


    def _health_check(self, database):
        # create config parser
        cfg_parser = ConfigParser(database)

        # get all collections
        collections = [c['_id'] for c in database['collections'].find({'dropped': {'$ne': True}})]

        # validate that for each collection, the corresponding chunks form a distribution from 
        # MinKey to MaxKey without gaps or overlaps.
        for namespace in collections:
            print '    ', namespace, 
            chunk_dist = cfg_parser.get_chunk_distribution(namespace)
            ret, msgs = chunk_dist.check()
            if ret: 
                print '  ok'
            else:
                print '  failed\n'
                for msg in msgs:
                    print '       ! %s' % msg
                print


    def _compare_collections(self):

        # get a set of all collections of all config servers
        collection_dicts = [ dict([ doc.values() for doc in db['collections'].find(fields=['_id', 'dropped']).sort([('_id', ASCENDING)]) ]) for db in self.config_dbs ]
        all_collections = set()
        for cd in collection_dicts:
            all_collections = all_collections.union(cd.keys())

        print_header = False

        # get longest collection and longest short uri for printing purposes

        shorturi_len = max( len(puri['short_uri']) for puri in self.parsed_uris )
        collection_len = max ( len(c) for c in all_collections )

        # now check for each collection...        
        for collection in all_collections:
            ok = True

            # ...if all config servers have this collection
            if not all(collection in cd for cd in collection_dicts):
                ok = False

            # ...that all config servers agree on the dropped state
            if ok:
                if not all(cd[collection] == collection_dicts[0][collection] for cd in collection_dicts):
                    ok = False

            if not ok:
                if not print_header:
                    print '! collections differ'.ljust(collection_len + 4),
                    print '    '.join( puri['short_uri'].ljust(shorturi_len) for puri in self.parsed_uris )
                    print
                    print_header = True

                print collection.ljust(collection_len + 4),
                for cd in collection_dicts:
                    if collection in cd:
                        word = 'dropped' if cd[collection] else 'sharded'
                    else:
                        word = 'missing'
                    print word.center(shorturi_len) + '   ',
                print
        print

        # save extracted information for next validation
        self.all_collections = all_collections
        self.collection_dicts = collection_dicts


    def _compare_chunks_and_reconstruct(self):

        config_parsers = [ ConfigParser(db) for db in self.config_dbs ]
        shorturi_len = max( len(puri['short_uri']) for puri in self.parsed_uris )


        for collection in self.all_collections:
            print collection, '\n'
            chunk_dists = [ parser.get_chunk_distribution(collection) for parser in config_parsers ]
            diff_found = False

            # check if the chunk distributions disagree on the chunks collection
            if not all_equal( chunk_dists ):
                diff_found = True
                chunk_len = len( str(dict(zip(chunk_dists[0][0].shardkey_fields, chunk_dists[0][0].max))) )
                shorturi_len = max(shorturi_len, chunk_len)

                print '    ! chunks differ',
                print '    ' + '    '.join( puri['short_uri'].ljust(shorturi_len) for puri in self.parsed_uris )
                print
            
                # TODO: prints all chunk differences but doesn't align them anymore, once a difference is found
                # all following chunks will also differ. needs to align them again.
                
                # for chunks in izip_longest(*chunk_dists, fillvalue=Chunk()):
                #     if not all_equal(chunks):
                #         print 'first diff >'.center(23),

                #         for ch in chunks:
                #             if ch.min:
                #                 print str(dict(zip(ch.shardkey_fields, ch.min))).ljust(chunk_len) + '   ',
                #             else:
                #                 print ''.ljust(chunk_len) + '   ',
                #         print
                #         break


            if diff_found:
                # now go backwards in time to find a point where all chunk distributions were the same
                chunk_dist_generators = [ parser.walk_distributions(collection) for parser in config_parsers ]
                
                # get first element of each generator
                current_chunk_dists = [ generator.next() for generator in chunk_dist_generators ]

                while not all_equal( current_chunk_dists ):
                    # find chunk distribution with highest chunk version
                    highest_dist_index = argmax( [ dist.max_shard_version() if isinstance(dist, ChunkDistribution) else (-1, -1) for dist in current_chunk_dists ] )

                    # let that generator generate the next chunk distribution and replace in current dists
                    try:
                        current_chunk_dists[ highest_dist_index ] = chunk_dist_generators[ highest_dist_index ].next()
                    except StopIteration:
                        current_chunk_dists[ highest_dist_index ] = None


                if all_equal( current_chunk_dists ) and current_chunk_dists[0] != None:
                    print "    metadata was identical last on %s" % current_chunk_dists[ highest_dist_index ].time
                    # TODO: print more info about when they were all equal etc.
                    return 




if __name__ == '__main__':
    tool = MConfCheckTool()
    tool.run()