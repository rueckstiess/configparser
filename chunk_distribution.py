from sorted_coll import SortedCollection

from pymongo import MongoClient, DESCENDING

from bson.min_key import MinKey
from bson.max_key import MaxKey


# Monkey-patch MinKey and MaxKey comparison (for now, see PYTHON-604)

MinKey.__le__ = lambda self, other: True
MinKey.__lt__ = lambda self, other: other != MinKey()
MinKey.__gt__ = lambda self, other: False
MinKey.__ge__ = lambda self, other: other == MinKey()

MaxKey.__le__ = lambda self, other: other == MaxKey()
MaxKey.__lt__ = lambda self, other: False
MaxKey.__gt__ = lambda self, other: other != MaxKey()
MaxKey.__ge__ = lambda self, other: True


class ChunkDistribution(SortedCollection):
    """ Holds a collection of chunks, sorted by chunk.range, which is a tuple of tuple of values. """

    def __init__(self, iterable=(), key=None):
        """ constructor, sets key of SortedCollection to chunk.range, then call superclass' __init__. """
        key = lambda chunk: chunk.range
        SortedCollection.__init__(self, iterable=iterable, key=key)

        self.time = None


    def check(self, verbose=False):
        """ check that chunk distribution is complete and correct """
        okay = True

        # check that range starts with MinKey on all fields
        if not all([value == MinKey() for value in self[0].min]):
            if verbose:
                print "Error: ChunkDistribution does not start with MinKey."
            okay = False

        # check that range ends with MaxKey on all fields
        if not all([value == MaxKey() for value in self[-1].max]):  
            if verbose:
                print "Error: ChunkDistribution does not end with MaxKey."
            okay = False

        # check that range has no gaps or overlaps (last chunks max needs to be equal to next chunks min for all chunks)
        for c1, c2 in zip(self[:-1], self[1:]):
            if c2.min != c1.max:
                if verbose:
                    print "Error: Gap or overlap found in ChunkDistribution for %s between the following points: %s --> %s" % (c1.namespace, c1.max, c2.min)
                okay = False

        # check that all chunks have the same namespace
        ns_set = set([ch.namespace for ch in self])
        if len(ns_set) > 1:
            if verbose:
                print "Error: Not all chunks have the same namespace: %s" % ', '.join(ns_set)
            okay = False

        return okay


    def __repr__(self):
        """ print first and last 3 chunks """
        s = 'ChunkDistribution( [\n'

        if len(self) < 6:
            s += ',\n'.join(['    ' + str(ch) for ch in self])
        else:
            s += ',\n'.join(['    ' + str(ch) for ch in self[:3]])
            s += ',\n    ...\n'
            s += ',\n'.join(['    ' + str(ch) for ch in self[-3:]])
                
        s += '\n] )'
        return s





