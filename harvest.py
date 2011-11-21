#!/usr/bin/env python

import sys
import shelve
import bsddb
from datetime import datetime, timedelta

from decorator import decorator
from oaipmh.client import Client
from oaipmh.metadata import MetadataRegistry, oai_dc_reader
from oaipmh.error import NoRecordsMatchError


def _memoize(func, *args, **kw):
    if kw: # frozenset is used to ensure hashability
        key = args, frozenset(kw.iteritems())
    else:
        key = args
    cache = func.cache  # attributed added by memoize
    if key in cache:
        return cache[key]
    else:
        cache[key] = result = func(*args, **kw)
        return result

def memoize(f):
    f.cache = {}
    return decorator(_memoize, f)

class Store:
    def __init__(self):
        self.store = shelve.BsdDbShelf(bsddb.btopen('datastore', 'c'))
    def write_record(self, header, metadata):
        identifier = header.identifier()
        datestamp = header.datestamp()
        data = metadata.getMap()
        data['datestamp'] = [datestamp] #list for consistency with other contents
        key = datestamp.strftime('%Y-%m-%d') + ' ' + identifier #nice ordering guarantees for our btree database
        self.store[key] = data
    @memoize
    def last(self):
        try:
            return self.store.last()[1]['datestamp']
        except:
            return None
    def close(self):
        self.store.close()



def now():
    return datetime.now().ctime()

print >>sys.stderr, "beginning @", now()


    

URL = "http://citeseerx.ist.psu.edu/oai2"

registry = MetadataRegistry()
registry.registerReader('oai_dc', oai_dc_reader)

client = Client(URL, registry)
client.updateGranularity()

store = Store()

if len(sys.argv) > 1:
    start = datetime.strptime(sys.argv[1], '%Y-%m-%d') #2011-10-27, for instance
#elif store.last(): #start with the last day in the database
#    start = store.last() #NOTE: doesn't work due to not returned in right order
else:
    start = client.identify().earliestDatestamp()

#try this and see if it works; if it does resumption tokens right, this should work fine.


chunk = timedelta(days=30)

#TODO: clearly they don't do this whole "ordered" thing. Grab records by month or year or something instead of all at once.
#TODO: luckily, once we've done a full slurp, we only need to remember when the last full slurp was and start since then. But if interrupted, we need to start back from where the last *full* slurp was, due to the ordering problem.

#TODO: structure this better, with the try effectively moved much further above. Really, move a lot more into functions
try:
    current = start #TODO: make a nice little generator so I can use a for loop
    while current <= datetime.now():
        print >>sys.stderr, "fetching records @", now(), "starting with", current.strftime('%Y-%m-%d')
        try:
            records = client.listRecords(metadataPrefix='oai_dc', from_=start, until=(current + chunk))
        except NoRecordsMatchError:
            print >>sys.stderr, "no records for this chunk, continuing to next"
            current += chunk
            continue
        print >>sys.stderr, "record fetch finished @", now()
        for index, (header, metadata, _) in enumerate(records, start=1):
            store.write_record(header, metadata)
            if index == 1 or index % 1000 == 0:
                print >>sys.stderr, "  wrote record", index, "of", header.datestamp().strftime('%Y-%m-%d'), "with id", header.identifier()
        current += chunk
finally:
    print >>sys.stderr, "closing store"
    store.close()
