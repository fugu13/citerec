#!/usr/bin/env python
import shelve
import bsddb

raw_db = bsddb.btopen('datastore', 'c')
store = shelve.BsdDbShelf(raw_db)
print store.first()[0]
print store.last()[0]

store.close()
