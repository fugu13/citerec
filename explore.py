#!/usr/bin/env python
import shelve
import bsddb3

raw_db = bsddb3.btopen('datastore', 'c')
store = shelve.BsdDbShelf(raw_db)
print store.first()[0]
print store.last()[0]

store.close()
