Introduction
============
clj-table is an ORM for SQL databases. It's still new and isn't quite fully-featured, but it's useful for me. 

Currently, it only works with Postgres, because that's what I use. Patches welcome!

Usage
=====

Require clj-table.user, then call deftable

    (ns clj-table.test.person
      (:require [clj-table.user :as table])
    
    (table/deftable person {:primary-keys [:id]
                            :columns [:id :name]})

You have to manually specify primary keys and columns right now, no table introspection yet. This will define a var named person in the current ns, and bunch of functions, so you can only deftable once per ns. For the complete list of functions defined, do `(clojure.contrib.ns-utils/docs <ns where you just deftable'd)` 

clj-table is built on clojure.contrib.sql for connectivity, so you'll need a c.c.sql connection:

find-one
--------
    (clojure.contrib.sql/with-connection db
        (person/find-one :where {:id 10}))

Returns a single row, or nil. Find-one asserts that it returns at most one row.

find-all
--------
    (clojure.contrib.sql/with-connection db
        (person/find-one :where {:gender "m"}))

where clauses
-------------
Normally, the where clause queries on strict equality, i.e. `:where {:id 10}` generates the sql for `["where id = ?" 10]`. The where clause can also take a set, to generate an IN clause:

    (clojure.contrib.sql/with-connection db
        (person/find-all :where {:id #{1 42 13}}))
 
performs a query for `..WHERE id IN (1, 42,13)..`

order-by
--------
    (person/find-all :order-by [:id])
    (song/find-all :order-by [:id :desc])

limits
------
    (song/find-all :limit 3)

Associations
============
clj-table supports Rails style associations between tables.
After defining two tables:

    (ns clj-table.test
      (:require [clj-table.test.album :as album])
      (:require [clj-table.test.recording :as recording]))

    (table/has-many album/album recording/recording {:id :album_id} :recordings)

has-many takes two vars defined by deftable, a map of source column to dest column, and the name of the association that will appear on the source column. Then you can do

    (clj-table.test.album/find-one :where {:name "Kind of Blue"} :load [:recordings])

This will return one album row, with an extra column, recordings. Since :recordings was defined as `has-many`, it will be a set of recordings rows. If `has-one` is used instead, the column will point to a single row, or nil.

Current associations are `has-one`, `has-many`, `belongs-to` and `has-many-through`.

For more examples, see test/clj-table/test.clj

License
=======
Licensed under the EPL, the same as Clojure