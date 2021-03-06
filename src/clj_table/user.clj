(ns clj-table.user
  (:import clojure.lang.IPersistentMap clojure.lang.IFn clojure.lang.ISeq)
  (:refer-clojure :exclude [get get-in])
  (:require [clj-table.core :as core])
  (:require clj-table.backends)
  (:use [clj-table.utils :only (subset? make-deftype-map-constructor throwf throw-if-not)]))

;;; The user-visible part of the table API

(declare get get-in)
(def row? core/row?)

(defn get
  "returns the value mapped to col. If column refers to an association and the association is not already loaded, hits the DB"
  [row col]
  (if (contains? row col)
    (clojure.core/get row col)
    (core/row-select-association row col)))

(defn get-in
  [row colseq]
  {:pre [(row? row)]}
  (reduce get row colseq))

(defn row-fn
  "all rows call this fn when used as an IFn"
  [this & [args]]
  (if (coll? args)
    (get-in this args)
    (get this args)))

(def belongs-to core/belongs-to)
(def has-one core/has-one)
(def has-many core/has-many)
(def has-many-through core/has-many-through)

(def load-associations-from-cache core/load-associations-from-cache)

(defmacro deftable 
  "defines a table. A var will be created in the current namespace with name 'varname'. This is the 'table object'. Many functions will take it as an argument.

Required Keys:

   primary-keys - a seq of keywords listing the column names of the primary key(s) of the table
   columns - a seq of keywords listing the column names of the table

Optional Keys
   tablename - the name of the table in the DB. If not specified, assumed to be the same as 'varname'

   primary-key-hook - a fn of two args, the table and the insert map. Called before inserting, only when the row is missing its primary keys. Must return a map containing the primary keys. When the hook is not provided, a default implementation is provided that tries to do the right thing for your DB. 

   pre-insert-hook - a fn of one argument, the map of row attributes. Called before a row is inserted. Returns the updated row. 

   to-db-row-hook - a fn of one argument, called with the where-map before selecting or updating

   from-db-row-hook - a fn of one argument, the row. Must 'update' the row. Called when selecting

   deftable will create a variety of functions in the same namespace. Use (clojure.contrib.ns-utils/docs namespace) to see them all."
  [varname & {:keys [tablename primary-keys columns sequence-name primary-key-hook to-db-row-hook from-db-row-hook to-db-where-hook] :as args}]

  (throw-if-not primary-keys "primary-keys is required")
  (throw-if-not columns " columns is required")
  (throw-if-not (subset? (set primary-keys) (set columns)) "primary keys must be a subset of columns")
  
  (let [varname (symbol varname)
        tablename (str (or tablename varname))
	row-class-name (symbol (str varname "-row"))
	symbol-columns (into [] (map (comp symbol name) columns))
        row-?-name (symbol (str varname "?"))
        table-ns *ns*]
    (assert (> (count tablename) 0))
  `(do
     (declare ~varname)
     (defrecord ~row-class-name [~@symbol-columns] 
       core/Row
       (table [this#]
              (let [rc# @(ns-resolve (quote ~table-ns) (quote ~varname))] ;; resolve, to make sure the record field doesn't shadow the var name
                (throw-if-not (clj-table.core/table? rc#) "expected table, got %s" rc#)
                rc#))
       IFn
       (applyTo [this# #^ISeq seq]
                (apply ~row-fn this# seq))
       (invoke [this# arg#]
               (~row-fn this# arg#)))

     (defonce ~varname (ref 
                        {:name ~tablename 
                         :primary-keys (map keyword (quote ~primary-keys))
                         :columns (map keyword (quote ~columns))
                         :row-deftype ~row-class-name
                         :associations #{}
                         :row-map-constructor (make-deftype-map-constructor ~row-class-name)}))

     (defn ~'primary-keys []
       (:primary-keys @~varname))
     
     (defn ~'find-all 
       "Returns a set of rows from the table.
options: 

  :where clause
     the clause is map of column names to values
     example
      :where {:id 10}

   :order-by
     to order by column foo:
      :order-by :foo
     or
     to order by column foo descending:
      :order-by [:foo :desc]
     
  :load seq-of-association-column-names
     the returned object will also have the association columns attached to the object. If the seq contains a map, loads the association named by the key, and the value is a seq of associations to load on the table named by the key.

   example
    :load [:a :b {:c [:d :e]}]

   :a,:b,:c,:d,:e are all member-names of associations. This will load the associations named by :a and :b in the current table. It will load :c, also on the current table, and then traverse to the table named by :c, and load the associations :d and :e on the rows returned from table :c
"
       [& ~'opts]
       (core/find-all ~varname ~'opts))
     
     (defn ~'find-one
       "options is the same as on find-all. asserts 0 or 1 rows were returned, and returns the single row, or nil. Options is the same as on find-one"
       [& ~'opts] 
       (core/find-one ~varname ~'opts))

     (defn ~'insert 
       "inserts a single row with the given attributes. attrs is a map of column names to values. Returns the inserted row. Pass :select? false to return nil (and avoid an extra select)"
       [~'attrs]
       (core/insert-row ~varname ~'attrs))

     (defn ~'intern-row
       "queries for a row with where-attrs attributes. Returns it if present, else inserts a row with insert attributes. Where-attrs should include the primary key(s) or a unique index, or unpredictable behavior will occur."
       [~'where-attrs & [~'insert-attrs]]
       (or (~'find-one :where ~'where-attrs)
           (~'insert (merge ~'where-attrs ~'insert-attrs))))
     
     (defn ~'insert-many
       "inserts multiple rows. attr-seq is a seq of maps. Returns nil."
       [~'attr-seq]
       (core/insert-rows ~varname ~'attr-seq))
     
     (defn ~'update 
       "updates a single row with the given attributes. set attrs is a map of column names to values. Returns the updated row."
       [~'row ~'set-attrs]
       (core/update-row ~varname ~'row ~'set-attrs))

     (defn ~'update-table
       [~'where-attrs ~'set-attrs]
       (core/update-table ~varname ~'where-attrs ~'set-attrs))
     
     (defn ~'delete-one
       "deletes a single row"
       [~'row]
       (core/delete-row ~varname ~'row))

     (defn ~'delete-many
       [& ~'opts]
       (core/delete-rows ~varname (apply hash-map ~'opts)))
     
     (defn ~'refresh
       "reloads a single row from the DB"
       [~'row]
       (core/refresh-row ~varname ~'row))

     (def ~'get-association clj-table.user/get)
     
     (defn ~'load-associations
       "Ensures the named associations are loaded on this row. Hits the DB if necessary. Returns the updated row"
       [~'row ~'col & ~'cols]
       (println "load-assoc:" ~'col)
       (let [~'row (assoc ~'row ~'col (get ~'row ~'col))]
         (if (seq ~'cols)
           (recur ~'row (first ~'cols) (rest ~'cols))
           ~'row)))

     (let [row-class# (class ((:row-map-constructor @~varname) nil))]
       (defn ~row-?-name 
         "returns true if row is a ~varname"
         [~'row]
         (= (class ~'row) row-class#)))

     (defn ~'columns []
       (:columns @~varname))

     (defn ~'new
       "returns a new instance of the row, but does not insert into the database. Args is a map of column names to values"
       [~'args]
       ((:row-map-constructor @~varname) ~'args)))))


