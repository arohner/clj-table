(ns clj-table.sql
  (:require [clojure.contrib.sql :as sql])
  ;; (:use [winston.utils :only (inspect apply-if)])
  (:require [clojure.string :as str])
  (:import java.sql.Statement))

(defmulti escape class)

(defmethod escape :default [x] x)

(defmethod escape String [x]
   (format "$$%s$$" x))

(defn execute
  "Executes an (optionally parameterized) SQL prepared statement on the
  open database connection. Each param-group is a seq of values for all of
  the parameters.

  This is the same as c.c.sql/do-prepared, but returns the generated keys. Uses .execute rather than .executeBatch because .executeBatch + returnGeneratedKeys is broken with postgresql."
  [sql & param-groups]
  (with-open [stmt (.prepareStatement #^java.sql.Connection (sql/connection) sql Statement/RETURN_GENERATED_KEYS)]
    (doseq [param-group param-groups]
      (doseq [[index value] (map vector (iterate inc 1) param-group)]
        (.setObject stmt index value))
      (.addBatch stmt))
    (sql/transaction
     (.execute stmt)
     (-> stmt (.getGeneratedKeys) (resultset-seq) (doall)))))

(defn insert-values
  "Inserts rows into a table with values for specified columns only.
  column-names is a vector of strings or keywords identifying columns. Each
  value-group is a vector containing a values for each column in
  order. When inserting complete rows (all columns), consider using
  insert-rows instead.

  Same as c.c.sql/insert-values, but calls our execute rather than c.c.sql/do-prepared"
  [table column-names & value-groups]
  (let [column-strs (map name column-names)
        n (count (first value-groups))
        template (apply str (interpose "," (replicate n "?")))
        columns (if (seq column-names)
                  (format "(%s)" (apply str (interpose "," column-strs)))
                  "")]
    (apply execute
           (format "INSERT INTO %s %s VALUES (%s)"
                   (name table) columns template)
           value-groups)))

(defn insert-rows
  "Inserts complete rows into a table. Each row is a vector of values for
  each of the table's columns in order."
  [table & rows]
  (apply insert-values table nil rows))

(defn insert-records
  "Inserts records into a table. records are maps from strings or
  keywords (identifying columns) to values."
  [table & records]
  (doall (apply concat
                (for [record records]
                  (insert-values table (keys record) (vals record))))))

(defn insert
  "insert a single row"
  [table attrs]
  (first (insert-records table attrs)))

(defn comma-separated [coll]
  (str/join ", " coll))

(defn comma-separated-list [coll]
  (format "(%s)" (comma-separated coll)))

(defn and-separated-list [coll]
  (str/join " AND " coll))

(defmulti handle-value type)

(defmethod handle-value clojure.lang.IPersistentCollection [x]
  (comma-separated-list x))

(defmethod handle-value String [x]
  (format "'%s'" x))

(defmethod handle-value :default [x]
  (str x))

(defn where-val-coll->str [val-coll]
  (if (seq val-coll)
    (format "ARRAY[%s]" (comma-separated (map handle-value val-coll)))
    "null"))

(defn where-val->str [val & {:keys [literal]}]
  (cond
   (coll? val) (format " = ANY(%s) " (where-val-coll->str val))
   literal (str "= " (escape val))
   :else (str "= ?")))

(defn where-key->str [key]
  (if (coll? key)
    (format "(%s)" (str/join "," (map name key)))
    (name key)))

(defn where-map->str
  [where-map & {:keys [literal]}]
  (and-separated-list (map (fn [[key val]]
                             (str (where-key->str key) (where-val->str val :literal literal))) where-map)))

(defn where-map-prepared-values
  "returns the seq of values that should show up in the prepared
  statement. i.e. filters out the values that shouldn't show up,
  because they're literals in the query because they can't belong in a
  prepared statement"
  [where-map]
  (filter #(not (coll? %)) (vals where-map)))

(defn update [table where-map set-map]
  (sql/update-values (:name @table) (apply vector (where-map->str where-map) (vals where-map)) set-map))

(defn query [sql & args]
  (sql/with-query-results results (into [] (concat [sql] args))
    (seq (doall results))))