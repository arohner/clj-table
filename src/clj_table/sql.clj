(ns clj-table.sql
  (:require [clojure.contrib.sql :as sql])
  ;; (:use [winston.utils :only (inspect apply-if)])
  (:require [clojure.string :as str])
  (:import java.sql.Statement))

(defmulti escape class)

(defmethod escape :default [x] x)

(defmethod escape String [x]
   (format "$$%s$$" x))

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