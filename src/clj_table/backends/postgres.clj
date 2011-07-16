(ns clj-table.backends.postgres
  (:require [clj-table.sql :as sql])
  (:require [clj-table.core :as core]))

(defn sequence-current-val [seqname]
  (let [rows (sql/query "select currval(?)" seqname)]
    (assert (= 1 (count rows)))
    (int (:currval (first rows)))))

(defn sequence-next-val [seqname]
  (let [rows (sql/query "select nextval(?)" seqname)]
    (assert (= 1 (count rows)))
    (int (:nextval (first rows)))))

(defn default-seq-name 
  "given the name of a table, and the name of the (singular) primary key, return the default name of the sequence"
  [table]
  (let [pkey (name (core/single-primary-key table))]
    (str (:name @table) "_" pkey "_seq")))

(defmethod clj-table.core/default-pkey-next-val "PostgreSQL" [table]
  (let [seq-name (or (:sequence-name @table) (default-seq-name table))]
    (sequence-next-val seq-name)))

(defmethod clj-table.core/default-primary-key-func "PostgreSQL" [table row]
  (if (core/has-primary-keys? table row)
    row
    (assoc row (core/single-primary-key table) (clj-table.core/default-pkey-next-val table))))