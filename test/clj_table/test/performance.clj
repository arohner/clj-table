(ns clj-table.test.performance
  (:require [clj-table.user :as table]))

(table/deftable performance
  :tablename "album_performance"
  :primary-keys [:album_id :person_id]
  :columns [:album_id :person_id])
