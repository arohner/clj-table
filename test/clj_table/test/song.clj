(ns clj-table.test.song
  (:require [clj-table.user :as table]))

(table/deftable song
  :primary-keys [:id]
  :columns [:id :name])

