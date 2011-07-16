(ns clj-table.test.album
  (:require [clj-table.user :as table]))

(table/deftable album {:primary-keys [:id]
                       :columns [:id :name :release_date]})