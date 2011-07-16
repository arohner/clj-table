(ns clj-table.test.person
  (:require [clj-table.user :as table]))

(table/deftable person {:primary-keys [:id]
                        :columns [:id :name]})