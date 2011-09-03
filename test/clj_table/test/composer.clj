(ns clj-table.test.composer
  (:require [clj-table.user :as table]))

(table/deftable composer
  :tablename "song_composers"
  :primary-keys [:song_id :person_id]
  :columns [:song_id :person_id])