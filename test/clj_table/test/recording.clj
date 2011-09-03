(ns clj-table.test.recording
  (:require [clj-table.user :as table]))

(table/deftable recording
  :tablename "recording"
  :primary-keys [:album_id :song_id]
  :columns [:album_id :song_id])