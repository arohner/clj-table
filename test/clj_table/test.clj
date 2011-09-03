(ns clj-table.test
  (:import org.postgresql.ds.PGSimpleDataSource)
  (:require [clj-table.user :as table])
  (:require [clojure.java.jdbc :as jdbc])
  (:use clojure.test)
  (:require [clj-table.test.person :as person])
  (:require [clj-table.test.album :as album])
  (:require [clj-table.test.song :as song])
  (:require [clj-table.test.recording :as recording])
  (:require [clj-table.test.composer :as composer])
  (:require [clj-table.test.performance :as performance]))

(def datasource (doto (PGSimpleDataSource.)
                  ;(.setDataSourceName "Table test")
                  (.setServerName "localhost")
                  (.setDatabaseName "table_test")
                  (.setUser "postgres")
                  (.setPassword "")))

(def db {:datasource datasource})

(defn create-test-db []
  (println "creating test DB")
  (jdbc/create-table
   :person
   [:id "serial" "primary key" ]
   [:name "varchar(32)"])
  
  (jdbc/create-table
   :album
   [:id "serial" "primary key"]
   [:name "varchar(32)"]
   [:release_date "varchar(32)"])
  
  (jdbc/create-table
   :song
   [:id "serial" "primary key"]
   [:name "varchar(64)"])
  
  (jdbc/create-table
   :song_composers
   [:song_id "integer" "not null"]
   [:person_id "integer" "not null"])
  
  (jdbc/create-table
   :recording
   [:album_id "integer" "not null"]
   [:song_id "integer" "not null"])
  
  (jdbc/create-table
   :album_performance
   [:album_id "integer" "not null"]
   [:person_id "integer" "not null"]
   [:credit "varchar(32)"]))

(defn drop-test-db []
  (println "dropping test DB")
  (doseq [table [:person :album :song :recording :song_composers :album_performance]]
    (try
     (jdbc/drop-table table)
     (catch Exception e
       (println e)))))

(table/has-many album/album recording/recording {:id :album_id} :recordings)
(table/has-one recording/recording song/song {:song_id :id} :song)

(table/has-many album/album performance/performance {:id :album_id} :performers)
(table/has-many person/person composer/composer {:id :person_id} :compositions)
(table/has-one composer/composer song/song {:song_id :id} :song)
(table/has-many song/song composer/composer {:id :song_id} :composers)
(table/has-many-through album/album song/song :recordings :song :songs)
(table/has-many-through person/person song/song :compositions :song :songs)
(table/belongs-to performance/performance person/person {:person_id :id} :person)
(table/belongs-to performance/performance album/album {:album_id :id} :album)

(defn insert-data []
  (jdbc/with-connection db
    (jdbc/transaction
     (def miles (person/insert {:name "Miles Davis"}))
     (def paul (person/insert {:name "Paul Chambers"}))
     (def jimmy (person/insert {:name "Jimmy Cobb"}))
     (def john (person/insert {:name "John Coltrane"}))
     (def bill (person/insert {:name "Bill Evans"}))
     (def cannonball (person/insert {:name "Cannonball Adderly"}))
    
     (def kind-of-blue (album/insert {:name "Kind of Blue" :release_date "1959"}))

     (def so-what (song/insert {:name "So What"}))
     (def freddie-freeloader (song/insert {:name "Freddie Freeloader"}))
     (def blue-in-green (song/insert {:name "Blue in Green"}))
     (def all-blues (song/insert {:name "All Blues"}))
     (def flamenco-sketches (song/insert {:name "Flamenco Sketches"}))
     (def flamenco-sketches2 (song/insert {:name "Flamenco Sketches (alternate take)"}))
    
     (doseq [person [miles paul jimmy john bill cannonball]]
       (performance/insert {:album_id (:id kind-of-blue) :person_id (:id person)}))
     
     (doseq [song [so-what freddie-freeloader blue-in-green all-blues flamenco-sketches flamenco-sketches2]]
       (recording/insert {:album_id (:id kind-of-blue) :song_id (:id song)})
       (composer/insert {:song_id (:id song) :person_id (:id miles)}))
    
     (composer/insert {:song_id (:id blue-in-green) :person_id (:id bill)}))))

(defn setup-fixture [f]
  (jdbc/with-connection db
    (drop-test-db)
    (create-test-db)
    (insert-data)
    (f)))

(use-fixtures :once setup-fixture)

(deftest test-select
  (let [row (person/find-one :where {:name "Miles Davis"})]
    (is (map? row))
    (is (integer? (:id row)))))

(deftest test-select-many
  (let [result (performance/find-all :where {:album_id (:id kind-of-blue)})]
    (is (coll? result))
    (is (= (count result) 6))))

(deftest test-load
  (let [miles-row (person/find-one :where {:name "Miles Davis"} :load [:compositions])]
    (is (:compositions miles-row))
    (is (= (count (:compositions miles-row)) 6))))

(deftest test-empty-association-returns-empty-col
  (let [cannonball (person/find-one :where {:name "Cannonball Adderly"} :load [:compositions])]
    (is (:compositions cannonball))
    (is (= (count (:compositions cannonball)) 0))))

(deftest test-load-nested
  (let [album-row (album/find-one :where {:name "Kind of Blue"} :load [{:recordings {:song :composers}} :performers])]
    (is (= (count (:recordings album-row)) 6))
    (is (= (count (:performers album-row)) 6))
    (is (-> album-row :recordings first :song :composers))))

(deftest test-get-cached-row
  (let [cache (clj-table.core/build-row-cache [(person/find-all)])]
    (is (= 6 (count (clj-table.core/get-cached-rows person/person nil cache))))))

(deftest test-load-from-cache
  (let [album-row (table/load-associations-from-cache (album/find-one :where {:name "Kind of Blue"})
                                                      :load [{:recordings {:song :composers}} :performers]
                                                      :cache [(recording/find-all) (song/find-all) (composer/find-all) (performance/find-all)])]
    (is (= (count (:recordings album-row)) 6))
    (is (= (count (:performers album-row)) 6))
    (is (-> album-row :recordings first :song :composers))
    (is (= album-row
           (album/find-one :where {:name "Kind of Blue"} :load [{:recordings {:song :composers}} :performers])))))

(deftest test-has-many-through
  (let [miles (person/find-one :where {:name "Miles Davis"} :load [:songs])]
    (is (= 6 (count (:songs miles))))
    (is (= (range 1 7) (sort (map :id (:songs miles)))))))

(deftest test-empty-has-many-through-returns-coll
  ;; loading an empty has-many-through should return an empty collection (rather than nil).
  (let [cannonball (person/find-one :where {:name "Cannonball Adderly"} :load [:songs])]
    (is (coll? (:songs cannonball)))
    (is (= 0 (count (:songs cannonball))))))

(deftest test-compound-val
  (let [rows (person/find-all :where {:name #{"Miles Davis" "Paul Chambers"}})]
    (is (= 2 (count rows)))
    (is (= ["Miles Davis" "Paul Chambers"] (sort-by :name (map :name rows))))))

(deftest test-compound-key
  (let [rows (composer/find-all :where {[:song_id :person_id] [[1 1] [3 5]]})]
    (is (= 2 (count rows)))
    (is (= #{(composer/new {:song_id 1 :person_id 1}) (composer/new {:song_id 3 :person_id 5})} (into #{} rows)))))

(deftest test-order-by
  (let [rows (song/find-all :order-by [:id])]
    (is (= (map :id rows) (range 1 7)))))

(deftest test-order-by-desc
  (let [rows (song/find-all :order-by [:id :desc])]
    (is (= (map :id rows) (reverse (range 1 7))))))

(deftest test-limit
  (let [rows (song/find-all :limit 3)]
    (is (= 3 (count rows)))))

(deftest test-limit-with-order-by
  (let [rows (song/find-all :order-by [:id :desc] :limit 3)]
    (is (= 3 (count rows)))
    (is (= (map :id rows) (reverse (range 4 7))))))

(deftest test-limit-with-association
  (let [people (person/find-all :load [:compositions] :order-by [:id] :limit 1)
        _ (is (= 1 (count people)))
        miles (first people)
        _ (is (= 1 (:id miles)))]
    (is (= 6 (count (:compositions miles))))))

(deftest test-empty-in-works
  ;;; regression test for a SQL syntax error when doing (foo/find-all :where {:id #{}})
  (let [rows (person/find-all :where {:id #{}})]
    (is (coll? rows))
    (is (zero? (count rows)))))

(deftest test-bogus-in-works
    ;;; regression test for doing (foo/find-all :where {:id #{-1 -2 -3}}) returns N nils
  (let [rows (person/find-all :where {:id #{-1 -2 -3 -4 -5}})]
    (is (coll? rows))
    (is (zero? (count rows)))))

(deftest test-range-works-with-incomplete-primary-key
  ;;; do an IN query, on a table with a composite primary key, and don't query on the entire primary key
  (let [miles (person/find-one :where {:name "Miles Davis"})
        rows (composer/find-all :where {:person_id [(:id miles)]})]
    (is (= 6 (count rows)))))