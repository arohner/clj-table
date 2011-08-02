(ns clj-table.core
  (:import java.util.regex.Pattern)
  (:require [clojure.set :as set])
  (:use [clojure.contrib.except :only [throwf throw-if-not]])
  (:require [clojure.string :as str])
  (:require [clojure.contrib.sql :as contrib-sql])
  (:require [clojure.contrib.sql.internal :as contrib-sql-internal])
  (:require [clojure.contrib.seq :as seq])

  (:require [clj-table.sql :as sql])
  (:use [clj-table.utils :only (ref? has-keys? make-deftype-map-constructor map-keys str->int select-vals apply-if filter-vals disjoint?)]))
  
(declare find-one find-all)

(defprotocol Row
  (table [row]))

(defn table? [t]
  (and
   (ref? t)
   (map? @t)
   (has-keys? @t [:name :primary-keys :columns :row-deftype :associations :primary-key-hook :to-db-row-hook :from-db-row-hook :to-db-where-hook :row-map-constructor])))

(defn row? 
  "With one argument, returns true if row implements the Row protocol. With two args, returns true if row implements the row protocol and came from table."
  ([row]
     (satisfies? Row row)) 
  ([table row]
     (= (class row) (class ((:row-map-constructor @table) nil)))))

(defn has-primary-keys? 
  "returns true if the row has all its primary keys in place"
  [table row]
  {:pre (map? row)}
  (reduce #(and %&) 
          (vals (select-keys row (:primary-keys @table)))))

(defn single-primary-key
  "asserts that the table has a single primary key column, and then returns the column name"
  [table]
  (assert (= 1 (count (:primary-keys @table))))
  (first (:primary-keys @table)))

(defn primary-keys
  "returns a seq of keywords, the columns for the primary keys of the table"
  [row]
  {:pre [(row? row)]
   :post [(seq %)
          (every? keyword? %)]}
  (:primary-keys @(table row)))

(defn primary-keys-table [table]
  (:primary-keys @table))

(defn primary-key-map [table row]
  (select-keys row (primary-keys-table table)))

(defn- init-row
  "do all necessary setup after loading a raw row out of the DB."
  [table raw-row]
  {:pre [(map? raw-row)]
   :post [(row? table %)]}

  (let [row ((:row-map-constructor @table) raw-row)]
    (assert (row? row))
    (if-let [hook (:from-db-row-hook @table)] 
      (hook row)
      row)))

(defn table-without-schema [table-name]
  {:pre [table-name
         (string? table-name)]}
  (last (re-find #"(.+\.)?(.+)" table-name)))

(defn mangle-table-name [table-name idx]
  (str  (table-without-schema table-name) "_" idx))

(defn mangle-col-name [mangled-table-name col-name]
  {:pre [(string? mangled-table-name)
         (keyword? col-name)]}
  (str mangled-table-name "_" (name col-name)))

(defn alias-col-name [mangled-table-name col-name]
  {:pre [(string? mangled-table-name)
         (keyword? col-name)]}
  (str mangled-table-name "." (name col-name) " AS " (mangle-col-name mangled-table-name col-name)))

(defn alias-table-name [table-name idx]
  (str table-name " AS " (mangle-table-name table-name idx)))

(defn select-cols [col-seq]
  (str/join ", " col-seq))

(defrecord association [type src-table tgt-table member-name singular?])

(def association-constructor (make-deftype-map-constructor association))

(defn association? [a]
  (instance? association a))

(defrecord join [src-table tgt-table column-map])

(def new-join (make-deftype-map-constructor join))

(defn join? [j]
  (instance? join j))

(defn find-association
  "returns the assocation map with the specified column name"
  [table column]
  {:pre [(ref? table)
         (keyword column)]
   :post [(association? %)]}
  (if-let [a (first (filter (fn [a]
                              (= (:member-name a) column)) (:associations @table)))]
    a
    (throwf "could not find association %s on table %s" column (:name @table))))

(defmulti association-joins
  "returns a seq of join objects"
  :type)

(defmethod association-joins :default [association]
  [(new-join (merge association {:columns (:columns @(:tgt-table association))}))])

(defmethod association-joins :has-many-through [association]
  (let [src-table (:src-table association)
        src-join-association (find-association src-table (:join-relation association))
        join-table (:tgt-table src-join-association)
        join-target-association (find-association join-table (:target-relation association))]
    [(new-join (merge src-join-association {:columns nil})) (new-join (merge join-target-association {:columns (:columns @(:tgt-table join-target-association))}))]))

(defn order-by-fragment
  "takes the name of a column, or a vector containing the name of a column and :desc"
  [m-table-name order-by]
  (assert (or (keyword? order-by)
              (and (vector? order-by) (every? keyword order-by))))
  (if (keyword? order-by)
    (str " ORDER BY "  m-table-name "." (name order-by))
    (let [[col desc] order-by]
      (when desc
        (assert (= desc :desc)))
      (str " ORDER BY " m-table-name "." (name col) (when (second order-by) " DESC")))))

(defn join-column [mangled-src-table mangled-tgt-table src-col tgt-col]
  (str mangled-src-table "." (name src-col) "=" mangled-tgt-table "." (name tgt-col)))

(defn outer-join-fragment [join]
  (let [{:keys [src-table tgt-table column-map]} join
        src-table @src-table
        tgt-table @tgt-table
        tgt-table-name (:name tgt-table)
        src-table-name (:name src-table)
        mangled-tgt-table (mangle-table-name (:name tgt-table) (:tgt-table-idx join))
        mangled-src-table (mangle-table-name (:name src-table) (:src-table-idx join))]
    (assert (>= (count column-map) 1))
    (str "LEFT OUTER JOIN " tgt-table-name " AS " mangled-tgt-table " ON "
         (sql/and-separated-list (map (fn [[src-col tgt-col]]
                                        (join-column mangled-src-table mangled-tgt-table src-col tgt-col)) column-map)))))

(defn expand-where-map
  [mangled-table-name where]
  (let [mangle (fn [key]
                 (keyword (str mangled-table-name "." (name key))))]
    (map-keys (fn [x]
                (if (coll? x)
                  (map mangle x)
                  (mangle x))) where)))

(defn select-outer-join-query-str
  [table & {:keys [where joins order-by limit] :as args}]
  {:post [(string? %)]}
  ;; it's very weird to combine joins with limit, you
  ;; probably won't get what you want.
  (assert (not (and (seq joins) limit))) 
  (let [table-name (:name @table)
        m-table-name (mangle-table-name table-name 1)
        cols (select-cols (concat (for [col (:columns @table)]
                                    (alias-col-name m-table-name col))
                                  (for [j joins
                                        col (:columns @(:tgt-table j))]
                                    (alias-col-name (mangle-table-name (:name @(:tgt-table j)) (:tgt-table-idx j)) col))))
        query-str (str "SELECT " cols
                       " FROM " (alias-table-name table-name 1) " " (str/join " " (map outer-join-fragment joins))
                       (when (seq where) (str " WHERE " (sql/where-map->str (expand-where-map m-table-name where))))
                       (when order-by (order-by-fragment m-table-name order-by))
                       (when limit (str " LIMIT " limit)))]
    query-str))

(defn split-mangled-col [col]
  {:pre [(keyword? col)]
   :post [(map? %)]}
  (let [match (re-find #"(.+_\d+)_(.+)" (name col))]
    (assert match)
    (assert (= (count match) 3))
    {:table (match 1)
     :col (match 2)}))

;;; Git rebase is broken and must have changes in order to resume.  These are those changes.

(defn lookup-table [tables name]
  {:pre [(string? name)
         (coll? tables)
         (every? ref? tables)
         (>= (count tables) 1)]
   :post [(ref? %)]}
  (let [idx (->> (re-find #"_(\d+)$" name)
                 (second)
                 (str->int))]
    (assert idx)
    (first (filter (fn [table]
                   (= name (mangle-table-name (:name @table) idx))) tables))))

(defn fast-group-by
  "like c.c.seq/group-by, but returns an unsorted map, so transients can be used"
  [f coll]
  
  (persistent! (reduce
                (fn [r x]
                  (let [k (f x)]
                    (assoc! r k (conj (clojure.core/get r k []) x))))
                (transient {}) coll)))

(defn transient-map? [x]
  (instance? clojure.lang.ITransientMap x))

(defn transient-set? [x]
  (instance? clojure.lang.ITransientSet x))

(defn get-row-order [table result-set]
  (let [pkeys (:primary-keys @table)
        pkey (if (= 1 (count pkeys))
                (first pkeys)
                pkeys)
        m-table-name (mangle-table-name (:name @table) 1)
        m-cols (if (coll? pkey)
                 (map #(keyword (mangle-col-name m-table-name %)) pkeys)
                 (keyword (mangle-col-name m-table-name pkey)))]
    {pkey (distinct (map (fn [row]
                            (if (coll? pkey)
                              (select-vals row m-cols)
                              (get row m-cols))) result-set))}))

(defn group-outer-join-rows
  [result-set]
  {:post [(or (map? %) (nil? %))
          (every? string? (keys %))
          (every? coll? (vals %))]}
   (let [table-name (memoize (fn table-name [col-string]
                               (:table (split-mangled-col col-string))))
         col-name (memoize (fn col [col-string]
                             (keyword (:col (split-mangled-col col-string)))))
         cleanup-row (memoize (fn cleanup-row [row]
                       {:post [(map? %)]}
                       (map-keys col-name row)))
         row-set (for [row result-set]
                   (let [multi-row row
                         multi-row (fast-group-by (fn [pair]
                                                    (table-name (key pair))) multi-row)]
                     (persistent! (reduce (fn [row-map [table row]]
                                            (let [row (cleanup-row row)
                                                  row (if (every? nil? (vals row)) nil row)]
                                              (if row
                                                (assoc! row-map table (conj (or (row-map table) []) row))
                                                row-map)))
                                          (transient {}) multi-row))))]
     (apply merge-with set/union row-set)))

(defn build-row-cache
  "builds a row cache from a seq of calls to query functions.

   example: (build-row-cache [(foo/find-all) (bar/find-all)]) "
  [results-seq]
  {:pre [(every? set? results-seq)
         (every? #(every? row? %) results-seq)]
   :post [(ref? %)
          (every? table? (keys @%))
          (every? coll? (vals @%))]}
  (let [row (first (first results-seq))]
    (assert (or (row? row) (nil? row))))
  
  (reduce (fn [cache set]
            (when (seq set)
              (dosync
               (alter cache assoc (table (first set)) {:rows set
                                                       :indexed-cols #{}
                                                       :index nil})))
            cache) (ref {}) results-seq))

(defn reindex [_ rows cols]
  (apply merge (for [col cols]
                 (clojure.set/index rows col))))

(defn add-index [cache table col]
  (dosync (alter cache update-in [table :indexed-cols] conj col)))

(defn ensure-index
  "ensures cache has an index for col on table, adds it if not present"
  [cache table col-seq]
  {:pre [(ref? cache)]}
  (when (not (get-in @cache [table :indexed-cols col-seq]))
    (dosync
     (alter cache update-in [table :indexed-cols] conj col-seq)
     (let [cols (get-in @cache [table :indexed-cols])
           rows (get-in @cache [table :rows])]
       (alter cache update-in [table :index] reindex rows cols))))
  cache)

(declare get-cached-row)

(defn split-compound-where-val
  "handles the {:id #{1 2 3}} case"
  [where key]
  (let [vals (get where key)]
    (assert (coll? vals))
    (map #(assoc where key %) vals)))

(defn split-compound-where-key
  "handles {[:foo :bar] [[1,2][3,4][5,6]]}"
  [where key]
  (let [val-seq (get where key)]
    (assert (coll? key))
    (assert (coll? val-seq))
    (map (fn [vals]
           (assert (= (count vals) (count key)))
           (-> where
               (dissoc key)
               (merge (into {} (map #(vector %1 %2) key vals))))) val-seq)))

(defn first-compound-where [where]
  (->> where
       (filter (fn [[key val]]
            (or (coll? key)
                (coll? val))))
       (first)))

(defn split-compound-where
  "given a where clause that contains an IN, like {:id #{1 2 3}} or
  {[:foo :bar] [[1,2] [3,4]]}, returns a seq of where clauses, ({:id
  1} {:id 2}, {:id 3}) or ({:foo 1 :bar 2}, {:foo 3, :bar 4})"
  [where]
  (if-let [[key val] (first-compound-where where)]
    (let [clauses (if (coll? key)
                    (split-compound-where-key where key)
                    (split-compound-where-val where key))]
      (mapcat split-compound-where clauses))
    [where]))

(defn get-cached-rows [table where cache]
  {:pre [(table? table)]}
  (if where
    (if (first-compound-where where)
      (filter identity (mapcat #(get-cached-rows table % cache) (split-compound-where where)))
      (do
        (ensure-index cache table (keys where))
        (get-in @cache [table :index where] #{})))
    (get-in @cache [table :rows] #{})))

(defn get-cached-row [table where cache]
  (let [rows (get-cached-rows table where cache)]
    (assert (<= (count rows) 1))
    (first rows)))

(defn new-association [& pairs]
  {:post (association? %)}
  (let [a (association-constructor (apply hash-map pairs))]
    (when (not (association? a))
      (throwf "invalid association: " a))
    a))

(defmulti row-select-association
  "loads rows for the association from the DB, and returns them."
  (fn [row column]
    (let [table (table row)
          a (find-association table column)]
      (assert a)
      (:type a))))

(defn get-association
  "returns a map of the table and where clause needed to select the associated rows"
  [association src-row]
  {:pre [(association? association)]
   :post [(or (map? %) (nil? %))]}
  (let [col-map (:column-map association)
        src-table (:src-table association)
        target-table (:tgt-table association)
        src-pairs (select-keys src-row (keys col-map))]
    (when (some identity (vals src-pairs))
      (let [target-attrs (map-keys #(clojure.core/get col-map %) src-pairs)]
        {:table target-table :where target-attrs}))))

(defmethod row-select-association :default [row column]
  {:pre [(row? row)]}
  (let [table (table row)
        a (find-association table column)
        {tgt-table :table where :where} (get-association a row)
        find-f (if (:singular? a) find-one find-all)]
    (assert a)
    (when tgt-table where
          (find-f tgt-table [:where where]))))

(defn- add-association [table a]
  (dosync
     (alter table update-in [:associations] conj a)))

(defn belongs-to 
  "declares a relationship between two tables, src and target. Looks in the src row for columns (keys column-map). Attaches *one* row from target with columns (vals column-map) with the name member-name. the target column should point at the primary key(s) of the target table "
  [src-table tgt-table column-map member-name & opts]
  (add-association src-table (new-association :type :belongs-to
                                              :src-table src-table
                                              :tgt-table tgt-table
                                              :column-map column-map
                                              :member-name member-name
                                              :singular? true)))

(defn has-one 
  "declares a relationship between two tables, src and target. Looks in the target row for columns (keys column-map)
"
  [src-table tgt-table column-map member-name]
  (add-association src-table (new-association :type :has-one
                                              :src-table src-table
                                              :tgt-table tgt-table
                                              :column-map column-map
                                              :member-name member-name
                                              :singular? true))
  nil)

(defn has-many 
  "Declares a relationship between two tabls, src and target. Looks in the src row for columns (keys column-map). Joins multiple rows from tgt table. Will add a member to src-row, member-name, containing all rows where src-table.(keys column-map) = tgt-table.(vals column_map)"
  [src-table tgt-table column-map member-name & [options]]
  (dosync
   (let [association (new-association :type :has-many
                                      :src-table src-table
                                      :tgt-table tgt-table
                                      :column-map column-map
                                      :member-name member-name
                                      :singular? false)]
     (add-association src-table association)
     (alter tgt-table update-in [:index-cols] conj (vals column-map))
     nil)))

(defn has-many-through
  "Declares a relationship between two tables, src and target, that are joined by a join table. join-relation is the name of the association from the source to join table. target-relation is the name of the association from the join to the target-table."
  [src-table tgt-table join-relation target-relation member-name]
  (dosync
   (let [association (new-association :type :has-many-through
                                      :src-table src-table
                                      :tgt-table tgt-table
                                      :join-relation join-relation
                                      :target-relation target-relation
                                      :member-name member-name
                                      :singular? false)]
     (add-association src-table association)
     nil)))

(defmethod row-select-association :has-many-through [row column]
  {:pre [(row? row)]}
  (let [table (table row)
        a (find-association table column)
        join-a (find-association table (:join-relation a))
        {join-table :table join-where :where :as args} (get-association join-a row)]
    (assert a)
    (when join-table join-where
          (let [join-rows (find-all join-table {:where join-where
                                                :load (:target-relation a)})]
            (if (:singular? join-a)
              (map :member-name join-rows)
              (mapcat :member-name join-rows))))))

(defmulti row-get-cached-association
    "returns the rows from the association corresponding to the column, using the cache"
  (fn [row column cache]
    (let [table (table row)
          a (find-association table column)
          _ (assert a)]
      (:type a))))

(defmethod row-get-cached-association :default [row column cache]
  {:pre [(row? row)]}
  {:post [(coll? (get row column))]}
  (let [table (table row)
        a (find-association table column)
        _ (assert a)
        {tgt-table :table where :where :as args} (get-association a row)]
    (when tgt-table
      (let [f (if (:singular? a) get-cached-row get-cached-rows)]
        (f tgt-table where cache)))))

(defmethod row-get-cached-association :has-many-through
  [row column cache]
  {:pre [(row? row)]
   :post [{:post [(coll? (get row column))]}]}
  (let [table (table row)
        a (find-association table column)
        _ (assert a)
        src-join (find-association table (:join-relation a))
        _ (assert src-join)
        join-table (:tgt-table src-join)
        _ (assert join-table)
        tgt-join (find-association join-table (:target-relation a))
        _ (assert tgt-join)
        join-rows (row-get-cached-association row (:join-relation a) cache)
        tgt-singular? (:singular? tgt-join)
        _ (assert (not (nil? tgt-singular?)))]
    (if tgt-singular?
      (doall (map #(row-get-cached-association % (:target-relation a) cache) join-rows))
      (doall (mapcat #(row-get-cached-association % (:target-relation a) cache) join-rows)))))

(defn row-assoc-association
  [row column cache]
  (assoc row column (row-get-cached-association row column cache)))

(defn load-nested-associations
  ""
  [row assoc-col-seq cache]
  {:pre [(row? row)]
   :post [(row? %)]}
  (when (not (row? row))
    (throwf "load-nested: expected row, got %s %s" (type row) row))
  (let [x assoc-col-seq]
    (cond
     (keyword? x) (row-assoc-association row x cache)
     (map? x) (reduce (fn [row [assoc-col children]]
                        (let [assoc-rows (row-get-cached-association row assoc-col cache)]
                          (cond
                           (row? assoc-rows) (assoc row assoc-col (load-nested-associations assoc-rows children cache))
                           (seq assoc-rows) (assoc row assoc-col (doall (map #(load-nested-associations % children cache) assoc-rows)))
                           :else (assoc row assoc-col #{})))) row x)
     (coll? x) (reduce (fn [row assoc-col]
                         (load-nested-associations row assoc-col cache)) row x))))

(defn print-associations [as]
  (println (count as) "associations:")
  (doseq [a as]
    (println (str (:name @(:src-table a)) "." (name (:member-name a)) " "))))

(defmulti index-joins
  "adds src-table-idx, tgt-table-idx to a seq of joins."
  (fn [association joins src-table-idx idx-atom]
    (:type association)))

(defmethod index-joins :default [association joins src-table-idx idx-atom]
  (assert (= 1 (count joins)))
  (let [join (first joins)]
    [(assoc join
       :src-table-idx src-table-idx
       :tgt-table-idx (swap! idx-atom inc))]))

(defmethod index-joins :has-many-through [association joins src-table-idx idx-atom]
  (let [j (first joins)
        tgt-idx (swap! idx-atom inc)
        joins (rest joins)
        j (assoc j
            :src-table-idx src-table-idx
            :tgt-table-idx tgt-idx)]
    (if (seq joins)
      (concat [j] (index-joins association joins tgt-idx idx-atom))
      [j])))

(defn find-nested-associations
  "returns a flattened list of all associations given a nested col-seq"
  ([table assoc-col-seq]
     (find-nested-associations table 1 assoc-col-seq (atom 1)))
  ([table table-idx assoc-col-seq idx-atom]
     {:post [(do (coll? %))
             (every? join? %)]}
     (cond
      (keyword? assoc-col-seq) (let [a (find-association table assoc-col-seq)
                                     joins (association-joins a)]
                                 (index-joins a joins table-idx idx-atom)) 
      (map? assoc-col-seq) (reduce (fn [ret-list [table-keyword new-col-seq]]
                                     (let [[tgt-assoc] (find-nested-associations table table-idx table-keyword idx-atom)
                                           tgt-table (:tgt-table tgt-assoc)]
                                       (when (not tgt-table)
                                         (throwf "couldn't find association %s on table %s" table-keyword (:name @table)))
                                       (concat ret-list [tgt-assoc] (find-nested-associations tgt-table (:tgt-table-idx tgt-assoc) new-col-seq idx-atom))))
                                   [] assoc-col-seq)
      (coll? assoc-col-seq) (reduce (fn [ret-list item]
                                      (concat ret-list (find-nested-associations table table-idx item idx-atom))) [] assoc-col-seq)

      :else (throwf "unhandled type: %s" assoc-col-seq))))

(def inst-col-seq [:comments {:instance-attributes {:attribute :allowed-enums}
                              :dataset [:instances :attributes]}])

(defn str-cache [row-cache]
  (for [[table row-set] row-cache]
    (str (count row-set) " in " (:name @table))))

(defn print-cache [row-cache]
  (doseq [[table row-set] row-cache]
    (printf "in table %s\n %s\n" (:name @table) (:rows row-set))))

(defn order-rows [rows order-by]
  (assert (coll? order-by))
  (assert (contains? #{1 2} (count order-by)))
  (let [sort-col (first order-by)
        sort-dir (or (nth order-by 1) :asc)
        rows (sort-by sort-col rows)]
    (apply-if (= :desc sort-dir) reverse rows)))

(defn load-associations-from-cache
  "Loads associations onto rows. Takes two keys, :load is a standard load clause. :cache is a seq of of calls to find-one, find-all, or the result of a call to build-row-cache"
  [rows & opts]
  (let [singular-row? (row? rows)
        rows (if singular-row? [rows] rows)
        opts (apply hash-map opts)
        {:keys [cache load order-seq]} opts
        cache (if (ref? cache) cache (build-row-cache cache))
        rows (doall (for [row rows]
             (if load
               (load-nested-associations row load cache)
               row)))]
    (if singular-row?
      (first rows)
      rows)))

(defn print-row-map [row-map]
  (println "row-map: ")
  (doseq [[table set] row-map]
    (println (:name @table) set)))

(declare select)

(defn select-ids [table & {:keys [where order-by limit] :as query-args}]
  (let [rows (select table :where where :order-by order-by :limit limit)
        primary-key (:primary-keys @table)
        primary-key (if (= 1 (count primary-key))
                      (first primary-key)
                      primary-key)
        where (apply merge-with conj {primary-key []}
                     (map (fn [row]
                            {primary-key (if (coll? primary-key)
                                           (into [] (select-vals row primary-key))
                                           (get row primary-key))}) rows))]
    (-> query-args
        (dissoc :limit)
        (assoc :where where))))

(defn select [table & {:keys [where load order-by limit]}]
  {:pre [(table? table)]}
  "where is a map of attrs to values. load a seq of (potentially nested) column names"
  (throw-if-not (table? table))
  (let [original-where where
        mangled-table-name (mangle-table-name (:name @table) 1)
        joins (if load (find-nested-associations table load) [])
        where-hook (or (:to-db-where-hook @table) identity)
        where (where-hook where)
        {:keys [where order-by limit]} (if (and (seq joins) limit)
                                         (select-ids table :where where :order-by order-by :limit limit)
                                         {:where where :order-by order-by :limit limit})
        mangled-where (expand-where-map mangled-table-name where)
        query (select-outer-join-query-str table :where where :joins joins :order-by order-by :limit limit)]
    (clojure.contrib.sql/with-query-results rows (apply vector query (sql/where-map-prepared-values where))
      (let [row-map (group-outer-join-rows rows)
            all-tables (concat [table] (map :tgt-table joins))
            row-map (reduce (fn [row-map [table new-set]]
                              (update-in row-map [(lookup-table all-tables table)] ;;; If a table is joined more than once, it could have multiple table names, foo_1, foo_2, etc. Consolidate those.
                                         (fn [existing-set]
                                           (set/union new-set (or existing-set #{}))))) {} row-map)
            row-map (into {} (map (fn [[table row-set]]
                                    [table (set (map #(init-row table %) row-set))]) row-map))
            where original-where
            row-order-where (if order-by
                              (get-row-order table rows)
                              nil)
            cache (build-row-cache (vals row-map))
            rows (get-cached-rows table (if order-by
                                          row-order-where
                                          where) cache)]
        (assert row-map)
        (load-associations-from-cache rows :load load :cache cache)))))

(defn map-to-row [table m]
  ((:row-map-constructor @table) (select-keys m (:columns @table))))

(defn prepare-insert 
  "do all necessary stuff before inserting a row in the DB"
  [table row]
  {:pre [(map? row)]
   :post [(has-primary-keys? table %)]}
  (let [row (map-to-row table row)
        pkey-hook (if (has-primary-keys? table row)
                    identity
                    #((:primary-key-hook @table) table %))
        pre-insert-hook (or (:to-db-row-hook @table) identity)]
    (->
     row
     pkey-hook
     pre-insert-hook)))

(defn assert-primary-keys [row]
  (throw-if-not (row? row) "%s is not a row" row)
  (throw-if-not (table row))
  (throw-if-not (has-primary-keys? (table row) row) "table %s: primary-keys are %s, got %s" (:name @(table row)) (primary-keys row) (select-keys row (primary-keys row)))
  true)

(defn insert-row
  [table attrs {:keys [select?] :or {:select? true}}]
  {:pre [(map? attrs)]
   :post [(if select?
            (assert-primary-keys %)
            (nil? %))]}
  (let [row (prepare-insert table attrs)
        row (filter-vals #(not (nil? %)) row)
        from-db-hook (or (:from-db-row-hook @table) identity)]
    (->> row
     (sql/insert (:name @table))
     (map-to-row table))))

(defn insert-rows [table attr-seq]
  (doseq [row attr-seq]
    (insert-row table row)))

(defn delete [table where-map]
  {:pre [(map? where-map)]}
  (contrib-sql/delete-rows (:name @table) (apply vector (sql/where-map->str where-map) (sql/where-map-prepared-values where-map))))

(defn delete-row [table row]
  {:pre [(row? row)]}
  (let [where-map (primary-key-map table row)]
    (delete table where-map)))

(defn delete-rows
  ([table opts]
     {:pre [(map? opts)]}
     (let [where-map (:where opts)]
       (assert (map? where-map))
       (delete table where-map)))
  ([table]
     (contrib-sql/delete-rows (:name @table) ["true"])))

(defn update-table [table where-attrs set-attrs]
  (let [hook (or (:to-db-where-hook @table) identity)]
    (sql/update table (hook where-attrs) set-attrs)))

(defn row-select-keys [row keyseq]
  (select-keys @row keyseq))

(defn get-connection-backend []
  (-> (contrib-sql/connection) .getMetaData .getDatabaseProductName))

(defmulti default-pkey-next-val (fn [table] (get-connection-backend)))
(defmulti default-primary-key-func (fn [table row] (get-connection-backend))) 

(defn find-all [table opts]
  (let [opts (apply hash-map opts)
        where (:where opts)
        load (:load opts)]
    (select table :where where :load load :order-by (:order-by opts) :limit (:limit opts))))

(defn find-one
  [table opts]
  {:post [(or (row? %) (nil? %))]}
  (let [rc (find-all table opts)]
    (when (> (count rc) 1)
      (throwf "expected 0 or 1 results, got %s for table %s %s" (count rc) (:name @table) opts))
    (first rc)))

(defn refresh-row [table row]
  {:pre [(map? row)
         (has-primary-keys? table row)]
   :post [(= (primary-key-map table row) (primary-key-map table %))]}
  (find-one table [:where (primary-key-map table row)]))

(defn update-row [table row attrs]
  {:pre [(map? attrs)
         (row? row)]}
  (when (not (has-primary-keys? table row))
    (throwf "primary keys must be present"))
  (when (not (disjoint? (set (keys attrs)) (set (primary-keys row))))
    (throwf "can't use update-row to modify primary keys"))
  (sql/update table (primary-key-map table row) attrs)
  (refresh-row table row))