(ns clj-table.utils
  (:require [clojure.set :as set]))

(defn ref? [x] (instance? clojure.lang.IRef x))

(defn has-keys?
  "required is a seq of keys. returns true if the map has all keys in required."
  [map required]
  (= (count required) (count (select-keys map required))))

(defn make-deftype-map-constructor 
  "returns a fn that takes a map, and constructs a deftype/record instance
  with the fields supplied. takes the class of the deftype/record" 
  [deftype-class]
  (let [ctor (first (sort-by #(count (.getParameterTypes %)) (.getConstructors deftype-class)))
        fn-arity (count (.getParameterTypes ctor))
        inst (.newInstance ctor (into-array Object (take fn-arity (repeatedly (constantly nil)))))]
    (fn [keyval-map]
      (let [args (keys inst)
            vals (into-array Object (map (fn [arg] (get keyval-map arg)) args))
            extra-keys (set/difference (set (keys keyval-map)) (set args))
            inst (.newInstance ctor vals)]
        (into inst (select-keys keyval-map extra-keys))))))

(defn map-keys 
  "returns a new map with f applied to each of the keys in m. f should
  supply a unique value for each key in map, or duplicate keys will be
  overwritten."
  [f m]
  (into {} (map (fn [[key val]]
                  [(f key) val]) m)))

(defn str->int [x]
  (Integer/parseInt x))

(defn select-vals [map keyseq]
  "returns a seq of the values of map that have keys in keyseq, preserving the order specified in keyseq"
  (for [key keyseq]
    (get map key)))

(defn filter-vals
  "returns a new map containing the key-value pairs where (filter f (vals map)) is true"
  [f m]
  (->> m
       (seq)
       (map (fn [pair]
                  (if (f (val pair))
                    pair
                    nil)))
       (into {})))

(defn apply-if
  "if test (apply f arg args) else arg"
  [test f arg & args]
  (if test
    (apply f arg args)
    arg))

(defn disjoint?
  "returns true if sets a and b share no elements"
  [a b]
  (= 0 (count (set/intersection a b))))

(defn subset?
  "returns true if a is a subset of b"
  [a b]
  {:pre [(set? a) (set? b)] }
  (= (clojure.set/intersection a b) a))