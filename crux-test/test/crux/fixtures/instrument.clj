(ns crux.fixtures.instrument
  (:require [crux.db :as db]
            [crux.index :as i]
            [crux.memory :as mem]
            [clojure.tools.logging :as log]))

(def ansi-colors {:white   "[37m"
                  :red     "[31m"
                  :green   "[32m"
                  :blue    "[34m"
                  :yellow  "[33m"
                  :magenta "[35m"})

(defn- in-ansi [c s]
  (if c
    (str "\u001b" (get ansi-colors c) s "\u001b[0m")
    s))

(defmulti index-name (fn [i] (-> i ^java.lang.Class type .getName symbol)))

(defmethod index-name :default [i]
  (-> i ^java.lang.Class type .getName (clojure.string/replace #"crux\.index\." "") symbol))

(defmethod index-name 'crux.index.NAryConstrainingLayeredVirtualIndex [i]
  (str "NAry-Constrained: " (clojure.string/join " " (map :name (:indexes i)))))

(defmethod index-name 'crux.index.NAryJoinLayeredVirtualIndex [i]
  (str "NAry: " (clojure.string/join " " (map :name (:indexes i)))))

(defmethod index-name 'crux.index.UnaryJoinVirtualIndex [i]
  (str "Unary: " (clojure.string/join " " (map :name (:indexes i)))))

(defmethod index-name 'crux.index.BinaryJoinLayeredVirtualIndex [i]
  (format "Binary: [%s %s %s]" (-> i meta :clause :e) (-> i meta :clause :a) (-> i meta :clause :v)))

(defmethod index-name 'crux.index.DocAttributeValueEntityEntityIndex [i]
  "AVE-E:")

(defmethod index-name 'crux.index.DocAttributeValueEntityValueIndex [i]
  "AVE-V:")

(defmethod index-name 'crux.index.DocAttributeEntityValueEntityIndex [i]
  "AVE-E:")

(defmethod index-name 'crux.index.DocAttributeEntityValueValueIndex [i]
  "AEV-V:")

(defn- trunc
  [s n]
  (subs s 0 (min (count s) n)))

(defn- trace-op [{:keys [level depth color] :as this} op & extra]
  (print (in-ansi color (format "%s%s   %s %s"
                                ({:seek "s" :next "n"} op) @level (apply str (take (get @depth op) (repeat " ")))
                                (clojure.string/join " " extra)))))

(defprotocol PrintVal
  (print-val [this]))

(extend-protocol PrintVal
  org.agrona.DirectBuffer
  (print-val [this]
    (trunc (str (mem/buffer->hex this)) 10))

  clojure.lang.PersistentArrayMap
  (print-val [this]
    (format "{%s}"
            (clojure.string/join ", "
                                 (map (fn [[k v]]
                                        (str k " " (print-val v))) this))))

  Object
  (print-val [this]
    (trunc (str this) 40)))

(defn- v->str [v]
  (str "[" (clojure.string/join " " (map print-val v)) "]"))

(defmulti index-seek (fn [{:keys [i]} _] (-> i ^java.lang.Class type .getName symbol)))

(defmethod index-seek :default [{:keys [i depth] :as ii} k]
  (trace-op ii :seek (index-name i))
  (if (#{'crux.index.DocAttributeValueEntityEntityIndex
         'crux.index.DocAttributeValueEntityValueIndex
         'crux.index.DocAttributeEntityValueEntityIndex
         'crux.index.DocAttributeEntityValueValueIndex}
       (-> i ^java.lang.Class type .getName symbol))
    (do
      (swap! depth update :seek inc)
      (let [v (db/seek-values i k)]
        (println "" (v->str v))
        (swap! depth update :seek dec)
        v))
    (do
      (println)
      (swap! depth update :seek inc)
      (let [v (db/seek-values i k)]
        (trace-op ii :seek "--->" (v->str v))
        (println)
        (swap! depth update :seek dec)
        v))))

(defrecord InstrumentedLayeredIndex [i depth level color]
  db/Index
  (seek-values [this k]
    (index-seek this k))

  (next-values [this]
    (trace-op this :next (index-name i))
    (println)
    (swap! depth update :next inc)
    (let [v (db/next-values i)]
      (swap! depth update :next dec)
      v))

  db/LayeredIndex
  (open-level [this]
    (swap! level inc)
    (db/open-level i))

  (close-level [this]
    (db/close-level i)
    (swap! level dec))

  (max-depth [this]
    (db/max-depth i)))

(defprotocol Instrument
  (instrument [i depth visited]))

(defn inst [depth visited i]
  (instrument i depth visited))

(defn- get-color [i visited]
  (when (instance? crux.index.BinaryJoinLayeredVirtualIndex i)
    (first (shuffle (clojure.set/difference (set (keys ansi-colors)) (set (map :color (vals @visited))))))))

(defn ->instrumented-index [i depth visited]
  (let  [ii (or (and (instance? InstrumentedLayeredIndex i) i)
                (get @visited i)
                (let [ii (InstrumentedLayeredIndex. i depth (atom 0) (get-color i visited))]
                  (swap! visited assoc i ii)
                  ii))]
    (if (instance? crux.index.BinaryJoinLayeredVirtualIndex i)
      (assoc ii :name (:name i))
      ii)))

(extend-protocol Instrument
  crux.index.NAryConstrainingLayeredVirtualIndex
  (instrument [this depth visited]
    (let [this (update this :n-ary-index (partial inst depth visited))]
      (->instrumented-index this depth visited)))

  crux.index.NAryJoinLayeredVirtualIndex
  (instrument [this depth visited]
    (let [this (update this :unary-join-indexes (fn [indexes] (map (partial inst depth visited) indexes)))]
      (->instrumented-index this depth visited)))

  crux.index.UnaryJoinVirtualIndex
  (instrument [this depth visited]
    (or (get @visited this)
        (let [this (update this :indexes (fn [indexes] (map (partial inst depth visited) indexes)))]
          (->instrumented-index this depth visited))))

  crux.index.BinaryJoinLayeredVirtualIndex
  (instrument [^crux.index.BinaryJoinLayeredVirtualIndex this depth visited]
    (let [state ^crux.index.BinaryJoinLayeredVirtualIndexState (.state this)
          [lhs rhs] (doall (map (partial inst depth visited) (.indexes state)))]
      (set! (.indexes state) [lhs rhs])
      (->instrumented-index this depth visited)))

  crux.index.RelationVirtualIndex
  (instrument [^crux.index.RelationVirtualIndex this depth visited]
    (let [state ^crux.index.RelationIteratorsState (.state this)]
      (set! (.indexes state) (mapv (partial inst depth visited) (.indexes state)))
      (->instrumented-index this depth visited)))

  Object
  (instrument [this depth visited]
    (->instrumented-index this depth visited)))

(def original-layered-idx->seq i/layered-idx->seq)
(defn instrument-layered-idx->seq [idx]
  (original-layered-idx->seq (instrument idx (atom {:seek 0 :next 0}) (atom {}))))

(defmacro with-instrumentation [& form]
  `(with-redefs [i/layered-idx->seq instrument-layered-idx->seq]
     ~@form))
