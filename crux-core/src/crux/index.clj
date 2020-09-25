(ns ^:no-doc crux.index
  (:require [crux.db :as db]
            [crux.memory :as mem])
  (:import [crux.index IndexStoreIndexState SortedVirtualIndexState UnaryJoinIteratorState UnaryJoinIteratorsThunkFnState UnaryJoinIteratorsThunkState]
           clojure.lang.Box
           java.util.function.Function
           [java.util Comparator Iterator NavigableMap TreeMap]
           org.agrona.DirectBuffer))

(set! *unchecked-math* :warn-on-boxed)

;; Index Store

(defrecord IndexStoreIndex [seek-fn ^IndexStoreIndexState state]
  db/Index
  (seek-values [this k]
    (let [[v & vs] (seek-fn k)]
      (set! (.-seq state) vs)
      (set! (.-key state) v)
      v))

  (next-values [this]
    (when-let [[v & vs] (.-seq state)]
      (set! (.-seq state) vs)
      (set! (.-key state) v)
      v)))

(defn new-index-store-index ^crux.index.IndexStoreIndex [seek-fn]
  (->IndexStoreIndex seek-fn (IndexStoreIndexState. nil nil)))

;; Range Constraints

(defrecord PredicateVirtualIndex [idx pred seek-k-fn]
  db/Index
  (seek-values [this k]
    (when-let [v (db/seek-values idx (seek-k-fn k))]
      (when (pred v)
        v)))

  (next-values [this]
    (when-let [v (db/next-values idx)]
      (when (pred v)
        v)))

  db/LayeredIndex
  (open-level [_]
    (db/open-level idx))

  (close-level [_]
    (db/close-level idx))

  (max-depth [_]
    (db/max-depth idx)))

(defn- value-comparsion-predicate
  ([compare-pred compare-v]
   (value-comparsion-predicate compare-pred compare-v Integer/MAX_VALUE))
  ([compare-pred ^Box compare-v ^long max-length]
   (if (.val compare-v)
     (fn [value]
       (and value (compare-pred (mem/compare-buffers value (.val compare-v) max-length))))
     (constantly true))))

(defn new-prefix-equal-virtual-index [idx ^Box prefix-v ^long prefix-size]
  (let [seek-k-pred (value-comparsion-predicate (comp not neg?) prefix-v prefix-size)
        pred (value-comparsion-predicate zero? prefix-v prefix-size)]
    (->PredicateVirtualIndex idx pred (fn [k]
                                        (if (seek-k-pred k)
                                          k
                                          (mem/limit-buffer (.val prefix-v) prefix-size))))))

(defn new-less-than-equal-virtual-index [idx max-v]
  (let [pred (value-comparsion-predicate (comp not pos?) max-v)]
    (->PredicateVirtualIndex idx pred identity)))

(defn new-less-than-virtual-index [idx max-v]
  (let [pred (value-comparsion-predicate neg? max-v)]
    (->PredicateVirtualIndex idx pred identity)))

(defn new-greater-than-equal-virtual-index [idx ^Box min-v]
  (let [pred (value-comparsion-predicate (comp not neg?) min-v)]
    (->PredicateVirtualIndex idx pred (fn [k]
                                        (if (pred k)
                                          k
                                          (.val min-v))))))

(defrecord GreaterThanVirtualIndex [idx]
  db/Index
  (seek-values [this k]
    (or (db/seek-values idx k)
        (db/next-values idx)))

  (next-values [this]
    (db/next-values idx))

  db/LayeredIndex
  (open-level [_]
    (db/open-level idx))

  (close-level [_]
    (db/close-level idx))

  (max-depth [_]
    (db/max-depth idx)))

(defn new-greater-than-virtual-index [idx ^Box min-v]
  (let [pred (value-comparsion-predicate pos? min-v)
        idx (->PredicateVirtualIndex idx pred (fn [k]
                                                (if (pred k)
                                                  k
                                                  (.val min-v))))]
    (->GreaterThanVirtualIndex idx)))

(defn new-equals-virtual-index [idx ^Box v]
  (let [pred (value-comparsion-predicate zero? v)]
    (->PredicateVirtualIndex idx pred (fn [k]
                                        (if (pred k)
                                          k
                                          (.val v))))))

(defn wrap-with-range-constraints [idx range-constraints]
  (if range-constraints
    (range-constraints idx)
    idx))

;; Utils

(defn idx->seq
  [idx]
  (when-let [result (db/seek-values idx nil)]
    (->> (repeatedly #(db/next-values idx))
         (take-while identity)
         (cons result))))

;; Join

(extend-protocol db/LayeredIndex
  Object
  (open-level [_])
  (close-level [_])
  (max-depth [_] 1))

(defn- new-unary-join-iterator-state [idx value]
  (UnaryJoinIteratorState. idx (or value mem/empty-buffer)))

(defn- long-mod ^long [^long num ^long div]
  (let [m (rem num div)]
    (if (or (zero? m) (= (pos? num) (pos? div)))
      m
      (+ m div))))

(defrecord UnaryJoinVirtualIndex [indexes ^UnaryJoinIteratorsThunkFnState state]
  db/Index
  (seek-values [this k]
    (->> #(let [iterators (->> (for [idx indexes]
                                 (new-unary-join-iterator-state idx (db/seek-values idx k)))
                               (sort-by (fn [x] (.key ^UnaryJoinIteratorState x)) mem/buffer-comparator)
                               (to-array))]
            (UnaryJoinIteratorsThunkState. iterators 0))
         (set! (.thunk state)))
    (db/next-values this))

  (next-values [this]
    (when-let [iterators-thunk (.thunk state)]
      (when-let [iterators-thunk ^UnaryJoinIteratorsThunkState (iterators-thunk)]
        (let [iterators ^objects (.iterators iterators-thunk)
              index (.index iterators-thunk)
              iterator-state ^UnaryJoinIteratorState (aget iterators index)
              max-index (long-mod (dec index) (alength iterators))
              max-k (.key ^UnaryJoinIteratorState (aget iterators max-index))
              match? (mem/buffers=? (.key iterator-state) max-k)
              idx (.idx iterator-state)]
          (->> #(let [v (if match?
                          (db/next-values idx)
                          (db/seek-values idx max-k))]
                  (when v
                    (set! (.-key iterator-state) v)
                    (set! (.index iterators-thunk) (long-mod (inc index) (alength iterators)))
                    iterators-thunk))
               (set! (.thunk state)))
          (if match?
            max-k
            (recur))))))

  db/LayeredIndex
  (open-level [this]
    (doseq [idx indexes]
      (db/open-level idx)))

  (close-level [this]
    (doseq [idx indexes]
      (db/close-level idx)))

  (max-depth [this]
    1))

(defn new-unary-join-virtual-index [indexes]
  (if (= 1 (count indexes))
    (first indexes)
    (->UnaryJoinVirtualIndex indexes (UnaryJoinIteratorsThunkFnState. nil))))

(defn tree-map-put-in [^TreeMap m [k & ks] v]
  (if ks
    (doto m
      (-> (.computeIfAbsent k
                            (reify Function
                              (apply [_ k]
                                (TreeMap. (.comparator m)))))
          (tree-map-put-in ks v)))
    (doto m
      (.put k v))))

(defrecord SortedVirtualIndex [^NavigableMap m ^SortedVirtualIndexState state]
  db/Index
  (seek-values [this k]
    (set! (.iterator state) (.iterator (.tailSet (.navigableKeySet m) (or k mem/empty-buffer))))
    (db/next-values this))

  (next-values [this]
    (when-let [iterator ^Iterator (.iterator state)]
      (when (.hasNext iterator)
        (.next iterator)))))

(defn new-sorted-virtual-index
  ([m]
   (->SortedVirtualIndex m (SortedVirtualIndexState. nil)))
  ([coll encode-value-fn]
   (new-sorted-virtual-index
    (reduce
     (fn [^NavigableMap acc x]
       (doto acc
         (.put (encode-value-fn x) nil)))
     (TreeMap. mem/buffer-comparator)
     coll)) ))

(defrecord SingletonVirtualIndex [v]
  db/Index
  (seek-values [_ k]
    (when-not (pos? (mem/compare-buffers (or k mem/empty-buffer) v))
      v))

  (next-values [_]))

(defn new-singleton-virtual-index [v encode-value-fn]
  (->SingletonVirtualIndex (encode-value-fn v)))
