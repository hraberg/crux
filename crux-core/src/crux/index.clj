(ns ^:no-doc crux.index
  (:require [crux.db :as db]
            [crux.memory :as mem])
  (:import [clojure.lang Box IDeref IPersistentVector]
           java.util.function.Function
           [java.util ArrayList Arrays Collection Comparator Iterator List NavigableSet NavigableMap TreeMap TreeSet]
           [java.util.concurrent ArrayBlockingQueue BlockingQueue ExecutionException LinkedBlockingQueue TimeUnit TimeoutException]
           org.agrona.DirectBuffer))

(set! *unchecked-math* :warn-on-boxed)

(deftype DerefIndex [idx ^:unsynchronized-mutable x]
  db/Index
  (seek-values [this k]
    (let [v (db/seek-values idx k)]
      (set! x v)
      v))

  (next-values [this]
    (let [v (db/next-values idx)]
      (set! x v)
      v))

  db/LayeredIndex
  (open-level [_]
    (db/open-level idx))

  (close-level [_]
    (db/close-level idx))

  (max-depth [_]
    (db/max-depth idx))

  IDeref
  (deref [_]
    x))

(defn new-deref-index ^crux.index.DerefIndex [idx]
  (if (instance? DerefIndex idx)
    idx
    (->DerefIndex idx nil)))

(deftype SeekFnIndex [seek-fn ^:unsynchronized-mutable xs]
  db/Index
  (seek-values [this k]
    (let [[v & vs] (seq (seek-fn k))]
      (set! xs vs)
      v))

  (next-values [this]
    (when-let [[v & vs] xs]
      (set! xs vs)
      v)))

(defn new-seek-fn-index ^crux.index.SeekFnIndex [seek-fn]
  (->SeekFnIndex seek-fn nil))

;; Range Constraints

(deftype PredicateVirtualIndex [idx pred seek-k-fn]
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

(deftype GreaterThanVirtualIndex [idx]
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

(def ^:const ^:private chunk-size 8)

(defn idx->seq
  [idx]
  (when-let [result (db/seek-values idx nil)]
    ((fn step [cb v]
       (if v
         (do (chunk-append cb v)
             (if (= (count cb) chunk-size)
               (chunk-cons (chunk cb)
                           (lazy-seq
                            (step (chunk-buffer chunk-size)
                                  (db/next-values idx))))
               (recur cb (db/next-values idx))))
         (chunk-cons (chunk cb) nil)))
     (chunk-buffer chunk-size) result)))

;; Join

(extend-protocol db/LayeredIndex
  Object
  (open-level [_])
  (close-level [_])
  (max-depth [_] 1))

(def ^:private ^Comparator unary-join-iterator-state-comparator
  (reify Comparator
    (compare [_ x y]
      (mem/compare-buffers (.deref ^IDeref x)
                           (.deref ^IDeref y)))))

(def ^:private init-state ::init)
(def ^:private next-state ::next)

(deftype UnaryJoinVirtualIndex [^objects indexes
                                ^:unsynchronized-mutable ^long index
                                ^:unsynchronized-mutable state]
  db/Index
  (seek-values [this k]
    (if-let [indexes (loop [n 0
                            acc indexes]
                       (if (= n (alength acc))
                         acc
                         (let [idx ^DerefIndex (aget indexes n)]
                           (when-let [v (db/seek-values idx k)]
                             (recur (inc n) acc)))))]
      (do (doto indexes
            (Arrays/sort unary-join-iterator-state-comparator))
          (set! index 0)
          (set! state init-state)
          (db/next-values this))
      (set! state nil)))

  (next-values [this]
    (when (and state (not (identical? init-state state)))
      (let [idx (aget indexes index)]
        (if (if (identical? next-state state)
              (db/next-values idx)
              (db/seek-values idx state))
          (let [index (inc index)
                index (if (= index (alength indexes))
                        0
                        index)]
            (set! (.index this) index))
          (set! state nil))))
    (when state
      (let [idx ^DerefIndex (aget indexes index)
            max-index (if (zero? index)
                        (dec (alength indexes))
                        (dec index))
            max-k (.deref ^DerefIndex (aget indexes max-index))
            match? (mem/buffers=? (.deref idx) max-k)]
        (set! state (if match?
                      next-state
                      max-k))
        (if match?
          max-k
          (recur)))))

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
    (->UnaryJoinVirtualIndex
     (object-array (for [idx indexes]
                     (new-deref-index idx)))
     0
     nil)))

(deftype NAryJoinLayeredVirtualIndex [^objects indexes
                                      ^:unsynchronized-mutable ^long depth
                                      constrain-result-fn
                                      ^:unsynchronized-mutable ^List join-keys]
  db/Index
  (seek-values [this k]
    (when-let [v (db/seek-values (aget indexes depth) k)]
      (if (constrain-result-fn (doto join-keys
                                 (.set depth v)) (inc depth))
        v
        (db/next-values this))))

  (next-values [this]
    (when-let [v (db/next-values (aget indexes depth))]
      (if (constrain-result-fn (doto join-keys
                                 (.set depth v)) (inc depth))
        v
        (recur))))

  db/LayeredIndex
  (open-level [this]
    (db/open-level (aget indexes depth))
    (set! depth (inc depth))
    nil)

  (close-level [this]
    (db/close-level (aget indexes (dec depth)))
    (set! depth (dec depth))
    nil)

  (max-depth [this]
    (alength indexes))

  IDeref
  (deref [this]
    join-keys))

(defn new-n-ary-join-layered-virtual-index
  ([indexes]
   (new-n-ary-join-layered-virtual-index indexes (constantly true)))
  ([indexes constrain-result-fn]
   (->NAryJoinLayeredVirtualIndex (object-array indexes)
                                  0
                                  constrain-result-fn
                                  (doto (ArrayList.)
                                    (.addAll (repeat (count indexes) nil))))))

(def ^:private ^:const timeout-ms 1000)
(def ^:private ^:const queue-size 128)

(defn layered-idx->seq
  ([idx]
   (layered-idx->seq idx vec))
  ([idx t-fn]
   (layered-idx->seq idx t-fn false))
  ([idx t-fn async?]
   (layered-idx->seq idx t-fn async? nil))
  ([idx t-fn async? limit]
   (when idx
     (let [limit (long (or limit Long/MAX_VALUE))
           ^BlockingQueue queue (if async?
                                  (ArrayBlockingQueue. queue-size)
                                  (LinkedBlockingQueue.))
           quere-ref (WeakReference. queue)
           max-depth (long (db/max-depth idx))
           done-fn (if async?
                     (fn [^BlockingQueue queue]
                       (when-not (.offer queue ::done timeout-ms TimeUnit/MILLISECONDS)
                         (throw (TimeoutException.))))
                     (fn [_]))
           produce-step (fn produce-step [^long depth ^long produced needs-seek?]
                          (when-let [queue ^BlockingQueue (.get queue-ref)]
                            (if (< produced limit)
                              (if (= depth (dec max-depth))
                                (let [produced (loop [v (db/seek-values idx nil)
                                                      produced produced]
                                                 (if v
                                                   (do (when-not (.offer queue (t-fn @idx) timeout-ms TimeUnit/MILLISECONDS)
                                                         (throw (TimeoutException.)))
                                                       (if (< produced limit)
                                                         (recur (db/next-values idx) (inc produced))
                                                         produced))
                                                   produced))]
                                  (if (pos? depth)
                                    (do (db/close-level idx)
                                        (recur (dec depth) (long produced) false))
                                    (done-fn queue)))
                                (if-let [v (if needs-seek?
                                             (db/seek-values idx nil)
                                             (db/next-values idx))]
                                  (do (db/open-level idx)
                                      (recur (inc depth) produced true))
                                  (if (pos? depth)
                                    (do (db/close-level idx)
                                        (recur (dec depth) produced false))
                                    (done-fn queue))))
                              (done-fn queue))))]
       (when (pos? max-depth)
         (if async?
           (let [f (future
                     (mem/with-region
                       (produce-step 0 0 true)))]
             (try
               ((fn consume-step [^List acc v]
                  (cond
                    (= ::done v)
                    (do @f
                        acc)

                    v
                    (do (doto acc
                          (.add v))
                        (.drainTo queue acc)
                        (if (= ::done (.get acc (dec (.size acc))))
                          (do @f
                              (.subList acc 0 (dec (.size acc))))
                          (recur acc (.poll queue timeout-ms TimeUnit/MILLISECONDS))))

                    :else
                    (do (when (future-done? f)
                          @f)
                        (let [tail (lazy-seq
                                    (consume-step (ArrayList.)
                                                  (or (.poll queue timeout-ms TimeUnit/MILLISECONDS)
                                                      (do (future-cancel f)
                                                          (throw (TimeoutException.))))))]
                          (if (empty? acc)
                            tail
                            (concat acc tail))))))
                (ArrayList.) (.poll queue timeout-ms TimeUnit/MILLISECONDS))
               (catch Throwable t
                 (future-cancel f)
                 (if (instance? ExecutionException t)
                   (throw (.getCause t))
                   (throw t)))))
           (do (mem/with-region
                 (produce-step 0 0 true))
               (seq queue))))))))

(deftype SortedVirtualIndex [^NavigableSet s ^:unsynchronized-mutable ^Iterator iterator]
  db/Index
  (seek-values [this k]
    (set! iterator (.iterator (.tailSet s (or k mem/empty-buffer))))
    (db/next-values this))

  (next-values [this]
    (when (and iterator (.hasNext iterator))
      (.next iterator))))

(defn- new-sorted-virtual-index [s]
  (->SortedVirtualIndex s nil))

(definterface IRelationVirtualIndexUpdate
  (^void updateIndex [tree rootIndex]))

(deftype RelationVirtualIndex [^long max-depth
                               encode-value-fn
                               ^:unsynchronized-mutable tree
                               ^:unsynchronized-mutable ^long depth
                               ^:unsynchronized-mutable ^objects path
                               ^:unsynchronized-mutable ^objects indexes]
  db/Index
  (seek-values [this k]
    (when-let [k (db/seek-values (aget indexes depth) (or k mem/empty-buffer))]
      (aset path depth k)
      k))

  (next-values [this]
    (when-let [k (db/next-values (aget indexes depth))]
      (aset path depth k)
      k))

  db/LayeredIndex
  (open-level [this]
    (when (= max-depth depth)
      (throw (IllegalStateException. (str "Cannot open level at max depth: " max-depth))))
    (set! depth (inc depth))
    (loop [n 0
           ^NavigableMap tree tree]
      (when tree
        (if (= depth n)
          (aset indexes depth (new-sorted-virtual-index (.navigableKeySet tree)))
          (recur (inc n) (.get tree (aget path n))))))
    nil)

  (close-level [this]
    (when (zero? depth)
      (throw (IllegalStateException. "Cannot close level at root.")))
    (set! depth (dec depth))
    nil)

  (max-depth [_]
    max-depth)

  IDeref
  (deref [_]
    (vec path))

  IRelationVirtualIndexUpdate
  (updateIndex [_ new-tree root-index]
    (set! tree new-tree)
    (set! depth 0)
    (aset indexes 0 root-index)))

(defn- tree-map-put-in [^TreeMap m [k & ks] v]
  (if ks
    (doto m
      (-> (.computeIfAbsent k
                            (reify Function
                              (apply [_ k]
                                (TreeMap. (.comparator m)))))
          (tree-map-put-in ks v)))
    (doto m
      (.put k v))))

(defn update-relation-virtual-index!
  ([^RelationVirtualIndex relation tuples]
   (update-relation-virtual-index! relation tuples (.encode_value_fn relation) false))
  ([^RelationVirtualIndex relation tuples encode-value-fn single-values?]
   (let [tree (when-not single-values?
                (reduce
                 (fn [acc tuple]
                   (tree-map-put-in acc (mapv encode-value-fn tuple) nil))
                 (TreeMap. mem/buffer-comparator)
                 tuples))
         root-idx (if single-values?
                    (new-sorted-virtual-index (if (instance? NavigableSet tuples)
                                                tuples
                                                (doto (TreeSet. mem/buffer-comparator)
                                                  (.addAll (mapv encode-value-fn tuples)))))
                    (new-sorted-virtual-index (.navigableKeySet ^NavigableMap tree)))]
     (.updateIndex relation tree root-idx)
     relation)))

(defn new-relation-virtual-index [tuples max-depth encode-value-fn]
  (update-relation-virtual-index! (->RelationVirtualIndex max-depth
                                                          encode-value-fn
                                                          nil
                                                          0
                                                          (object-array (long max-depth))
                                                          (object-array (long max-depth)))
                                  tuples))

(deftype SingletonVirtualIndex [v]
  db/Index
  (seek-values [_ k]
    (when-not (pos? (mem/compare-buffers (or k mem/empty-buffer) v))
      v))

  (next-values [_]))

(defn new-singleton-virtual-index [v encode-value-fn]
  (->SingletonVirtualIndex (encode-value-fn v)))
