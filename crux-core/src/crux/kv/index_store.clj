(ns ^:no-doc crux.kv.index-store
  (:require [clojure.spec.alpha :as s]
            [crux.cache :as cache]
            [crux.cache.nop :as nop-cache]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.error :as err]
            [crux.fork :as fork]
            [crux.io :as cio]
            [crux.kv :as kv]
            [crux.kv.mutable-kv :as mut-kv]
            [crux.memory :as mem]
            [crux.morton :as morton]
            [crux.status :as status]
            [crux.system :as sys])
  (:import clojure.lang.MapEntry
           crux.api.IndexVersionOutOfSyncException
           [crux.codec EntityTx Id]
           java.io.Closeable
           java.nio.ByteOrder
           [java.util ArrayList Collections Date HashMap List Map NavigableSet TreeSet]
           [java.util.function Function Supplier]
           java.util.concurrent.atomic.AtomicBoolean
           java.util.function.Supplier
           [org.agrona DirectBuffer ExpandableDirectByteBuffer MutableDirectBuffer]))

(set! *unchecked-math* :warn-on-boxed)

(def ^:private ^ThreadLocal seek-buffer-tl
  (ThreadLocal/withInitial
   (reify Supplier
     (get [_]
       (ExpandableDirectByteBuffer.)))))

;; NOTE: A buffer returned from an kv/KvIterator can only be assumed
;; to be valid until the next call on the same iterator. In practice
;; this limitation is only for RocksJNRKv.
;; TODO: It would be nice to make this explicit somehow.

(defrecord PrefixKvIterator [i ^DirectBuffer prefix]
  kv/KvIterator
  (seek [_ k]
    (when-let [k (kv/seek i k)]
      (when (mem/buffers=? k prefix (.capacity prefix))
        k)))

  (prev [_]
    (when-let [k (kv/prev i)]
      (when (mem/buffers=? k prefix (.capacity prefix))
        k)))

  (next [_]
    (when-let [k (kv/next i)]
      (when (mem/buffers=? k prefix (.capacity prefix))
        k)))

  (value [_]
    (kv/value i))

  Closeable
  (close [_]
    (.close ^Closeable i)))

(defn- new-prefix-kv-iterator ^java.io.Closeable [i prefix]
  (->PrefixKvIterator i prefix))

(defn- all-keys-in-prefix
  ([i ^DirectBuffer prefix] (all-keys-in-prefix i prefix (.capacity prefix) {}))
  ([i seek-k prefix-length] (all-keys-in-prefix i seek-k prefix-length {}))
  ([i ^DirectBuffer seek-k, prefix-length {:keys [entries? reverse?]}]
   (letfn [(step [k]
             (lazy-seq
              (when (and k (mem/buffers=? seek-k k prefix-length))
                (cons (if entries?
                        (MapEntry/create (mem/ensure-on-heap k) (mem/ensure-on-heap (kv/value i)))
                        (mem/ensure-on-heap k))
                      (step (if reverse? (kv/prev i) (kv/next i)))))))]
     (step (if reverse?
             (when (kv/seek i (-> seek-k (mem/copy-buffer) (mem/inc-unsigned-buffer!)))
               (kv/prev i))
             (kv/seek i seek-k))))))

(defn- buffer-or-value-buffer [v]
  (cond
    (instance? DirectBuffer v)
    v

    (some? v)
    (c/->value-buffer v)

    :else
    mem/empty-buffer))

(defn ^EntityTx enrich-entity-tx [entity-tx ^DirectBuffer content-hash]
  (assoc entity-tx :content-hash (when (pos? (.capacity content-hash))
                                   (c/safe-id (c/new-id content-hash)))))

(defn safe-entity-tx ^crux.codec.EntityTx [entity-tx]
  (-> entity-tx
      (update :eid c/safe-id)
      (update :content-hash c/safe-id)))

(defrecord Quad [attr eid content-hash value])

(defn- key-suffix [^DirectBuffer k ^long prefix-size]
  (mem/slice-buffer k prefix-size (- (.capacity k) prefix-size)))

;;;; Content indices

(defn- encode-av-key-to
  (^org.agrona.MutableDirectBuffer[b attr]
   (encode-av-key-to b attr mem/empty-buffer))
  (^org.agrona.MutableDirectBuffer
   [^MutableDirectBuffer b ^DirectBuffer attr ^DirectBuffer v]
   (assert (= c/id-size (.capacity attr)) (mem/buffer->hex attr))
   (let [^MutableDirectBuffer b (or b (mem/allocate-buffer (+ c/index-id-size c/id-size (.capacity v))))]
     (mem/limit-buffer
      (doto b
        (.putByte 0 c/av-index-id)
        (.putBytes c/index-id-size attr 0 c/id-size)
        (.putBytes (+ c/index-id-size c/id-size) v 0 (.capacity v)))
      (+ c/index-id-size c/id-size (.capacity v))))))

(defn- encode-ave-key-to
  (^org.agrona.MutableDirectBuffer[b attr]
   (encode-ave-key-to b attr mem/empty-buffer mem/empty-buffer))
  (^org.agrona.MutableDirectBuffer[b attr v]
   (encode-ave-key-to b attr v mem/empty-buffer))
  (^org.agrona.MutableDirectBuffer
   [^MutableDirectBuffer b ^DirectBuffer attr ^DirectBuffer v ^DirectBuffer entity]
   (assert (= c/id-size (.capacity attr)) (mem/buffer->hex attr))
   (let [^MutableDirectBuffer b (or b (mem/allocate-buffer (+ c/index-id-size c/id-size (.capacity v) (.capacity entity))))]
     (mem/limit-buffer
      (doto b
        (.putByte 0 c/ave-index-id)
        (.putBytes c/index-id-size attr 0 c/id-size)
        (.putBytes (+ c/index-id-size c/id-size) v 0 (.capacity v))
        (.putBytes (+ c/index-id-size c/id-size (.capacity v)) entity 0 (.capacity entity)))
      (+ c/index-id-size c/id-size (.capacity v) (.capacity entity))))))

(defn- decode-ave-key->e-from [^DirectBuffer k, ^long v-size]
  (let [av-size (+ c/index-id-size c/id-size v-size)]
    (mem/slice-buffer k av-size (- (.capacity k) av-size))))

(defn- encode-ae-key-to
  (^org.agrona.MutableDirectBuffer [b]
   (encode-ae-key-to b mem/empty-buffer mem/empty-buffer))
  (^org.agrona.MutableDirectBuffer [b attr]
   (encode-ae-key-to b attr mem/empty-buffer))
  (^org.agrona.MutableDirectBuffer [b ^DirectBuffer attr ^DirectBuffer entity]
   (assert (or (zero? (.capacity attr)) (= c/id-size (.capacity attr)))
           (mem/buffer->hex attr))
   (let [^MutableDirectBuffer b (or b (mem/allocate-buffer (+ c/index-id-size (.capacity attr) (.capacity entity))))]
     (-> (doto b
           (.putByte 0 c/ae-index-id)
           (.putBytes c/index-id-size attr 0 (.capacity attr))
           (.putBytes (+ c/index-id-size (.capacity attr)) entity 0 (.capacity entity)))
         (mem/limit-buffer (+ c/index-id-size (.capacity attr) (.capacity entity)))))))

(defn- encode-ecav-key-to
  (^org.agrona.MutableDirectBuffer [b entity]
   (encode-ecav-key-to b entity mem/empty-buffer mem/empty-buffer mem/empty-buffer))
  (^org.agrona.MutableDirectBuffer [b entity content-hash]
   (encode-ecav-key-to b entity content-hash mem/empty-buffer mem/empty-buffer))
  (^org.agrona.MutableDirectBuffer [b entity content-hash attr]
   (encode-ecav-key-to b entity content-hash attr mem/empty-buffer))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b ^DirectBuffer entity ^DirectBuffer content-hash ^DirectBuffer attr ^DirectBuffer v]
   (assert (or (zero? (.capacity attr)) (= c/id-size (.capacity attr)))
           (mem/buffer->hex attr))
   (assert (or (zero? (.capacity content-hash)) (= c/id-size (.capacity content-hash)))
           (mem/buffer->hex content-hash))
   (let [^MutableDirectBuffer b (or b (mem/allocate-buffer (+ c/index-id-size (.capacity entity) (.capacity content-hash) (.capacity attr) (.capacity v))))]
     (-> (doto b
           (.putByte 0 c/ecav-index-id)
           (.putBytes c/index-id-size entity 0 (.capacity entity))
           (.putBytes (+ c/index-id-size (.capacity entity)) content-hash 0 (.capacity content-hash))
           (.putBytes (+ c/index-id-size (.capacity entity) (.capacity content-hash)) attr 0 (.capacity attr))
           (.putBytes (+ c/index-id-size (.capacity entity) (.capacity content-hash) (.capacity attr)) v 0 (.capacity v)))
         (mem/limit-buffer (+ c/index-id-size (.capacity entity) (.capacity content-hash) (.capacity attr) (.capacity v)))))))

(defn- decode-ecav-key-from ^crux.kv.index_store.Quad [^DirectBuffer k ^long eid-size]
  (let [length (long (.capacity k))]
    (assert (<= (+ c/index-id-size eid-size c/id-size c/id-size) length) (mem/buffer->hex k))
    (let [index-id (.getByte k 0)]
      (assert (= c/ecav-index-id index-id))
      (let [entity (mem/slice-buffer k c/index-id-size eid-size)
            content-hash (Id. (mem/slice-buffer k (+ c/index-id-size eid-size) c/id-size) 0)
            attr (Id. (mem/slice-buffer k (+ c/index-id-size eid-size c/id-size) c/id-size) 0)
            value (key-suffix k (+ c/index-id-size eid-size c/id-size c/id-size))]
        (->Quad attr entity content-hash value)))))

(defn- encode-hash-cache-key-to
  (^org.agrona.MutableDirectBuffer [b value]
   (encode-hash-cache-key-to b value mem/empty-buffer))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b ^DirectBuffer value ^DirectBuffer entity]
   (let [^MutableDirectBuffer b (or b (mem/allocate-buffer (+ c/index-id-size (.capacity value) (.capacity entity))))]
     (-> (doto b
           (.putByte 0 c/hash-cache-index-id)
           (.putBytes c/index-id-size value 0 (.capacity value))
           (.putBytes (+ c/index-id-size (.capacity value)) entity 0 (.capacity entity)))
         (mem/limit-buffer (+ c/index-id-size (.capacity value) (.capacity entity)))))))

;;;; Bitemp indices

(defn- encode-bitemp-key-to
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b]
   (encode-bitemp-key-to b mem/empty-buffer nil nil nil))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b entity]
   (encode-bitemp-key-to b entity nil nil nil))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b entity valid-time]
   (encode-bitemp-key-to b entity valid-time nil nil))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b ^DirectBuffer entity ^Date valid-time ^Long tx-id ^Date tx-time]
   (assert (or (= c/id-size (.capacity entity))
               (zero? (.capacity entity))) (mem/buffer->hex entity))
   (let [^MutableDirectBuffer b (or b (mem/allocate-buffer (cond-> (+ c/index-id-size (.capacity entity))
                                                             valid-time (+ Long/BYTES)
                                                             tx-id (+ Long/BYTES)
                                                             tx-time (+ Long/BYTES))))]
     (-> b
         (doto (.putByte 0 c/entity+vt+tt+tx-id->content-hash-index-id))
         (doto (.putBytes c/index-id-size entity 0 (.capacity entity)))
         (doto (cond-> valid-time (.putLong (+ c/index-id-size c/id-size)
                                            (c/date->reverse-time-ms valid-time)
                                            ByteOrder/BIG_ENDIAN)))
         (doto (cond-> tx-id (.putLong (+ c/index-id-size c/id-size Long/BYTES)
                                       (c/descending-long tx-id)
                                       ByteOrder/BIG_ENDIAN)))
         (doto (cond-> tx-time (.putLong (+ c/index-id-size c/id-size Long/BYTES Long/BYTES)
                                         (c/date->reverse-time-ms tx-time)
                                         ByteOrder/BIG_ENDIAN)))
         (mem/limit-buffer (+ c/index-id-size
                              (.capacity entity)
                              (c/maybe-long-size valid-time)
                              (c/maybe-long-size tx-time)
                              (c/maybe-long-size tx-id)))))))

(defn- decode-bitemp-key-from ^crux.codec.EntityTx [^DirectBuffer k]
  (assert (= (+ c/index-id-size c/id-size Long/BYTES Long/BYTES Long/BYTES) (.capacity k)) (mem/buffer->hex k))
  (let [index-id (.getByte k 0)]
    (assert (= c/entity+vt+tt+tx-id->content-hash-index-id index-id))
    (let [entity (Id. (mem/slice-buffer k c/index-id-size c/id-size) 0)
          valid-time (c/reverse-time-ms->date (.getLong k (+ c/index-id-size c/id-size) ByteOrder/BIG_ENDIAN))
          tx-id (c/descending-long (.getLong k (+ c/index-id-size c/id-size Long/BYTES) ByteOrder/BIG_ENDIAN))
          tx-time (c/reverse-time-ms->date (.getLong k (+ c/index-id-size c/id-size Long/BYTES Long/BYTES) ByteOrder/BIG_ENDIAN))]
      (c/->EntityTx entity valid-time tx-time tx-id nil))))

(defn- decode-bitemp-key-as-tx-id-from ^java.util.Date [^DirectBuffer k]
  (c/descending-long (.getLong k (+ c/index-id-size c/id-size Long/BYTES) ByteOrder/BIG_ENDIAN)))

(defn- encode-entity-tx-z-number [valid-time tx-id]
  (morton/longs->morton-number (c/date->reverse-time-ms valid-time)
                               (c/descending-long tx-id)))

(defn- encode-bitemp-z-key-to
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b]
   (encode-bitemp-z-key-to b mem/empty-buffer nil))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b entity]
   (encode-bitemp-z-key-to b entity nil nil))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b entity z]
   (encode-bitemp-z-key-to b entity z nil))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b ^DirectBuffer entity z ^Long tx-time]
   (assert (or (= c/id-size (.capacity entity))
               (zero? (.capacity entity))) (mem/buffer->hex entity))
   (let [^MutableDirectBuffer b (or b (mem/allocate-buffer (cond-> (+ c/index-id-size (.capacity entity))
                                                             z (+ (* 2 Long/BYTES))
                                                             tx-time (+ Long/BYTES))))
         [upper-morton lower-morton] (when z
                                       (morton/morton-number->interleaved-longs z))]
     (.putByte b 0 c/entity+z+tx-id->content-hash-index-id)
     (.putBytes b c/index-id-size entity 0 (.capacity entity))
     (when z
       (.putLong b (+ c/index-id-size c/id-size) upper-morton ByteOrder/BIG_ENDIAN)
       (.putLong b (+ c/index-id-size c/id-size Long/BYTES) lower-morton ByteOrder/BIG_ENDIAN))
     (when tx-time
       (.putLong b (+ c/index-id-size c/id-size Long/BYTES Long/BYTES) (c/date->reverse-time-ms tx-time) ByteOrder/BIG_ENDIAN))
     (->> (+ c/index-id-size (.capacity entity) (if z (* 2 Long/BYTES) 0) (c/maybe-long-size tx-time))
          (mem/limit-buffer b)))))

(defn- decode-bitemp-z-key-as-z-number-from [^DirectBuffer k]
  (assert (= (+ c/index-id-size c/id-size Long/BYTES Long/BYTES Long/BYTES) (.capacity k)) (mem/buffer->hex k))
  (let [index-id (.getByte k 0)]
    (assert (= c/entity+z+tx-id->content-hash-index-id index-id))
    (morton/interleaved-longs->morton-number
     (.getLong k (+ c/index-id-size c/id-size) ByteOrder/BIG_ENDIAN)
     (.getLong k (+ c/index-id-size c/id-size Long/BYTES) ByteOrder/BIG_ENDIAN))))

(defn- decode-bitemp-z-key-from ^crux.codec.EntityTx [^DirectBuffer k]
  (assert (= (+ c/index-id-size c/id-size Long/BYTES Long/BYTES Long/BYTES) (.capacity k)) (mem/buffer->hex k))
  (let [index-id (.getByte k 0)]
    (assert (= c/entity+z+tx-id->content-hash-index-id index-id))
    (let [entity (Id. (mem/slice-buffer k c/index-id-size c/id-size) 0)
          [valid-time tx-id] (morton/morton-number->longs (decode-bitemp-z-key-as-z-number-from k))
          tx-time (c/reverse-time-ms->date (.getLong k (+ c/index-id-size c/id-size Long/BYTES Long/BYTES) ByteOrder/BIG_ENDIAN))]
      (c/->EntityTx entity (c/reverse-time-ms->date valid-time) tx-time (c/descending-long tx-id) nil))))

(defn- etx->kvs [^EntityTx etx]
  (let [eid (c/->id-buffer (.eid etx))
        z (encode-entity-tx-z-number (.vt etx) (.tx-id etx))]
    [(MapEntry/create (encode-bitemp-key-to nil eid (.vt etx) (.tx-id etx) (.tt etx))
                      (c/->id-buffer (.content-hash etx)))
     (MapEntry/create (encode-bitemp-z-key-to nil eid z (.tt etx))
                      (c/->id-buffer (.content-hash etx)))]))

;; Index Version

(defn- encode-index-version-key-to ^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b]
  (let [^MutableDirectBuffer b (or b (mem/allocate-buffer c/index-id-size))]
    (.putByte b 0 c/index-version-index-id)
    (mem/limit-buffer b c/index-id-size)))

(defn- encode-index-version-value-to ^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b ^long version]
  (let [^MutableDirectBuffer b (or b (mem/allocate-buffer c/index-version-size))]
    (doto b
      (.putLong 0 version ByteOrder/BIG_ENDIAN))
    (mem/limit-buffer b c/index-version-size)))

(defn- decode-index-version-value-from ^long [^MutableDirectBuffer b]
  (.getLong b 0 ByteOrder/BIG_ENDIAN))

(defn- current-index-version [kv]
  (with-open [snapshot (kv/new-snapshot kv)]
    (some->> (kv/get-value snapshot (encode-index-version-key-to (.get seek-buffer-tl)))
             (decode-index-version-value-from))))

(defn- check-and-store-index-version [{:keys [kv-store skip-index-version-bump]}]
  (let [index-version (current-index-version kv-store)]
    (or (when (and index-version (not= c/index-version index-version))
          (let [[skip-from skip-to] skip-index-version-bump]
            (when-not (and (= skip-from index-version)
                           (= skip-to c/index-version))
              (throw (IndexVersionOutOfSyncException.
                      (str "Index version on disk: " index-version " does not match index version of code: " c/index-version))))))
        (doto kv-store
          (kv/store [[(encode-index-version-key-to nil)
                      (encode-index-version-value-to nil c/index-version)]])
          (kv/fsync)))))

;; Meta

(defn- encode-meta-key-to ^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b ^DirectBuffer k]
  (assert (= c/id-size (.capacity k)) (mem/buffer->hex k))
  (let [^MutableDirectBuffer b (or b (mem/allocate-buffer (+ c/index-id-size c/id-size)))]
    (mem/limit-buffer
     (doto b
       (.putByte 0 c/meta-key->value-index-id)
       (.putBytes c/index-id-size k 0 (.capacity k)))
     (+ c/index-id-size c/id-size))))

(defn meta-kv [k v]
  [(encode-meta-key-to nil (c/->id-buffer k))
   (mem/->nippy-buffer v)])

(defn store-meta [kv k v]
  (kv/store kv [(meta-kv k v)]))

(defn- read-meta-snapshot
  ([snapshot k] (read-meta-snapshot snapshot k nil))
  ([snapshot k not-found]
   (if-let [v (kv/get-value snapshot (encode-meta-key-to (.get seek-buffer-tl) (c/->id-buffer k)))]
     (mem/<-nippy-buffer v)
     not-found)))

(defn read-meta
  ([kv k] (read-meta kv k nil))
  ([kv k not-found]
   (with-open [snapshot (kv/new-snapshot kv)]
     (read-meta-snapshot snapshot k not-found))))

;;;; Failed tx-id

(defn- encode-failed-tx-id-key-to
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b]
   (encode-failed-tx-id-key-to b nil))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b tx-id]
   (let [^MutableDirectBuffer b (or b (mem/allocate-buffer (+ c/index-id-size (c/maybe-long-size tx-id))))]
     (.putByte b 0 c/failed-tx-id-index-id)
     (when tx-id
       (.putLong b c/index-id-size (c/descending-long tx-id) ByteOrder/BIG_ENDIAN))
     (mem/limit-buffer b (+ c/index-id-size (c/maybe-long-size tx-id))))))

;;;; tx-id/tx-time mappings

(def tx-time-mapping-prefix
  (doto ^MutableDirectBuffer (mem/allocate-buffer c/index-id-size)
    (.putByte 0 c/tx-time-mapping-id)))

(defn- encode-tx-time-mapping-key-to [to tx-time tx-id]
  (let [size (+ c/index-id-size (c/maybe-long-size tx-time) (c/maybe-long-size tx-id))
        ^MutableDirectBuffer to (or to (mem/allocate-buffer size))]
    (assert (>= (.capacity to) size))
    (-> to
        (doto (.putByte 0 c/tx-time-mapping-id))
        (cond-> tx-time (doto (.putLong c/index-id-size (c/date->reverse-time-ms tx-time) ByteOrder/BIG_ENDIAN)))
        (cond-> tx-id (doto (.putLong (+ c/index-id-size Long/BYTES) (c/descending-long tx-id) ByteOrder/BIG_ENDIAN)))
        (mem/limit-buffer size))))

(defn- decode-tx-time-mapping-key-from [^DirectBuffer k]
  (assert (= c/tx-time-mapping-id (.getByte k 0)))
  {:crux.tx/tx-time (c/reverse-time-ms->date (.getLong k c/index-id-size ByteOrder/BIG_ENDIAN))
   :crux.tx/tx-id (c/descending-long (.getLong k (+ c/index-id-size Long/BYTES) ByteOrder/BIG_ENDIAN))})

;;;; Entity as-of

(defn- find-first-entity-tx-within-range [i min max eid]
  (let [prefix-size (+ c/index-id-size c/id-size)
        seek-k (encode-bitemp-z-key-to (.get seek-buffer-tl)
                                       eid
                                       min)]
    (loop [k (kv/seek i seek-k)]
      (when (and k (mem/buffers=? seek-k k prefix-size))
        (let [z (decode-bitemp-z-key-as-z-number-from k)]
          (if (morton/morton-number-within-range? min max z)
            (let [entity-tx (safe-entity-tx (decode-bitemp-z-key-from k))
                  v (kv/value i)]
              (if-not (mem/buffers=? c/nil-id-buffer v)
                [(c/->id-buffer (.eid entity-tx))
                 (enrich-entity-tx entity-tx v)
                 z]
                [::deleted-entity entity-tx z]))
            (let [[litmax bigmin] (morton/morton-range-search min max z)]
              (when-not (neg? (.compareTo ^Comparable bigmin z))
                (recur (kv/seek i (encode-bitemp-z-key-to (.get seek-buffer-tl)
                                                          eid
                                                          bigmin)))))))))))

(defn- find-entity-tx-within-range-with-highest-valid-time [i min max eid prev-candidate]
  (if-let [[_ ^EntityTx entity-tx z :as candidate] (find-first-entity-tx-within-range i min max eid)]
    (let [[^long x ^long y] (morton/morton-number->longs z)
          min-x (long (first (morton/morton-number->longs min)))
          max-x (dec x)]
      (if (and (not (pos? (Long/compareUnsigned min-x max-x)))
               (not= y -1))
        (let [min (morton/longs->morton-number
                   min-x
                   (unchecked-inc y))
              max (morton/longs->morton-number
                   max-x
                   -1)]
          (recur i min max eid candidate))
        candidate))
    prev-candidate))

;;;; History

(defn- ->entity-tx [[k v]]
  (-> (decode-bitemp-key-from k)
      (enrich-entity-tx v)))

(defn- entity-history-seq-ascending
  ([i eid] ([i eid] (entity-history-seq-ascending i eid {})))
  ([i eid {:keys [with-corrections? start-valid-time end-valid-time start-tx-id end-tx-id]}]
   (let [seek-k (encode-bitemp-key-to nil (c/->id-buffer eid) start-valid-time)]
     (-> (all-keys-in-prefix i seek-k (+ c/index-id-size c/id-size)
                             {:reverse? true, :entries? true})
         (->> (map ->entity-tx))
         (cond->> end-valid-time (take-while (fn [^EntityTx entity-tx]
                                               (neg? (compare (.vt entity-tx) end-valid-time))))
                  start-tx-id (remove (fn [^EntityTx entity-tx]
                                        (< ^long (.tx-id entity-tx) ^long start-tx-id)))
                  end-tx-id (filter (fn [^EntityTx entity-tx]
                                      (< ^long (.tx-id entity-tx) ^long end-tx-id))))
         (cond-> (not with-corrections?) (->> (partition-by :vt)
                                              (map last)))))))

(defn- entity-history-seq-descending
  ([i eid] (entity-history-seq-descending i eid {}))
  ([i eid {:keys [with-corrections? start-valid-time start-tx-id end-valid-time end-tx-id]}]
   (let [seek-k (encode-bitemp-key-to nil (c/->id-buffer eid) start-valid-time)]
     (-> (all-keys-in-prefix i seek-k (+ c/index-id-size c/id-size)
                             {:entries? true})
         (->> (map ->entity-tx))
         (cond->> end-valid-time (take-while (fn [^EntityTx entity-tx]
                                               (pos? (compare (.vt entity-tx) end-valid-time))))
                  start-tx-id (remove (fn [^EntityTx entity-tx]
                                        (> ^long (.tx-id entity-tx) ^long start-tx-id)))
                  end-tx-id (filter (fn [^EntityTx entity-tx]
                                      (> ^long (.tx-id entity-tx) ^long end-tx-id))))
         (cond-> (not with-corrections?) (->> (partition-by :vt)
                                              (map first)))))))

;;;; IndexSnapshot

(declare new-kv-index-snapshot)

(defn- advance-iterator-to-hash-cache-value [i value-buffer]
  (let [hash-cache-prefix-key (encode-hash-cache-key-to (.get seek-buffer-tl) value-buffer)
        found-k (kv/seek i hash-cache-prefix-key)]
    (and found-k
         (mem/buffers=? found-k hash-cache-prefix-key (.capacity hash-cache-prefix-key)))))

<<<<<<< HEAD
(deftype SortedListSet [^List vs]
  NavigableSet
  (tailSet [_ k]
    (let [i (Collections/binarySearch vs k mem/buffer-comparator)]
      (SortedListSet. (.subList vs
                                (if (neg? i)
                                  (dec (- i))
                                  i)
                                (.size vs)))))
  (contains [_ k]
    (not (neg? (Collections/binarySearch vs k mem/buffer-comparator))))
  (first [_]
    (.get vs 0))
  (isEmpty [_]
    (.isEmpty vs))
  (iterator [_]
    (.iterator vs))
  (size [_]
    (.size vs)))

(defn- cav-cache-lookup ^java.util.NavigableSet [cav-cache cache-i ^DirectBuffer eid-value-buffer
                                                 ^DirectBuffer content-hash-buffer ^DirectBuffer attr-buffer]
  (cache/compute-if-absent cav-cache
                           (MapEntry/create content-hash-buffer attr-buffer)
                           (fn [_]
                             (MapEntry/create (mem/ensure-on-heap content-hash-buffer)
                                              (mem/ensure-on-heap attr-buffer)))
                           (fn [_]
                             (let [vs (ArrayList.)
                                   prefix (encode-ecav-key-to nil
                                                              eid-value-buffer
                                                              content-hash-buffer
                                                              attr-buffer)
                                   i (new-prefix-kv-iterator cache-i prefix)]
                               (loop [k (kv/seek i prefix)]
                                 (when k
                                   (let [v (key-suffix k (.capacity prefix))
                                         v (mem/ensure-on-heap v)]
                                     (.add vs v)
                                     (recur (kv/next i)))))
                               (->SortedListSet vs)))))

(defn- step-fn [i k-fn seek-k]
  ((fn step [^DirectBuffer k]
     (when k
       (if-let [k (k-fn k)]
         (cons k (lazy-seq (step (kv/next i))))
         (recur (kv/next i)))))
   (kv/seek i seek-k)))

(defn- latest-completed-tx-i [i]
  (some-> (kv/seek i tx-time-mapping-prefix)
          decode-tx-time-mapping-key-from))

(defn latest-completed-tx [kv-store]
  (with-open [snapshot (kv/new-snapshot kv-store)
              i (-> (kv/new-iterator snapshot)
                    (new-prefix-kv-iterator tx-time-mapping-prefix))]
    (latest-completed-tx-i i)))

(defrecord KvIndexSnapshot [snapshot
                            close-snapshot?
                            level-1-iterator-delay
                            level-2-iterator-delay
                            entity-as-of-iterator-delay
                            decode-value-iterator-delay
                            cache-iterator-delay
                            nested-index-snapshot-state
                            cav-cache
                            ^Map temp-hash-cache
                            ^AtomicBoolean closed?]
  Closeable
  (close [_]
    (when (.compareAndSet closed? false true)
      (doseq [nested-index-snapshot @nested-index-snapshot-state]
        (cio/try-close nested-index-snapshot))
      (doseq [i [level-1-iterator-delay level-2-iterator-delay entity-as-of-iterator-delay decode-value-iterator-delay cache-iterator-delay]
              :when (realized? i)]
        (cio/try-close @i))
      (when close-snapshot?
        (cio/try-close snapshot))))

  db/IndexSnapshot
  (av [_ a min-v]
    (let [attr-buffer (c/->id-buffer a)
          prefix (encode-av-key-to nil attr-buffer)
          i (new-prefix-kv-iterator @level-1-iterator-delay prefix)]
      (some->> (encode-av-key-to (.get seek-buffer-tl)
                                 attr-buffer
                                 (buffer-or-value-buffer min-v))
               (step-fn i #(key-suffix % (.capacity prefix))))))

  (ave [_ a v min-e entity-resolver-fn]
    (let [attr-buffer (c/->id-buffer a)
          value-buffer (buffer-or-value-buffer v)
          prefix (encode-ave-key-to nil attr-buffer value-buffer)
          i (new-prefix-kv-iterator @level-2-iterator-delay prefix)]
      (some->> (encode-ave-key-to (.get seek-buffer-tl)
                                  attr-buffer
                                  value-buffer
                                  (buffer-or-value-buffer min-e))
               (step-fn i #(let [eid-value-buffer (key-suffix % (.capacity prefix))]
                             (when-let [content-hash-buffer (entity-resolver-fn eid-value-buffer)]
                               (when-let [vs (cav-cache-lookup cav-cache @cache-iterator-delay
                                                               eid-value-buffer content-hash-buffer attr-buffer)]
                                 (when (.contains vs value-buffer)
                                   eid-value-buffer))))))))

  (ae [_ a min-e]
    (let [attr-buffer (c/->id-buffer a)
          prefix (encode-ae-key-to nil attr-buffer)
          i (new-prefix-kv-iterator @level-1-iterator-delay prefix)]
      (some->> (encode-ae-key-to (.get seek-buffer-tl)
                                 attr-buffer
                                 (buffer-or-value-buffer min-e))
               (step-fn i #(key-suffix % (.capacity prefix))))))

  (aev [_ a e min-v entity-resolver-fn]
    (let [attr-buffer (c/->id-buffer a)
          eid-value-buffer (buffer-or-value-buffer e)]
      (when-let [content-hash-buffer (entity-resolver-fn eid-value-buffer)]
        (when-let [vs (cav-cache-lookup cav-cache @cache-iterator-delay
                                        eid-value-buffer content-hash-buffer attr-buffer)]
          (.tailSet vs (buffer-or-value-buffer min-v))))))

  (entity-as-of-resolver [this eid valid-time tx-id]
    (assert tx-id)
    (let [i @entity-as-of-iterator-delay
          prefix-size (+ c/index-id-size c/id-size)
          eid (if (instance? DirectBuffer eid)
                (if (c/id-buffer? eid)
                  eid
                  (db/decode-value this eid))
                eid)
          eid-buffer (c/->id-buffer eid)
          seek-k (encode-bitemp-key-to (.get seek-buffer-tl)
                                       eid-buffer
                                       valid-time
                                       tx-id
                                       nil)]
      (loop [k (kv/seek i seek-k)]
        (when (and k (mem/buffers=? seek-k k prefix-size))
          (if (<= (compare (decode-bitemp-key-as-tx-id-from k) tx-id) 0)
            (let [v (kv/value i)]
              (when-not (mem/buffers=? c/nil-id-buffer v)
                v))
            (if morton/*use-space-filling-curve-index?*
              (let [seek-z (encode-entity-tx-z-number valid-time tx-id)]
                (when-let [[k v] (find-entity-tx-within-range-with-highest-valid-time i seek-z morton/z-max-mask eid-buffer nil)]
                  (when-not (= ::deleted-entity k)
                    (c/->id-buffer (.content-hash ^EntityTx v)))))
              (recur (kv/next i))))))))

  (entity-as-of [_ eid valid-time tx-id]
    (assert tx-id)
    (let [i @entity-as-of-iterator-delay
          prefix-size (+ c/index-id-size c/id-size)
          eid-buffer (c/->id-buffer eid)
          seek-k (encode-bitemp-key-to (.get seek-buffer-tl)
                                       eid-buffer
                                       valid-time
                                       tx-id
                                       nil)]
      (loop [k (kv/seek i seek-k)]
        (when (and k (mem/buffers=? seek-k k prefix-size))
          (let [entity-tx (safe-entity-tx (decode-bitemp-key-from k))
                v (kv/value i)]
            (if (<= (compare (.tx-id entity-tx) tx-id) 0)
              (cond-> entity-tx
                (not (mem/buffers=? c/nil-id-buffer v)) (enrich-entity-tx v))
              (if morton/*use-space-filling-curve-index?*
                (let [seek-z (encode-entity-tx-z-number valid-time tx-id)]
                  (when-let [[_ v] (find-entity-tx-within-range-with-highest-valid-time i seek-z morton/z-max-mask eid-buffer nil)]
                    v))
                (recur (kv/next i)))))))))

  (entity-history [_ eid sort-order opts]
    (let [i @entity-as-of-iterator-delay
          entity-history-seq (case sort-order
                               :asc entity-history-seq-ascending
                               :desc entity-history-seq-descending)]
      (entity-history-seq i eid opts)))

  (decode-value [_ value-buffer]
    (assert (some? value-buffer))
    (if (c/can-decode-value-buffer? value-buffer)
      (c/decode-value-buffer value-buffer)
      (or (.get temp-hash-cache value-buffer)
          (let [i @decode-value-iterator-delay]
            (when (advance-iterator-to-hash-cache-value i value-buffer)
              (cio/with-nippy-thaw-all
                (some-> (kv/value i) (mem/<-nippy-buffer))))))))

  (encode-value [_ value]
    (let [value-buffer (c/->value-buffer value)]
      (when (and (not (c/can-decode-value-buffer? value-buffer))
                 (not (advance-iterator-to-hash-cache-value @decode-value-iterator-delay value-buffer)))
        (.put temp-hash-cache (mem/ensure-on-heap value-buffer) value))
      value-buffer))

  (resolve-tx [_ {:crux.tx/keys [tx-time tx-id] :as tx}]
    (with-open [i (-> (kv/new-iterator snapshot)
                      (new-prefix-kv-iterator tx-time-mapping-prefix))]
      (let [latest-tx (latest-completed-tx-i i)]
        (cond
          (= tx latest-tx) tx

          tx-time (if (or (nil? latest-tx) (pos? (compare tx-time (:crux.tx/tx-time latest-tx))))
                    (throw (err/node-out-of-sync {:requested tx, :available latest-tx}))

                    (let [found-tx (some-> (kv/seek i (encode-tx-time-mapping-key-to (.get seek-buffer-tl) tx-time tx-id))
                                           decode-tx-time-mapping-key-from)]
                      (if (and tx-id (not= tx-id (:crux.tx/tx-id found-tx)))
                        (throw (err/illegal-arg :tx-id-mismatch
                                                {::err/message "Mismatching tx-id for tx-time"
                                                 :requested tx
                                                 :available found-tx}))

                        (do
                          (when (and found-tx (not= tx-time (:crux.tx/tx-time found-tx)))
                            (let [next-tx (some-> (kv/prev i)
                                                  decode-tx-time-mapping-key-from)]
                              (when (= tx-time (:crux.tx/tx-time next-tx))
                                (throw (err/illegal-arg :tx-id-mismatch
                                                        {::err/message "Mismatching tx-id for tx-time"
                                                         :requested tx
                                                         :available next-tx})))))

                          {:crux.tx/tx-time tx-time
                           :crux.tx/tx-id (:crux.tx/tx-id found-tx)}))))

          tx-id (if (or (nil? latest-tx) (> ^long tx-id ^long (:crux.tx/tx-id latest-tx)))
                  (throw (err/node-out-of-sync {:requested tx, :available latest-tx}))
                  ;; TODO find corresponding tx-time?
                  tx)

          :else latest-tx))))

  (open-nested-index-snapshot [_]
    (let [nested-index-snapshot (new-kv-index-snapshot snapshot false cav-cache temp-hash-cache)]
      (swap! nested-index-snapshot-state conj nested-index-snapshot)
      nested-index-snapshot))

  db/IndexMeta
  (-read-index-meta [_ k not-found]
    (read-meta-snapshot snapshot k not-found)))

(defn- new-kv-index-snapshot [snapshot close-snapshot? cav-cache temp-hash-cache]
  (->KvIndexSnapshot snapshot
                     close-snapshot?
                     (delay (kv/new-iterator snapshot))
                     (delay (kv/new-iterator snapshot))
                     (delay (kv/new-iterator snapshot))
                     (delay (kv/new-iterator snapshot))
                     (delay (kv/new-iterator snapshot))
                     (atom [])
                     cav-cache
                     temp-hash-cache
                     (AtomicBoolean.)))

;;;; IndexStore

(defn- ->content-idx-kvs [docs]
  (let [attr-bufs (->> (into #{} (mapcat keys) (vals docs))
                       (into {} (map (juxt identity c/->id-buffer))))]
    (->> (for [[content-hash doc] docs
               :let [id (:crux.db/id doc)
                     eid-value-buffer (c/->value-buffer id)
                     content-hash (c/->id-buffer content-hash)]
               [a v] doc
               :let [a (get attr-bufs a)]
               v (c/vectorize-value v)
               :let [value-buffer (c/->value-buffer v)]
               :when (pos? (.capacity value-buffer))]
           (cond-> [(MapEntry/create (encode-av-key-to nil a value-buffer) mem/empty-buffer)
                    (MapEntry/create (encode-ave-key-to nil a value-buffer eid-value-buffer) mem/empty-buffer)
                    (MapEntry/create (encode-ae-key-to nil a eid-value-buffer) mem/empty-buffer)
                    (MapEntry/create (encode-ecav-key-to nil eid-value-buffer content-hash a value-buffer) mem/empty-buffer)]
             (not (c/can-decode-value-buffer? value-buffer))
             (conj (MapEntry/create (encode-hash-cache-key-to nil value-buffer eid-value-buffer) (mem/->nippy-buffer v)))))
         (apply concat))))

(defrecord KvIndexStoreTx [persistent-kv-store transient-kv-store tx fork-at !evicted-eids cav-cache temp-hash-cache]
  db/IndexStoreTx
  (index-docs [_ docs]
    (let [crux-db-id (c/->id-buffer :crux.db/id)
          docs (with-open [persistent-kv-snapshot (kv/new-snapshot persistent-kv-store)
                           transient-kv-snapshot (kv/new-snapshot transient-kv-store)]
                 (->> docs
                      (into {} (remove (fn [[content-hash doc]]
                                         (let [eid-value (c/->value-buffer (:crux.db/id doc))
                                               k (encode-ecav-key-to (.get seek-buffer-tl)
                                                                     eid-value
                                                                     (c/->id-buffer content-hash)
                                                                     crux-db-id
                                                                     eid-value)]
                                           (or (kv/get-value persistent-kv-snapshot k)
                                               (kv/get-value transient-kv-snapshot k))))))
                      not-empty))
          content-idx-kvs (->content-idx-kvs docs)]
      (some->> (seq content-idx-kvs) (kv/store transient-kv-store))
      {:bytes-indexed (->> content-idx-kvs (transduce (comp (mapcat seq) (map mem/capacity)) +))
       :indexed-docs docs}))

  (unindex-eids [_ eids]
    (when (seq eids)
      (swap! !evicted-eids into eids)

      (with-open [p-snapshot (kv/new-snapshot persistent-kv-store)
                  t-snapshot (kv/new-snapshot transient-kv-store)
                  pi (kv/new-iterator p-snapshot)
                  ti (kv/new-iterator t-snapshot)]
        (letfn [(merge-idxs [k]
                  (fork/merge-seqs (all-keys-in-prefix pi k)
                                   (all-keys-in-prefix ti k)))]
          (let [bitemp-ks (->> (for [eid eids
                                     :let [eid-buf (c/->id-buffer eid)]
                                     k (concat (merge-idxs (encode-bitemp-key-to nil eid-buf))
                                               (merge-idxs (encode-bitemp-z-key-to nil eid-buf)))]
                                 k)
                               (into #{}))

                ecav-ks (->> (for [eid eids
                                   :let [eid-buf (c/->value-buffer eid)]
                                   ecav-key (merge-idxs (encode-ecav-key-to nil eid-buf))]
                               [eid eid-buf ecav-key (decode-ecav-key-from ecav-key (.capacity eid-buf))])
                             (vec))

                tombstones (->> (for [[eid _ _ ^Quad quad] ecav-ks]
                                  (MapEntry/create (.content-hash quad) eid))
                                (into {})
                                (into {} (map (fn [[ch eid]]
                                                (MapEntry/create ch
                                                                 {:crux.db/id (c/new-id eid)
                                                                  :crux.db/evicted? true})))))
                content-ks (->> (for [[_ eid-buf ecav-key ^Quad quad] ecav-ks
                                      :let [attr-buf (c/->id-buffer (.attr quad))
                                            value-buf ^DirectBuffer (.value quad)
                                            sole-av? (empty? (->> (merge-idxs (encode-ave-key-to nil attr-buf value-buf))
                                                                  (remove (comp #(mem/buffers=? % eid-buf)
                                                                                #(decode-ave-key->e-from % (.capacity value-buf))))))]

                                      k (cond-> [ecav-key
                                                 (encode-ae-key-to nil attr-buf eid-buf)
                                                 (encode-ave-key-to nil attr-buf value-buf eid-buf)]
                                          sole-av? (conj (encode-av-key-to nil attr-buf value-buf))

                                          (c/can-decode-value-buffer? value-buf)
                                          (conj (encode-hash-cache-key-to nil value-buf eid-buf)))]
                                  k)
                                (into #{}))]

            (run! #(cache/evict cav-cache %) (keys tombstones))

            (kv/store transient-kv-store
                      (for [k (concat bitemp-ks content-ks)]
                        (MapEntry/create k nil)))

            {:tombstones tombstones})))))

  (index-entity-txs [_ entity-txs]
    (let [{:crux.tx/keys [tx-id tx-time]} tx]
      (kv/store transient-kv-store
                (->> (conj (mapcat etx->kvs entity-txs)
                           (MapEntry/create (encode-tx-time-mapping-key-to nil tx-time tx-id) mem/empty-buffer))
                     (sort-by key mem/buffer-comparator)))))

  (commit-index-tx [_]
    (with-open [snapshot (kv/new-snapshot transient-kv-store)]
      (kv/store persistent-kv-store (seq snapshot))))

  (abort-index-tx [_]
    (with-open [snapshot (kv/new-snapshot transient-kv-store)]
      (let [{:crux.tx/keys [tx-id tx-time]} tx]
        ;; we still put the ECAV KVs in so that we can keep track of what we need to evict later
        ;; the bitemp indices will ensure these are never returned in queries
        (kv/store persistent-kv-store
                  (conj (->> (seq snapshot)
                             (filter (fn [[^DirectBuffer k-buf v-buf]]
                                       (= c/ecav-index-id (.getByte k-buf 0)))))

                        (MapEntry/create (encode-failed-tx-id-key-to nil tx-id) mem/empty-buffer)
                        (MapEntry/create (encode-tx-time-mapping-key-to nil tx-time tx-id) mem/empty-buffer))))))

  db/IndexSnapshotFactory
  (open-index-snapshot [_]
    (fork/->MergedIndexSnapshot (-> (new-kv-index-snapshot (kv/new-snapshot persistent-kv-store) true
                                                           cav-cache canonical-buffer-cache temp-hash-cache)
                                    (fork/->CappedIndexSnapshot (:crux.db/valid-time fork-at)
                                                                (get fork-at :crux.tx/tx-id (:crux.tx/tx-id tx))))
                                (new-kv-index-snapshot (kv/new-snapshot transient-kv-store) true
                                                       cav-cache canonical-buffer-cache temp-hash-cache)
                                @!evicted-eids)))

(defrecord KvIndexStore [kv-store cav-cache]
  db/IndexStore
  (begin-index-tx [_ tx fork-at]
    (->KvIndexStoreTx kv-store (mut-kv/->mutable-kv-store) tx fork-at
                      (atom #{})
                      (nop-cache/->nop-cache {}) (nop-cache/->nop-cache {}) (HashMap.)))

  ;; TODO, make use of this fn in unindex-eids
  (exclusive-avs [_ eids]
    (with-open [snapshot (kv/new-snapshot kv-store)
                ecav-i (kv/new-iterator snapshot)
                av-i (kv/new-iterator snapshot)]
      (doall
       (for [eid eids
             :let [eid-value-buffer (c/->value-buffer eid)]
             ecav-key (all-keys-in-prefix ecav-i (encode-ecav-key-to nil eid-value-buffer))
             :let [quad ^Quad (decode-ecav-key-from ecav-key (.capacity eid-value-buffer))
                   attr-buf (c/->id-buffer (.attr quad))
                   value-buf ^DirectBuffer (.value quad)
                   ave-k (encode-ave-key-to nil attr-buf value-buf)]
             :when (empty? (->> (all-keys-in-prefix av-i ave-k)
                                (remove (comp #(= eid-value-buffer %)
                                              #(decode-ave-key->e-from % (.capacity value-buf))))))]
         [attr-buf value-buf]))))

  (store-index-meta [_ k v]
    (store-meta kv-store k v))

  (latest-completed-tx [_]
    (latest-completed-tx kv-store))

  (tx-failed? [_ tx-id]
    (with-open [snapshot (kv/new-snapshot kv-store)]
      (some? (kv/get-value snapshot (encode-failed-tx-id-key-to nil tx-id)))))

  db/IndexMeta
  (-read-index-meta [_ k not-found]
    (read-meta kv-store k not-found))

  db/IndexSnapshotFactory
  (open-index-snapshot [_]
    (new-kv-index-snapshot (kv/new-snapshot kv-store) true cav-cache (HashMap.)))

  status/Status
  (status-map [this]
    {:crux.index/index-version (current-index-version kv-store)
     :crux.doc-log/consumer-state (db/read-index-meta this :crux.doc-log/consumer-state)
     :crux.tx-log/consumer-state (db/read-index-meta this :crux.tx-log/consumer-state)}))

(defn ->kv-index-store {::sys/deps {:kv-store 'crux.mem-kv/->kv-store
                                    :cav-cache 'crux.cache/->cache}
                        ::sys/args {:skip-index-version-bump {:spec (s/tuple int? int?)
                                                              :doc "Skip an index version bump. For example, to skip from v10 to v11, specify [10 11]"}}}
  [{:keys [kv-store cav-cache] :as opts}]
  (check-and-store-index-version opts)
  (->KvIndexStore kv-store cav-cache))
