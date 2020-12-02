(ns crux.range
  (:import [org.agrona DirectBuffer MutableDirectBuffer]
           [org.roaringbitmap.longlong Roaring64Bitmap Roaring64NavigableMap]
           clojure.lang.PersistentQueue
           java.nio.ByteOrder
           java.nio.charset.StandardCharsets
           [java.util ArrayList Arrays HashMap Map NavigableSet TreeSet])
  (:require [clojure.string :as str]
            [crux.codec :as c]
            [crux.memory :as mem]))

;; "Adaptive Range Filters for Cold Data: Avoiding Trips to Siberia"
;; http://www.vldb.org/pvldb/vol6/p1714-kossmann.pdf

;; "SuRF: Practical Range Query Filtering with Fast Succinct Tries"
;; https://www.cs.cmu.edu/~pavlo/papers/mod601-zhangA-hm.pdf

(set! *unchecked-math* :warn-on-boxed)

(defn buffer->long ^long [^DirectBuffer b]
  (if (>= (.capacity b) Long/BYTES)
    (.getLong b 0 ByteOrder/BIG_ENDIAN)
    (loop [n 0
           shift (- Long/SIZE Byte/SIZE)
           acc 0]
      (if (= n (.capacity b))
        acc
        (recur (inc n)
               (- shift Byte/SIZE)
               (bit-or acc (bit-shift-left (Byte/toUnsignedLong (.getByte b n)) shift)))))))

(defn long->buffer ^org.agrona.DirectBuffer [^long x]
  (let [b (doto ^MutableDirectBuffer (mem/allocate-buffer Long/BYTES)
            (.putLong 0 x ByteOrder/BIG_ENDIAN))]
    (loop [n Long/BYTES]
      (cond
        (zero? n)
        b
        (not (zero? (.getByte b (dec n))))
        (mem/limit-buffer b n)
        :else
        (recur (dec n))))))

(defn seek-higher [^Roaring64NavigableMap bm ^long k]
  (let [rank (.rankLong bm k)]
    (when (neg? (Long/compareUnsigned rank (.getLongCardinality bm)))
      (.select bm rank))))

(defn range-may-contain? [^Roaring64NavigableMap bm ^long k]
  (even? (.rankLong bm k)))

(defn insert-key ^org.roaringbitmap.longlong.Roaring64NavigableMap [^Roaring64NavigableMap bm ^long k]
  (if (range-may-contain? bm k)
    bm
    (doto bm
      (.flip k)
      (.flip (unchecked-inc k)))))

(defn insert-empty-range ^org.roaringbitmap.longlong.Roaring64NavigableMap [^Roaring64NavigableMap bm ^long start ^long end]
  (when-not (or (and (.contains bm end) (not (range-may-contain? bm start)))
                (= start end))
    (let [start-rank (.rankLong bm start)]
      (dotimes [n (unchecked-subtract (.rankLong bm end) start-rank)]
        (let [n (unchecked-add n (unchecked-dec start-rank))]
          (when (neg? (Long/compareUnsigned n (.getLongCardinality bm)))
            (let [candidate (.select bm n)]
              (when (and (not (pos? (Long/compareUnsigned start candidate)))
                         (neg? (Long/compareUnsigned candidate end)))
                (.removeLong bm candidate)))))))
    (doto bm
      (.addLong start)
      (.addLong end))))

(defn ->range-filter ^org.roaringbitmap.longlong.Roaring64NavigableMap []
  (Roaring64NavigableMap.))

(deftype FilteredSet [^NavigableSet s ^Roaring64NavigableMap bm ^Roaring64NavigableMap cr ^Map cache])

(defn ->fs
  (^crux.range.FilteredSet []
   (->fs (TreeSet. mem/buffer-comparator)))
  (^crux.range.FilteredSet [^NavigableSet s]
   (FilteredSet. s (->range-filter) (->range-filter) (HashMap.))))

(defn fs-add ^crux.range.FilteredSet [^FilteredSet fs ^DirectBuffer k]
  (.add ^NavigableSet (.s fs) k)
  (insert-key (.bm fs) (buffer->long k))
  fs)

(defn fs-seek-potential-k ^org.agrona.DirectBuffer [^FilteredSet fs ^DirectBuffer k]
  (let [k-long (buffer->long k)]
    (if (range-may-contain? (.bm fs) k-long)
      k
      (when-let [next-k (seek-higher (.bm fs) k-long)]
        (long->buffer next-k)))))

(defn fs-seek-potential-long ^Long [^FilteredSet fs ^long k-long]
  (if (range-may-contain? (.bm fs) k-long)
    k-long
    (when-let [next-k (seek-higher (.bm fs) k-long)]
      next-k)))

(defn fs-seek ^org.agrona.DirectBuffer [^FilteredSet fs ^DirectBuffer k]
  (when-let [k-probe (fs-seek-potential-k fs k)]
    (let [found (.ceiling ^NavigableSet (.s fs) k-probe)]
      (when-not (= k found)
        (insert-empty-range (.bm fs)
                            (buffer->long k)
                            (if (nil? found)
                              -1
                              (buffer->long found))))
      found)))

(def ^:dynamic *seeks* (atom 0))
(def ^:dynamic *io-cost-ms* 0)

(defn fs-seek-exact ^org.agrona.DirectBuffer [^FilteredSet fs ^DirectBuffer k]
  (let [found (.ceiling ^NavigableSet (.s fs) k)]
    (swap! *seeks* inc)
    (Thread/sleep *io-cost-ms*)
    (when found
      (.addLong ^Roaring64NavigableMap (.cr fs) (buffer->long found)))
    (when-not (= k found)
      (let [start (if k
                    (buffer->long k)
                    0)
            start (long (loop [start start]
                          (if (.contains ^Roaring64NavigableMap (.cr fs) start)
                            (recur (unchecked-inc start))
                            start)))
            end (if (nil? found)
                  -1
                  (buffer->long found))]
        (when (<= start end)
          (insert-empty-range (.bm fs) start end)
          (let [cache ^Map (.cache fs)
                x (.get cache end)]
            (when (or (nil? x) (neg? (mem/compare-buffers found x)))
              (.put ^Map (.cache fs) end found))))))
    found))

(defn fs-contains? [^FilteredSet fs ^DirectBuffer k]
  (and (range-may-contain? (.bm fs) (buffer->long k))
       (= k (fs-seek fs k))))

(defn fs-higher ^org.agrona.DirectBuffer [^FilteredSet fs ^DirectBuffer k]
  #_(fs-seek-exact fs (mem/inc-unsigned-buffer! (mem/copy-to-unpooled-buffer k)))
  (let [k-long (buffer->long k)]
    (if (or (range-may-contain? (.bm fs) k-long)
            (.contains ^Roaring64NavigableMap (.cr fs) k-long))
      (fs-seek-exact fs (mem/inc-unsigned-buffer! (mem/copy-to-unpooled-buffer k)))
      (when-let [next-k (seek-higher (.bm fs) k-long)]
        (.get ^Map (.cache fs) next-k)))))

(defn fs-ceiling ^org.agrona.DirectBuffer [^FilteredSet fs ^DirectBuffer k]
  #_(fs-seek-exact fs k)
  (let [k-long (buffer->long k)]
    (if (or (range-may-contain? (.bm fs) k-long)
            (.contains ^Roaring64NavigableMap (.cr fs) k-long))
      (fs-seek-exact fs k)
      (when-let [next-k (seek-higher (.bm fs) k-long)]
        (.get ^Map (.cache fs) next-k)))))

(defn fs-intersect-sets [xs]
  (let [xs (sort-by #(first (.s ^crux.range.FilteredSet %)) mem/buffer-comparator xs)
        keys (object-array (map #(first (.s ^crux.range.FilteredSet %)) xs))
        sets (object-array xs)
        len (count xs)
        acc (ArrayList.)]
    (loop [n 0
           max-k (aget keys (dec len))]
      (let [k (aget keys n)
            ^FilteredSet s (aget sets n)]
        (if-let [max-k (if (mem/buffers=? k max-k)
                         (do (.add acc k)
                             (fs-higher s k))
                         (fs-ceiling s max-k))]
          (do (aset keys n max-k)
              (recur (rem (inc n) len) max-k))
          acc)))))

(defn intersect-sets [xs]
  (let [xs (sort-by first xs)
        keys (object-array (map first xs))
        sets (object-array xs)
        len (count xs)
        acc (ArrayList.)]
    (loop [n 0
           max-k (aget keys (dec len))]
      (let [k (aget keys n)
            ^NavigableSet s (aget sets n)]
        (if-let [max-k (if (= k max-k)
                         (do (.add acc k)
                             (.higher s k))
                         (.ceiling s max-k))]
          (do (aset keys n max-k)
              (recur (rem (inc n) len) max-k))
          acc)))))

(defn- test-sets [indexes]
  (let [expected (mapv c/decode-value-buffer (intersect-sets (mapv #(.s ^FilteredSet %) indexes)))
        bm-fn (fn []
                (vec (for [[n ^FilteredSet idx] (map-indexed vector indexes)]
                       {:idx-name n
                        :cardinality-bm (.getLongCardinality ^Roaring64NavigableMap (.bm idx))
                        :size-in-bytes-bm (.serializedSizeInBytes ^Roaring64NavigableMap (.bm idx))
                        :cardinality-cr (.getLongCardinality ^Roaring64NavigableMap (.cr idx))
                        :size-in-bytes-cr (.serializedSizeInBytes ^Roaring64NavigableMap (.cr idx))})))
        do-run-fn (fn [run-name]
                    (binding [*seeks* (atom 0)]
                      (let [start-time (System/currentTimeMillis)
                            r (mapv c/decode-value-buffer (fs-intersect-sets indexes))
                            total-time (- (System/currentTimeMillis) start-time)]
                        (assert (= r expected))
                        {:name run-name
                         :time-ms total-time
                         :count [(count r) (count expected)]
                         :match (= r expected)
                         :seeks @*seeks*
                         :range-filters (bm-fn)})))]
    (clojure.pprint/pprint (binding [*io-cost-ms* 1]
                             [(do-run-fn "cold-run")
                              (do-run-fn "warm-run")
                              (do (doseq [idx indexes]
                                    (.clear ^Roaring64NavigableMap (.bm ^FilteredSet idx)))
                                  (do-run-fn "reset-run"))]))))

(comment
  (let [a-idx (->fs (doto (TreeSet. mem/buffer-comparator)
                      (.addAll (map c/->value-buffer (map #(Long/toHexString %) [0 1 3 4 5 6 7 8 9 11 12])))))
        b-idx (->fs (doto (TreeSet. mem/buffer-comparator)
                      (.addAll (map c/->value-buffer (map #(Long/toHexString %) [0 2 6 7 8 9 12])))))
        c-idx (->fs (doto (TreeSet. mem/buffer-comparator)
                      (.addAll (map c/->value-buffer (map #(Long/toHexString %) [2 4 5 8 10 12])))))
        indexes [a-idx b-idx c-idx]]
    (test-sets indexes))

  (let [a-idx (->fs (doto (TreeSet. mem/buffer-comparator)
                      (.addAll (map c/->value-buffer [0 1 3 4 5 6 7 8 9 11 12]))))
        b-idx (->fs (doto (TreeSet. mem/buffer-comparator)
                      (.addAll (map c/->value-buffer [0 2 6 7 8 9 12]))))
        c-idx (->fs (doto (TreeSet. mem/buffer-comparator)
                      (.addAll (map c/->value-buffer [2 4 5 8 10 12]))))
        indexes [a-idx b-idx c-idx]]
    (test-sets indexes))

  (let [a-idx (->fs (doto (TreeSet. mem/buffer-comparator)
                      (.addAll (map c/->value-buffer (map #(format "%08x" %)
                                                          (repeatedly 10000 #(rand-int 100000)))))))
        b-idx (->fs (doto (TreeSet. mem/buffer-comparator)
                      (.addAll (map c/->value-buffer (map #(format "%08x" %)
                                                          (repeatedly 10000 #(rand-int 100000)))))))
        c-idx (->fs (doto (TreeSet. mem/buffer-comparator)
                      (.addAll (map c/->value-buffer (map #(format "%08x" %)
                                                          (repeatedly 10000 #(rand-int 100000)))))))]
    (test-sets [a-idx b-idx c-idx])))

(comment
  (do (let [fs ^FilteredSet (reduce fs-add
                                    (->fs)
                                    (->>  ["f"
                                           "far"
                                           "fast"
                                           "s"
                                           "top"
                                           "toy"
                                           "trie"]
                                          (mapv c/->value-buffer)))]
        (assert (= "f" (c/decode-value-buffer (fs-seek fs (c/->value-buffer "a")))))
        (assert (= [604327874509406208 605735249392959488] ;; "a" "f"
                   (vec (.toArray ^Roaring64NavigableMap (.bm fs)))))
        (assert (= "f" (c/decode-value-buffer (fs-seek fs (c/->value-buffer "b")))))
        (assert (= [604327874509406208 605735249392959488]
                   (vec (.toArray ^Roaring64NavigableMap (.bm fs)))))
        (fs-add fs (c/->value-buffer "c"))
        (fs-add fs (c/->value-buffer "g"))
        (assert (true? (fs-contains? fs (c/->value-buffer "c"))))
        (assert (= [604327874509406208 604890824462827520 ;; "a" "c"
                    604890824462827521 605735249392959488] ;; (unchecked-inc "c") "f"
                   (vec (.toArray ^Roaring64NavigableMap (.bm fs)))))
        (assert (= "top" (c/decode-value-buffer (fs-seek fs (c/->value-buffer "t")))))
        (assert (= "top" (c/decode-value-buffer (fs-seek fs (c/->value-buffer "to")))))
        (assert (false? (fs-contains? fs (c/->value-buffer "too"))))
        (assert (true? (fs-contains? fs (c/->value-buffer "toy"))))
        (assert (= [604327874509406208 604890824462827520 ;; "a" "c"
                    604890824462827521 605735249392959488 ;; (unchecked-inc "c") "f"
                    609675899066908672 609799534012268544]  ;; "t "top"
                   (vec (.toArray ^Roaring64NavigableMap (.bm fs)))))

        (assert (= ["c"
                    "f"
                    "far"
                    "fast"
                    "g"
                    "s"
                    "top"
                    "toy"
                    "trie"] (mapv c/decode-value-buffer (.s fs)))))

      (let [fs ^FilteredSet (reduce fs-add
                                    (->fs)
                                    (->>  [1 2 3 5 6 11]
                                          (mapv c/->value-buffer)))]
        (assert (= 11 (c/decode-value-buffer (fs-seek fs (c/->value-buffer 8)))))
        (assert (= [397583404603801600 397636181161934848]
                   (vec (.toArray ^Roaring64NavigableMap (.bm fs))))))

      (let [fs ^FilteredSet (reduce fs-add
                                    (->fs)
                                    (->>  ["01" "02" "03" "05" "06" "11"]
                                          (mapv c/->value-buffer)))]
        (assert (= "11" (c/decode-value-buffer (fs-seek fs (c/->value-buffer "08")))))
        (assert (= [590598277108334592 590872055503650816] ;; "08" "11"
                   (vec (.toArray ^Roaring64NavigableMap (.bm fs)))))
        (assert (nil? (fs-seek fs (c/->value-buffer "12"))))
        (assert (= [590598277108334592 590872055503650816  ;; "08" "11"
                    590873155015278592 -1] ;; "12" -1
                   (vec (.toArray ^Roaring64NavigableMap (.bm fs))))))))
