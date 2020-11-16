(ns crux.range
  (:import [org.agrona DirectBuffer MutableDirectBuffer]
           org.roaringbitmap.longlong.Roaring64NavigableMap
           java.nio.ByteOrder
           java.nio.charset.StandardCharsets
           [java.util NavigableSet TreeSet])
  (:require [clojure.string :as str]
            [crux.memory :as mem]))

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
               (bit-or acc (bit-shift-left (.getByte b n) shift)))))))

(defn long->buffer ^org.agrona.DirectBuffer [^long x]
  (doto ^MutableDirectBuffer (mem/allocate-buffer Long/BYTES)
    (.putLong 0 x ByteOrder/BIG_ENDIAN)))

(defn seek-bitmap [^Roaring64NavigableMap bm ^long k]
  (let [rank (.rankLong bm k)]
    (when (< rank (.getLongCardinality bm))
      (.select bm rank))))

(defn range-may-contain? [^Roaring64NavigableMap bm ^long probe]
  (even? (.rankLong bm probe)))

(defn insert-empty-range ^org.roaringbitmap.longlong.Roaring64NavigableMap [^Roaring64NavigableMap bm ^long start ^long end]
  (if (and (.contains bm end) (not (range-may-contain? bm start)))
    bm
    (do (dotimes [n (- (.rankLong bm end) (.rankLong bm start))]
          (when (< n (.getLongCardinality bm))
            (let [candidate (.select bm n)]
              (when (and (not (pos? (Long/compareUnsigned start candidate)))
                         (not (pos? (Long/compareUnsigned candidate end))))
                (.removeLong bm candidate)))))
        (doto bm
          (.addLong start)
          (.addLong end)))))

(defn insert-key ^org.roaringbitmap.longlong.Roaring64NavigableMap [^Roaring64NavigableMap bm ^long k]
  (if (range-may-contain? bm k)
    bm
    (doto bm
      (.flip k)
      (.flip (unchecked-inc k)))))

(deftype FilteredSet [^NavigableSet s ^Roaring64NavigableMap bm])

(defn ->fs []
  (FilteredSet. (TreeSet.) (Roaring64NavigableMap.)))

(defn fs-add ^crux.memory.FilteredSet [^FilteredSet fs k]
  (.add ^NavigableSet (.s fs) k)
  (insert-key (.bm fs) (buffer->long (mem/as-buffer (.getBytes (str k) StandardCharsets/UTF_8))))
  fs)

(defn fs-seek-potential-k [^FilteredSet fs k]
  (let [k-long (buffer->long (mem/as-buffer (.getBytes (str k) StandardCharsets/UTF_8)))]
    (if (range-may-contain? (.bm fs) k-long)
      k
      (when-let [next-k (seek-bitmap (.bm fs) k-long)]
        (str/replace (String. (mem/->on-heap (long->buffer next-k)) StandardCharsets/UTF_8) "\u0000" "")))))

(defn fs-seek [^FilteredSet fs k]
  (when-let [k-probe (fs-seek-potential-k fs k)]
    (when-let [found (first (.tailSet ^NavigableSet (.s fs) k-probe))]
      (when-not (= k found)
        (insert-empty-range (.bm fs)
                            (buffer->long (mem/as-buffer (.getBytes (str k) StandardCharsets/UTF_8)))
                            (buffer->long (mem/as-buffer (.getBytes (str found) StandardCharsets/UTF_8)))))
      found)))

(defn fs-contains? [^FilteredSet fs k]
  (let [k-long (buffer->long (mem/as-buffer (.getBytes (str k) StandardCharsets/UTF_8)))]
    (and (range-may-contain? (.bm fs) k-long)
         (= k (fs-seek fs k)))))
