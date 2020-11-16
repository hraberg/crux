(ns crux.range
  (:import [org.agrona DirectBuffer MutableDirectBuffer]
           org.roaringbitmap.longlong.Roaring64Bitmap
           java.nio.ByteOrder
           java.nio.charset.StandardCharsets
           [java.util NavigableSet TreeSet])
  (:require [clojure.string :as str]
            [crux.memory :as mem]))

;; "Adaptive Range Filters for Cold Data: Avoiding Trips to Siberia"
;; http://www.vldb.org/pvldb/vol6/p1714-kossmann.pdf

;; "SuRF: Practical Range Query Filtering with Fast Succinct Tries"
;; https://www.cs.cmu.edu/~pavlo/papers/mod601-zhangA-hm.pdf

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

(defn long->prefix ^String [^long x]
  (str/replace (String. (mem/->on-heap (long->buffer x)) StandardCharsets/UTF_8) #"\u0000*$" ""))

(defn str->long ^long [x]
  (buffer->long (mem/as-buffer (.getBytes (str x) StandardCharsets/UTF_8))))

(defn seek-bitmap [^Roaring64Bitmap bm ^long k]
  (let [rank (.rankLong bm k)]
    (when (< rank (.getLongCardinality bm))
      (.select bm rank))))

(defn range-may-contain? [^Roaring64Bitmap bm ^long probe]
  (even? (.rankLong bm probe)))

(defn insert-empty-range ^org.roaringbitmap.longlong.Roaring64Bitmap [^Roaring64Bitmap bm ^long start ^long end]
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

(defn insert-key ^org.roaringbitmap.longlong.Roaring64Bitmap [^Roaring64Bitmap bm ^long k]
  (if (range-may-contain? bm k)
    bm
    (doto bm
      (.flip k)
      (.flip (unchecked-inc k)))))

(deftype FilteredSet [^NavigableSet s ^Roaring64Bitmap bm])

(defn ->fs
  (^crux.range.FilteredSet [] (->fs (TreeSet.)))
  (^crux.range.FilteredSet [^NavigableSet s]
   (FilteredSet. s (Roaring64Bitmap.))))

(defn fs-add ^crux.range.FilteredSet [^FilteredSet fs k]
  (.add ^NavigableSet (.s fs) k)
  (insert-key (.bm fs) (str->long k))
  fs)

(defn fs-seek-potential-k [^FilteredSet fs k]
  (let [k-long (str->long k)]
    (if (range-may-contain? (.bm fs) k-long)
      k
      (when-let [next-k (seek-bitmap (.bm fs) k-long)]
        (long->prefix next-k)))))

(defn fs-seek [^FilteredSet fs k]
  (when-let [k-probe (fs-seek-potential-k fs k)]
    (when-let [found (first (.tailSet ^NavigableSet (.s fs) k-probe))]
      (when-not (= k found)
        (insert-empty-range (.bm fs)
                            (str->long k)
                            (str->long found)))
      found)))

(defn fs-contains? [^FilteredSet fs k]
  (let [k-long (str->long k)]
    (and (range-may-contain? (.bm fs) k-long)
         (= k (fs-seek fs k)))))

(comment
  (let [fs (->fs (TreeSet. ["f",
                            "far",
                            "fast",
                            "s",
                            "top",
                            "toy",
                            "trie"]))]
    (assert (= "f" (fs-seek fs "a")))
    (assert (= ["a" "f"] (map long->prefix (iterator-seq (.iterator ^Roaring64Bitmap (.bm fs))))))
    (assert (= "f" (fs-seek fs "b")))
    (assert (= ["a" "f"] (map long->prefix (iterator-seq (.iterator ^Roaring64Bitmap (.bm fs))))))
    (fs-add fs "c")
    (fs-add fs "g")
    (assert (true? (fs-contains? fs "c")))
    (assert (= ["a" "c"
                (long->prefix (unchecked-inc (str->long "c")))  "f"]
               (map long->prefix (iterator-seq (.iterator ^Roaring64Bitmap (.bm fs))))))
    (assert (= "top" (fs-seek fs "t")))
    (assert (= "top" (fs-seek fs "to")))
    (assert (false? (fs-contains? fs "too")))
    (assert (true? (fs-contains? fs "toy")))
    (assert (= ["a" "c"
                (long->prefix (unchecked-inc (str->long "c")))  "f"
                "t" "top"]
               (map long->prefix (iterator-seq (.iterator ^Roaring64Bitmap (.bm fs))))))

    (assert (= ["c",
                "f",
                "far",
                "fast",
                "g",
                "s",
                "top",
                "toy",
                "trie"] (vec (.s fs))))))
