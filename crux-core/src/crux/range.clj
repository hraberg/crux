(ns crux.range
  (:import [org.agrona DirectBuffer MutableDirectBuffer]
           org.roaringbitmap.longlong.Roaring64Bitmap
           java.nio.ByteOrder
           java.nio.charset.StandardCharsets
           [java.util NavigableSet TreeSet])
  (:require [clojure.string :as str]
            [crux.codec :as c]
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

(defn ->fs ^crux.range.FilteredSet []
  (FilteredSet. (TreeSet. mem/buffer-comparator) (Roaring64Bitmap.)))

(defn fs-add ^crux.range.FilteredSet [^FilteredSet fs ^DirectBuffer k]
  (.add ^NavigableSet (.s fs) k)
  (insert-key (.bm fs) (buffer->long k))
  fs)

(defn fs-seek-potential-k ^org.agrona.DirectBuffer [^FilteredSet fs ^DirectBuffer k]
  (let [k-long (buffer->long k)]
    (if (range-may-contain? (.bm fs) k-long)
      k
      (when-let [next-k (seek-bitmap (.bm fs) k-long)]
        (long->buffer next-k)))))

(defn fs-seek ^org.agrona.DirectBuffer [^FilteredSet fs ^DirectBuffer k]
  (when-let [k-probe (fs-seek-potential-k fs k)]
    (when-let [found (.ceiling ^NavigableSet (.s fs) k-probe)]
      (when-not (= k found)
        (insert-empty-range (.bm fs)
                            (buffer->long k)
                            (buffer->long found)))
      found)))

(defn fs-contains? [^FilteredSet fs ^DirectBuffer k]
  (let [k-long (buffer->long k)]
    (and (range-may-contain? (.bm fs) k-long)
         (= k (fs-seek fs k)))))

(comment
  (let [fs ^FilteredSet (reduce fs-add
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
    (assert (= [604327874509406208 605735249392959488]
               (iterator-seq (.iterator ^Roaring64Bitmap (.bm fs)))))
    (assert (= "f" (c/decode-value-buffer (fs-seek fs (c/->value-buffer "b")))))
    (assert (= [604327874509406208 605735249392959488]
               (iterator-seq (.iterator ^Roaring64Bitmap (.bm fs)))))
    (fs-add fs (c/->value-buffer "c"))
    (fs-add fs (c/->value-buffer "g"))
    (assert (true? (fs-contains? fs (c/->value-buffer "c"))))
    (assert (= [604327874509406208 604890824462827520
                604890824462827521 605735249392959488]
               (iterator-seq (.iterator ^Roaring64Bitmap (.bm fs)))))
    (assert (= "top" (c/decode-value-buffer (fs-seek fs (c/->value-buffer "t")))))
    (assert (= "top" (c/decode-value-buffer (fs-seek fs (c/->value-buffer "to")))))
    (assert (false? (fs-contains? fs (c/->value-buffer "too"))))
    (assert (true? (fs-contains? fs (c/->value-buffer "toy"))))
    (= [604327874509406208 604890824462827520
        604890824462827521 605735249392959488
        609675899066908672 609799534012268544]
       (iterator-seq (.iterator ^Roaring64Bitmap (.bm fs))))

    (assert (= ["c",
                "f"
                "far"
                "fast"
                "g"
                "s"
                "top"
                "toy"
                "trie"] (mapv c/decode-value-buffer (.s fs))))))
