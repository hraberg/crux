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

(defn seek-higher [^Roaring64Bitmap bm ^long k]
  (let [rank (.rankLong bm k)]
    (when (< rank (.getLongCardinality bm))
      (.select bm rank))))

(defn range-may-contain? [^Roaring64Bitmap bm ^long k]
  (even? (.rankLong bm k)))

(defn insert-key ^org.roaringbitmap.longlong.Roaring64Bitmap [^Roaring64Bitmap bm ^long k]
  (if (range-may-contain? bm k)
    bm
    (doto bm
      (.flip k)
      (.flip (unchecked-inc k)))))

(defn insert-empty-range ^org.roaringbitmap.longlong.Roaring64Bitmap [^Roaring64Bitmap bm ^long start ^long end]
  (if (or (and (.contains bm end) (not (range-may-contain? bm start)))
          (= start end))
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
      (when-let [next-k (seek-higher (.bm fs) k-long)]
        (long->buffer next-k)))))

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

(defn fs-contains? [^FilteredSet fs ^DirectBuffer k]
  (and (range-may-contain? (.bm fs) (buffer->long k))
       (= k (fs-seek fs k))))

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
                   (vec (.toArray ^Roaring64Bitmap (.bm fs)))))
        (assert (= "f" (c/decode-value-buffer (fs-seek fs (c/->value-buffer "b")))))
        (assert (= [604327874509406208 605735249392959488]
                   (vec (.toArray ^Roaring64Bitmap (.bm fs)))))
        (fs-add fs (c/->value-buffer "c"))
        (fs-add fs (c/->value-buffer "g"))
        (assert (true? (fs-contains? fs (c/->value-buffer "c"))))
        (assert (= [604327874509406208 604890824462827520 ;; "a" "c"
                    604890824462827521 605735249392959488] ;; (unchecked-inc "c") "f"
                   (vec (.toArray ^Roaring64Bitmap (.bm fs)))))
        (assert (= "top" (c/decode-value-buffer (fs-seek fs (c/->value-buffer "t")))))
        (assert (= "top" (c/decode-value-buffer (fs-seek fs (c/->value-buffer "to")))))
        (assert (false? (fs-contains? fs (c/->value-buffer "too"))))
        (assert (true? (fs-contains? fs (c/->value-buffer "toy"))))
        (assert (= [604327874509406208 604890824462827520 ;; "a" "c"
                    604890824462827521 605735249392959488 ;; (unchecked-inc "c") "f"
                    609675899066908672 609799534012268544]  ;; "t "top"
                   (vec (.toArray ^Roaring64Bitmap (.bm fs)))))

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
        (assert (empty? (vec (.toArray ^Roaring64Bitmap (.bm fs))))))

      (let [fs ^FilteredSet (reduce fs-add
                                    (->fs)
                                    (->>  ["01" "02" "03" "05" "06" "11"]
                                          (mapv c/->value-buffer)))]
        (assert (= "11" (c/decode-value-buffer (fs-seek fs (c/->value-buffer "08")))))
        (assert (= [590598277108334592 590872055503650816] ;; "08" "11"
                   (vec (.toArray ^Roaring64Bitmap (.bm fs)))))
        (assert (nil? (fs-seek fs (c/->value-buffer "12"))))
        (prn (vec (.toArray ^Roaring64Bitmap (.bm fs))))
        (assert (= [590598277108334592 590872055503650816  ;; "08" "11"
                    590873155015278592 -1] ;; "12" -1
                   (vec (.toArray ^Roaring64Bitmap (.bm fs))))))))

;; Succinct tree spikes, not used by the above code.

(deftype LOUDS [^Roaring64Bitmap tree labels])

(defn build-louds ^org.roaringbitmap.longlong.Roaring64Bitmap [^String bits labels]
  (->LOUDS
   (reduce
    (fn [^Roaring64Bitmap acc [n x]]
      (when (= \1 x)
        (.addLong acc n))
      acc)
    (Roaring64Bitmap.)
    (map-indexed vector (str/replace bits #"\s+" "")))
   labels))

(defn rank-0 ^long [^Roaring64Bitmap bm ^long i]
  (unchecked-subtract (unchecked-inc i) (.rankLong bm i)))

(defn rank-1 ^long [^Roaring64Bitmap bm ^long i]
  (.rankLong bm i))

(defn select-0 ^long [^Roaring64Bitmap bm ^long i]
  (loop [n 0
         acc 0]
    (if (= acc i)
      n
      (recur (unchecked-inc n)
             (if-not (.contains bm n)
               (unchecked-inc acc)
               acc)))))

(defn select-1 ^long [^Roaring64Bitmap bm ^long i]
  (.select bm i))

;; SuRF paper

;; Position of the i-th node = select 0 (i) + 1
(defn louds-node ^long [^LOUDS louds ^long i]
  (unchecked-inc (select-0 (.tree louds) i)))

(defn louds-label [^LOUDS louds ^long i]
  (nth (.labels louds) (unchecked-dec (rank-0 (.tree louds) (unchecked-inc i)))))

;; Position of the k-th child of the node started at p = select 0 (rank 1 (p + k)) + 1
(defn louds-child ^long [^LOUDS louds ^long p ^long k]
  (unchecked-inc (select-0 (.tree louds) (rank-1 (.tree louds) (unchecked-add p k)))))

;; Position of the parent of the node started at p = select 1 (rank 0 (p))
(defn louds-parent ^long [^LOUDS louds ^long p]
  (select-1 (.tree louds) (rank-0 (.tree louds) p)))

;; http://www-erato.ist.hokudai.ac.jp/alsip2012/docs/tutorial.pdf

(defn louds-node ^long [^LOUDS louds ^long i]
  (select-1 (.tree louds) (unchecked-inc i)))

(defn louds-child ^long [^LOUDS louds ^long x ^long i]
  (unchecked-add (select-0 (.tree louds) (rank-1 (.tree louds) x)) i))

(defn louds-parent ^long [^LOUDS louds ^long x]
  (select-1 (.tree louds) (unchecked-dec (rank-0 (.tree louds) x)))
  #_(select-1 (.tree louds) (rank-0 (.tree louds) x)))

(comment
  (let [louds (build-louds
               "110 10 110 1110 110 110 0 10 0 0 0 10 0 0 0"
               ["0" "1" "2" "3" "4" "5" "6" "7" "8" "9" "A" "B" "C" "D" "E"])]
    (louds-label louds (louds-child louds 0 1)))

  (let [louds ^LOUDS (build-louds "10 110 10 110 0 110 10 0 0 0"
                                  ["1" "2" "3" "4" "5" "6" "7" "8" "9"])]
    (louds-child louds 4 2)
    (louds-parent louds 14)
    (louds-node louds 3)
    (louds-node louds 8)
    #_(louds-node louds 3)
    #_(+ (select-0 (.tree louds) (rank-1 (.tree louds) 4)) 2)))
