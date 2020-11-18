(ns crux.range
  (:import [org.agrona DirectBuffer MutableDirectBuffer]
           org.roaringbitmap.longlong.Roaring64Bitmap
           clojure.lang.PersistentQueue
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
        (assert (= [397583404603801600 397636181161934848]
                   (vec (.toArray ^Roaring64Bitmap (.bm fs))))))

      (let [fs ^FilteredSet (reduce fs-add
                                    (->fs)
                                    (->>  ["01" "02" "03" "05" "06" "11"]
                                          (mapv c/->value-buffer)))]
        (assert (= "11" (c/decode-value-buffer (fs-seek fs (c/->value-buffer "08")))))
        (assert (= [590598277108334592 590872055503650816] ;; "08" "11"
                   (vec (.toArray ^Roaring64Bitmap (.bm fs)))))
        (assert (nil? (fs-seek fs (c/->value-buffer "12"))))
        (assert (= [590598277108334592 590872055503650816  ;; "08" "11"
                    590873155015278592 -1] ;; "12" -1
                   (vec (.toArray ^Roaring64Bitmap (.bm fs))))))))

;; Succinct tree spikes, not used by the above code.

(deftype LOUDS [^Roaring64Bitmap tree labels])

(defn str->bitmap ^org.roaringbitmap.longlong.Roaring64Bitmap [^String bits]
  (reduce
   (fn [^Roaring64Bitmap acc [n x]]
     (when (= \1 x)
       (.addLong acc n))
     acc)
   (Roaring64Bitmap.)
   (map-indexed vector (str/replace bits #"\s+" ""))))

(defn str->louds ^crux.range.LOUDS [^String bits labels]
  (->LOUDS (str->bitmap bits) labels))

(defn tree->louds ^crux.range.LOUDS [tree]
  (loop [stack (conj PersistentQueue/EMPTY tree)
         labels []
         bits (doto (Roaring64Bitmap.)
                (.addLong 0))
         bit-offset 2]
    (if (empty? stack)
      (->LOUDS bits labels)
      (let [[node & children] (peek stack)]
        (recur
         (into (pop stack) children)
         (conj labels node)
         (if (zero? (count children))
           bits
           (doto bits
             (.add bit-offset (+ bit-offset (count children)))))
         (+ bit-offset (inc (count children))))))))

(defn rank-0 ^long [^Roaring64Bitmap bm ^long i]
  (unchecked-subtract (unchecked-inc i) (.rankLong bm i)))

(defn rank-1 ^long [^Roaring64Bitmap bm ^long i]
  (.rankLong bm i))

(defn select-0 ^long [^Roaring64Bitmap bm ^long i]
  (let [highest-bit (.select bm (unchecked-dec (.getLongCardinality bm)))
        zereos-in-range (unchecked-subtract highest-bit (unchecked-dec (.getLongCardinality bm)))
        needed-zereos (unchecked-inc i)]
    (if (neg? (Long/compareUnsigned zereos-in-range needed-zereos))
      (unchecked-add highest-bit (unchecked-subtract needed-zereos zereos-in-range))
      (loop [low 0
             high highest-bit]
        (let [mid (unsigned-bit-shift-right (unchecked-add low high) 1)
              zeroes (rank-0 bm mid)]
          (cond
            (= zeroes needed-zereos)
            (loop [n mid]
              (if (.contains bm n)
                (recur (unchecked-dec n))
                n))

            (pos? (Long/compareUnsigned needed-zereos zeroes))
            (recur (unchecked-inc mid) high)

            :else
            (recur low (unchecked-dec mid))))))))

(defn select-1 ^long [^Roaring64Bitmap bm ^long i]
  (.select bm i))

;; SuRF paper

;; ;; Position of the i-th node = select 0 (i) + 1
;; (defn louds-node ^long [^LOUDS louds ^long i]
;;   (unchecked-inc (select-0 (.tree louds) i)))

;; (defn louds-label [^LOUDS louds ^long i]
;;   (nth (.labels louds) (unchecked-dec (rank-0 (.tree louds) (unchecked-inc i)))))

;; ;; Position of the k-th child of the node started at p = select 0 (rank 1 (p + k)) + 1
;; (defn louds-child ^long [^LOUDS louds ^long p ^long k]
;;   (unchecked-inc (select-0 (.tree louds) (rank-1 (.tree louds) (unchecked-add p k)))))

;; ;; Position of the parent of the node started at p = select 1 (rank 0 (p))
;; (defn louds-parent ^long [^LOUDS louds ^long p]
;;   (select-1 (.tree louds) (rank-0 (.tree louds) p)))

;; http://www-erato.ist.hokudai.ac.jp/alsip2012/docs/tutorial.pdf
;; slide 37, note that the 14 is wrong, should be 13, with zero based
;; it becomes 12.

(defn louds-node ^long [^LOUDS louds ^long i]
  (unchecked-dec (rank-1 (.tree louds) i)))

(defn louds-label [^LOUDS louds ^long i]
  (nth (.labels louds) (louds-node louds i)))

(defn louds-child ^long [^LOUDS louds ^long x ^long i]
  (unchecked-add (select-0 (.tree louds) (unchecked-dec (rank-1 (.tree louds) x))) (unchecked-inc i)))

(defn louds-parent ^long [^LOUDS louds ^long x]
  (select-1 (.tree louds) (unchecked-dec (rank-0 (.tree louds) x))))

(comment
  (let [louds ^LOUDS (tree->louds ["1"
                                   ["2"
                                    ["4"]]
                                   ["3"
                                    ["5"
                                     ["7"]
                                     ["8"]]
                                    ["6"
                                     ["9"]]]])]
    (assert (= 8 (louds-child louds (louds-child louds 0 1) 1)))
    (assert (= 12 (louds-child louds (louds-child louds (louds-child louds 0 1) 0) 1)))
    (assert (= 3 (louds-parent louds (louds-child louds (louds-child louds 0 1) 1))))
    (assert (= 0 (louds-parent louds (louds-child louds 0 1))))

    (assert (= "1" (louds-label louds 0)))
    (assert (= "2" (louds-label louds (louds-child louds 0 0))))
    (assert (= "3" (louds-label louds (louds-child louds 0 1))))
    (assert (= "4" (louds-label louds (louds-child louds (louds-child louds 0 0) 0))))
    (assert (= "5" (louds-label louds (louds-child louds (louds-child louds 0 1) 0))))
    (assert (= "6" (louds-label louds (louds-child louds (louds-child louds 0 1) 1))))
    (assert (= "7" (louds-label louds (louds-child louds (louds-child louds (louds-child louds 0 1) 0) 0))))
    (assert (= "8" (louds-label louds (louds-child louds (louds-child louds (louds-child louds 0 1) 0) 1))))
    (assert (= "9" (louds-label louds (louds-child louds (louds-child louds (louds-child louds 0 1) 1) 0))))))
