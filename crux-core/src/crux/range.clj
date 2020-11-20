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

;; https://bitbucket.org/vsmirnov/memoria/wiki/LabeledTree

;; Tests, uses one based node indexes, and zero based positions:
;; https://github.com/laysakura/louds-rs/blob/master/src/louds/louds.rs

;; http://www-erato.ist.hokudai.ac.jp/alsip2012/docs/tutorial.pdf
;; slide 37, note that the 14 is wrong, should be 13, with zero based
;; it becomes 12.

;; i is bitmap position index (points to a one), n is BFS node index,
;; c is child index. All zero based.

;; node index operations

(defn louds-node-label [^LOUDS louds ^long n]
  (nth (.labels louds) n))

(defn louds-node-position ^long [^LOUDS louds ^long n]
  (select-1 (.tree louds) n))

(defn louds-node-degree ^long [^LOUDS louds ^long n]
  (unchecked-subtract (select-0 (.tree louds) (unchecked-inc n)) (unchecked-inc (select-0 (.tree louds) n))))

(defn louds-node-first-child ^long [^LOUDS louds ^long n]
  (let [ci (unchecked-inc (select-0 (.tree louds) n))]
    (if (.contains ^Roaring64Bitmap (.tree louds) ci)
      (unchecked-dec (unchecked-subtract ci n))
      -1)))

(defn louds-node-parent ^long [^LOUDS louds ^long n]
  (if (zero? n)
    -1
    (unchecked-dec (rank-0 (.tree louds) (louds-node-position louds n)))))

;; position index operations

(defn louds-node ^long [^LOUDS louds ^long i]
  (unchecked-dec (rank-1 (.tree louds) i)))

(defn louds-label [^LOUDS louds ^long i]
  (louds-node-label louds (louds-node louds i)))

(defn louds-child ^long [^LOUDS louds ^long i ^long c]
  (let [ci (unchecked-add (select-0 (.tree louds) (unchecked-dec (rank-1 (.tree louds) i))) (unchecked-inc c))]
    (if (.contains ^Roaring64Bitmap (.tree louds) ci)
      ci
      -1)))

(defn louds-leaf? [^LOUDS louds ^long i]
  (= -1 (louds-child louds i 0)))

(defn louds-sibling ^long [^LOUDS louds ^long i]
  (let [si (unchecked-inc i)]
    (if (and (.contains ^Roaring64Bitmap (.tree louds) i)
             (.contains ^Roaring64Bitmap (.tree louds) si))
      si
      -1)))

(defn louds-parent ^long [^LOUDS louds ^long i]
  (if (zero? i)
    -1
    (select-1 (.tree louds) (unchecked-dec (rank-0 (.tree louds) i)))))

(defn louds-depth ^long [^LOUDS louds ^long i]
  (loop [depth 0
         i i]
    (if (zero? i)
      depth
      (recur (unchecked-inc depth)
             (louds-parent louds i)))))

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
    (assert (= -1 (louds-parent louds 0)))
    (assert (= -1 (louds-child louds (louds-child louds (louds-child louds 0 0) 0) 0)))
    (assert (= 7 (louds-child louds (louds-child louds 0 1) 0)))
    (assert (= 8 (louds-sibling louds (louds-child louds (louds-child louds 0 1) 0))))
    (assert (= -1 (louds-sibling louds (louds-sibling louds (louds-child louds (louds-child louds 0 1) 0)))))
    (assert (not (louds-leaf? louds 0)))
    (assert (not (louds-leaf? louds 2)))
    (assert (louds-leaf? louds 11))
    (assert (zero? (louds-depth louds 0)))
    (assert (= 1 (louds-depth louds 3)))
    (assert (= 2 (louds-depth louds 8)))
    (assert (= 3 (louds-depth louds 12)))

    (assert (= "1" (louds-label louds 0)))
    (assert (= "2" (louds-label louds (louds-child louds 0 0))))
    (assert (= "3" (louds-label louds (louds-child louds 0 1))))
    (assert (= "4" (louds-label louds (louds-child louds (louds-child louds 0 0) 0))))
    (assert (= "5" (louds-label louds (louds-child louds (louds-child louds 0 1) 0))))
    (assert (= "6" (louds-label louds (louds-child louds (louds-child louds 0 1) 1))))
    (assert (= "7" (louds-label louds (louds-child louds (louds-child louds (louds-child louds 0 1) 0) 0))))
    (assert (= "8" (louds-label louds (louds-child louds (louds-child louds (louds-child louds 0 1) 0) 1))))
    (assert (= "9" (louds-label louds (louds-child louds (louds-child louds (louds-child louds 0 1) 1) 0))))
    (assert (= 0 (louds-node-position louds 0)))
    (assert (= 2 (louds-node-position louds 1)))
    (assert (= 1 (louds-node louds (louds-node-position louds 1))))
    (assert (= 5 (louds-node-position louds 3)))
    (assert (= 3 (louds-node louds (louds-node-position louds 3))))
    (assert (= 2 (louds-node-degree louds 0)))
    (assert (= 2 (louds-node-degree louds 2)))
    (assert (= 1 (louds-node-degree louds 5)))
    (assert (= 0 (louds-node-parent louds 2)))
    (assert (= 5 (louds-node-parent louds 8)))
    (assert (= 1 (louds-node-first-child louds 0)))
    (assert (= 3 (louds-node-first-child louds 1)))
    (assert (= 8 (louds-node-first-child louds 5)))
    (assert (= -1 (louds-node-first-child louds 8)))))

;; Fast Succinct Tree

(deftype LOUDSSparse [^bytes labels ^Roaring64Bitmap has-child? ^Roaring64Bitmap tree ^objects values])

(defn str->louds-sparse ^crux.range.LOUDSSparse [labels has-child? tree values]
  (->LOUDSSparse (byte-array (map unchecked-int (seq labels)))
                 (str->bitmap has-child?)
                 (str->bitmap tree)
                 (object-array values)))

;; select1(S-LOUDS, rank1(S-HasChild, pos) + 1)
(defn louds-sparse-child ^long [^LOUDSSparse louds-sparse ^long i]
  (if (.contains ^Roaring64Bitmap (.has-child? louds-sparse) i)
    (select-1 (.tree louds-sparse)
              (rank-1 (.has-child? louds-sparse) i))
    -1))

;; select1(S-HasChild, rank1(S-LOUDS, pos) - 1)
(defn louds-sparse-parent ^long [^LOUDSSparse louds-sparse ^long i]
  (if (zero? i)
    -1
    (select-1 (.has-child? louds-sparse)
              (unchecked-subtract (rank-1 (.tree louds-sparse) i) 2))))

;; pos - rank1(S-HasChild, pos)
(defn louds-sparse-value [^LOUDSSparse louds-sparse ^long i]
  (when-not (.contains ^Roaring64Bitmap (.has-child? louds-sparse) i)
    (aget ^objects (.values louds-sparse)
          (unchecked-subtract i (rank-1 (.has-child? louds-sparse) i)))))

(defn louds-sparse-find [^LOUDSSparse louds-sparse ^bytes k]
  (loop [n 0
         level 0]
    (when (< level (alength k))
      (let [kb (aget k level)
            n (long (loop [c n]
                      (cond
                        (= (aget ^bytes (.labels louds-sparse) c) kb)
                        c

                        (.contains ^Roaring64Bitmap (.tree louds-sparse) (unchecked-inc c))
                        -1

                        :else
                        (recur (unchecked-inc c)))))]
        (when (not= -1 n)
          (let [c (louds-sparse-child louds-sparse n)]
            (cond
              (= -1 c)
              (when (= (alength k) (inc level))
                (louds-sparse-value louds-sparse n))

              (and (= (alength k) (inc level))
                   (= (Byte/toUnsignedLong (aget ^bytes (.labels louds-sparse) c)) 0xff)
                   (not (.contains ^Roaring64Bitmap (.has-child? louds-sparse) c)))
              (louds-sparse-value louds-sparse c)

              :else
              (recur c (inc level)))))))))

(deftype LOUDSDense [^Roaring64Bitmap labels ^Roaring64Bitmap has-child? ^Roaring64Bitmap prefix-key? ^objects values])

;; 256 × rank1(D-HasChild, pos)
(defn louds-dense-child ^long [^LOUDSDense louds-dense ^long i]
  (if (.contains ^Roaring64Bitmap (.has-child? louds-dense) i)
    (unchecked-multiply 256 (rank-1 (.has-child? louds-dense) i))
    -1))

;; select1(D-HasChild, [pos/256])
(defn louds-dense-parent ^long [^LOUDSDense louds-dense ^long i]
  (if (zero? i)
    -1
    (select-1 (.has-child? louds-dense) (unchecked-dec (Long/divideUnsigned i 256)))))

;; rank1(D-Labels, pos) - rank1(D-HasChild, pos) + rank1(D-IsPrefixKey, [pos/256]) - 1
(defn louds-dense-value [^LOUDSDense louds-dense ^long i]
  (when-not (.contains ^Roaring64Bitmap (.has-child? louds-dense) i)
    (aget ^objects (.values louds-dense)
          (unchecked-add (unchecked-subtract (rank-1 (.labels louds-dense) i)
                                             (rank-1 (.has-child? louds-dense) i))
                         (unchecked-dec (rank-1 (.prefix-key? louds-dense) (Long/divideUnsigned i 256)))))))

(deftype LOUDSDS [^LOUDSDense dense ^LOUDSSparse sparse ^long dense-height ^long dense-nodes ^long dense-children])

(defn louds-ds-find [^LOUDSDS louds-ds ^bytes k]
  (let [louds-dense ^LOUDSDense (.dense louds-ds)
        louds-sparse ^LOUDSSparse (.sparse louds-ds)]
    (loop [n 0
           level 0]
      (when (< level (alength k))
        (if (< level (.dense-height louds-ds))
          (let [nn (Long/divideUnsigned n 256)
                kp (+ n (Byte/toUnsignedLong (aget k level)))]
            (when (.contains ^Roaring64Bitmap (.labels louds-dense) kp)
              (let [c (louds-dense-child louds-dense kp)]
                (cond
                  (= -1 c)
                  (when (= (alength k) (inc level))
                    (louds-dense-value louds-dense kp))

                  (and (= (alength k) (inc level))
                       (.contains ^Roaring64Bitmap (.prefix-key? louds-dense) (Long/divideUnsigned c 256)))
                  (louds-dense-value louds-dense c)

                  :else
                  (if (= (.dense-height louds-ds) (inc level))
                    (recur (select-1 (.tree louds-sparse)
                                     (unchecked-subtract (Long/divideUnsigned c 256) (.dense-nodes louds-ds)))
                           (inc level))
                    (recur c (inc level)))))))
          (let [kb (aget k level)
                n (long (loop [c n]
                          (cond
                            (= (aget ^bytes (.labels louds-sparse) c) kb)
                            c

                            (.contains ^Roaring64Bitmap (.tree louds-sparse) (unchecked-inc c))
                            -1

                            :else
                            (recur (unchecked-inc c)))))]
            (when (not= -1 n)
              (let [c (if (.contains ^Roaring64Bitmap (.has-child? louds-sparse) n)
                        (select-1 (.tree louds-sparse)
                                  (unchecked-subtract (unchecked-add (rank-1 (.has-child? louds-sparse) n)
                                                                     (.dense-children louds-ds))
                                                      (.dense-nodes louds-ds)))
                        -1)]
                (cond
                  (= -1 c)
                  (when (= (alength k) (inc level))
                    (louds-sparse-value louds-sparse n))

                  (and (= (alength k) (inc level))
                       (= (Byte/toUnsignedLong (aget ^bytes (.labels louds-sparse) c)) 0xff)
                       (not (.contains ^Roaring64Bitmap (.has-child? louds-sparse) c)))
                  (louds-sparse-value louds-sparse c)

                  :else
                  (recur c (inc level)))))))))))

(comment
  (let [louds (str->louds-sparse "fst\u00ffaorrstpyiy\u00fftep"
                                 "1 0 1 0 1 1 1 0 1 0 0 0 1 0 0 0 0 0"
                                 "1 0 0 1 0 1 0 1 0 0 1 0 1 0 1 0 1 0"
                                 ["v1" "v2" "v3" "v4" "v5" "v6" "v7" "v8" "v9" "v10" "v11"])]

    (assert (= "v1" (louds-sparse-find louds (.getBytes "s" StandardCharsets/UTF_8))))
    (assert (= "v2" (louds-sparse-find louds (.getBytes "f" StandardCharsets/UTF_8))))
    (assert (= "v3" (louds-sparse-find louds (.getBytes "far" StandardCharsets/UTF_8))))
    (assert (= "v4" (louds-sparse-find louds (.getBytes "fat" StandardCharsets/UTF_8))))
    (assert (= "v5" (louds-sparse-find louds (.getBytes "top" StandardCharsets/UTF_8))))
    (assert (= "v6" (louds-sparse-find louds (.getBytes "toy" StandardCharsets/UTF_8))))
    (assert (= "v7" (louds-sparse-find louds (.getBytes "try" StandardCharsets/UTF_8))))
    (assert (= "v8" (louds-sparse-find louds (.getBytes "fas" StandardCharsets/UTF_8))))
    (assert (= "v9" (louds-sparse-find louds (.getBytes "fast" StandardCharsets/UTF_8))))
    (assert (= "v10" (louds-sparse-find louds (.getBytes "trie" StandardCharsets/UTF_8))))
    (assert (= "v11" (louds-sparse-find louds (.getBytes "trip" StandardCharsets/UTF_8))))

    (assert (nil? (louds-sparse-find louds (.getBytes "" StandardCharsets/UTF_8))))
    (assert (nil? (louds-sparse-find louds (.getBytes "so" StandardCharsets/UTF_8))))
    (assert (nil? (louds-sparse-find louds (.getBytes "tr" StandardCharsets/UTF_8))))
    (assert (nil? (louds-sparse-find louds (.getBytes "faster" StandardCharsets/UTF_8))))

    (assert (= 3 (louds-sparse-child louds 0)))
    (assert (= -1 (louds-sparse-child louds (louds-sparse-child louds 0))))
    (assert (= 7 (louds-sparse-child louds (inc (louds-sparse-child louds 0)))))
    (assert (= 14 (louds-sparse-child louds (inc (louds-sparse-child louds (inc (louds-sparse-child louds 0)))))))

    (assert (= -1 (louds-sparse-parent louds 0)))
    (assert (= 0 (louds-sparse-parent louds (louds-sparse-child louds 0))))
    (assert (= 4 (louds-sparse-parent louds (louds-sparse-child louds (inc (louds-sparse-child louds 0))))))
    (assert (= 8 (louds-sparse-parent louds (louds-sparse-child louds (inc (louds-sparse-child louds (inc (louds-sparse-child louds 0)))))))))

  (let [louds (->LOUDSDense
               (doto (Roaring64Bitmap.)
                 (.addLong (unchecked-int \f))
                 (.addLong (unchecked-int \s))
                 (.addLong (unchecked-int \t))
                 (.addLong (+ (* 1 256) (unchecked-int \a)))
                 (.addLong (+ (* 2 256) (unchecked-int \o)))
                 (.addLong (+ (* 2 256) (unchecked-int \r))))
               (doto (Roaring64Bitmap.)
                 (.addLong (unchecked-int \f))
                 (.addLong (unchecked-int \t))
                 (.addLong (+ (* 1 256) (unchecked-int \a)))
                 (.addLong (+ (* 2 256) (unchecked-int \o)))
                 (.addLong (+ (* 2 256) (unchecked-int \r))))
               (doto (Roaring64Bitmap.)
                 (.addLong 1))
               (object-array ["v1" "v2"]))]
    (assert (= 256 (louds-dense-child louds (unchecked-int \f))))
    (assert (= -1 (louds-dense-child louds (unchecked-int \s))))
    (assert (= 512 (louds-dense-child louds (unchecked-int \t))))
    (assert (= (unchecked-int \f) (louds-dense-parent louds (louds-dense-child louds (unchecked-int \f)))))

    (assert (= 768 (louds-dense-child louds (+ (louds-dense-child louds (unchecked-int \f)) (unchecked-int \a)))))
    (assert (= (+ (louds-dense-child louds (unchecked-int \f)) (unchecked-int \a))
               (louds-dense-parent louds (louds-dense-child louds (+ (louds-dense-child louds (unchecked-int \f)) (unchecked-int \a))))))
    (assert (= -1 (louds-dense-child louds (+ (louds-dense-child louds (unchecked-int \f)) (unchecked-int \b)))))

    (assert (= "v1" (louds-dense-value louds (unchecked-int \s))))
    (assert (= "v2" (louds-dense-value louds 256))))

  (let [louds (->LOUDSDS
               (->LOUDSDense
                (doto (Roaring64Bitmap.)
                  (.addLong (unchecked-int \f))
                  (.addLong (unchecked-int \s))
                  (.addLong (unchecked-int \t))
                  (.addLong (+ (* 1 256) (unchecked-int \a)))
                  (.addLong (+ (* 2 256) (unchecked-int \o)))
                  (.addLong (+ (* 2 256) (unchecked-int \r))))
                (doto (Roaring64Bitmap.)
                  (.addLong (unchecked-int \f))
                  (.addLong (unchecked-int \t))
                  (.addLong (+ (* 1 256) (unchecked-int \a)))
                  (.addLong (+ (* 2 256) (unchecked-int \o)))
                  (.addLong (+ (* 2 256) (unchecked-int \r))))
                (doto (Roaring64Bitmap.)
                  (.addLong 1))
                (object-array ["v1" "v2"]))
               (str->louds-sparse "rstpyiy\u00fftep"
                                  "0 1 0 0 0 1 0 0 0 0 0"
                                  "1 0 0 1 0 1 0 1 0 1 0"
                                  ["v3" "v4" "v5" "v6" "v7" "v8" "v9" "v10" "v11"])
               2
               3
               5)]

    (assert (= "v1" (louds-ds-find louds (.getBytes "s" StandardCharsets/UTF_8))))
    (assert (= "v2" (louds-ds-find louds (.getBytes "f" StandardCharsets/UTF_8))))
    (assert (= "v3" (louds-ds-find louds (.getBytes "far" StandardCharsets/UTF_8))))
    (assert (= "v4" (louds-ds-find louds (.getBytes "fat" StandardCharsets/UTF_8))))
    (assert (= "v5" (louds-ds-find louds (.getBytes "top" StandardCharsets/UTF_8))))
    (assert (= "v6" (louds-ds-find louds (.getBytes "toy" StandardCharsets/UTF_8))))
    (assert (= "v7" (louds-ds-find louds (.getBytes "try" StandardCharsets/UTF_8))))
    (assert (= "v8" (louds-ds-find louds (.getBytes "fas" StandardCharsets/UTF_8))))
    (assert (= "v9" (louds-ds-find louds (.getBytes "fast" StandardCharsets/UTF_8))))
    (assert (= "v10" (louds-ds-find louds (.getBytes "trie" StandardCharsets/UTF_8))))
    (assert (= "v11" (louds-ds-find louds (.getBytes "trip" StandardCharsets/UTF_8))))))
