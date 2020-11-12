(ns crux.sketch
  (:require [crux.memory :as mem])
  (:import [org.agrona DirectBuffer MutableDirectBuffer]
           java.nio.ByteOrder))

;; http://dimacs.rutgers.edu/~graham/pubs/papers/cacm-sketch.pdf

;; http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
(defn ->hyper-log-log
  (^org.agrona.MutableDirectBuffer []
   (->hyper-log-log 1024))
  (^org.agrona.MutableDirectBuffer [^long m]
   (mem/allocate-unpooled-buffer (* Integer/BYTES m))))

(defn hyper-log-log-update ^org.agrona.MutableDirectBuffer [^MutableDirectBuffer hll v]
  (let [m (/ (.capacity hll) Integer/BYTES)
        b (Integer/numberOfTrailingZeros m)
        x (long (hash v))
        j (bit-and (bit-shift-right x (- Integer/SIZE b)) (dec m))
        w (bit-and x (dec (bit-shift-left 1 (- Integer/SIZE b))))]
    (doto hll
      (.putInt (* j Integer/BYTES)
               (max (.getInt hll (* j Integer/BYTES) ByteOrder/BIG_ENDIAN)
                    (- (inc (Integer/numberOfLeadingZeros w)) b))
               ByteOrder/BIG_ENDIAN))))

(defn hyper-log-log-estimate ^double [^DirectBuffer hll]
  (let [m (/ (.capacity hll) Integer/BYTES)
        z (/ 1.0 (double (loop [n 0
                                acc 0.0]
                           (if (< n (.capacity hll))
                             (recur (+ n Integer/BYTES)
                                    (+ acc (Math/pow 2.0 (- (.getInt hll n ByteOrder/BIG_ENDIAN)))))
                             acc))))
        am (/ 0.7213 (inc (/ 1.079 m)))
        e (* am (Math/pow m 2.0) z)]
    (cond
      (<= e (* (double (/ 5 2)) m))
      (let [v (long (loop [n 0
                           acc 0]
                           (if (< n (.capacity hll))
                             (recur (+ n Integer/BYTES)
                                    (+ acc (if (zero? (.getInt hll n ByteOrder/BIG_ENDIAN))
                                             1
                                             0)))
                             acc)))]
        (if (zero? v)
          e
          (* m (Math/log (/ m v)))))
      (> e (* (double (/ 1 30)) (Integer/toUnsignedLong -1)))
      (* (Math/pow -2.0 32)
         (Math/log (- 1 (/ e (Integer/toUnsignedLong -1)))))
      :else
      e)))

(def ^:private ^:const count-min-d 8)

(defn ->count-min
  (^org.agrona.MutableDirectBuffer []
   (let [w 128
         d count-min-d
         epsilon (/ 2.0 w)
         confidence (- 1 (/ 1 (Math/pow 2.0 d)))]
     (->count-min (* d w Integer/BYTES))))
  (^org.agrona.MutableDirectBuffer [^long size]
   (mem/allocate-unpooled-buffer size)))

(defn- long-mod ^long [^long num ^long div]
  (let [m (rem num div)]
    (if (or (zero? m) (= (pos? num) (pos? div)))
      m
      (+ m div))))

(defn count-min-add
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer cm x]
   (count-min-add cm x 1))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer cm x ^long count]
   (let [d count-min-d
         w (/ (.capacity cm) (* d Integer/BYTES))
         h (hash x)]
     (dotimes [n d]
       (let [idx (* (+ (long-mod (mix-collection-hash h n) w)
                       (* d n))
                    Integer/BYTES)]
         (.putInt cm idx (+ count (.getInt cm idx ByteOrder/BIG_ENDIAN)) ByteOrder/BIG_ENDIAN)))
     cm)))

(defn count-min-estimate ^long [^DirectBuffer cm x]
  (let [d count-min-d
        w (/ (.capacity cm) (* d Integer/BYTES))
        h (hash x)]
    (loop [n 0
           estimate Integer/MAX_VALUE]
      (if (= n d)
        estimate
        (let [idx (* (+ (long-mod (mix-collection-hash h n) w)
                        (* d n))
                     Integer/BYTES)]
          (recur (inc n) (min estimate (.getInt cm idx ByteOrder/BIG_ENDIAN))))))))

(def ^:private ^:const bloom-filter-hashes 2)

(defn ->bloom-filter
  (^org.agrona.MutableDirectBuffer []
   (->bloom-filter 1024))
  (^org.agrona.MutableDirectBuffer [^long size]
   (mem/allocate-unpooled-buffer (/ size Byte/SIZE))))

(defn bloom-filter-add ^org.agrona.MutableDirectBuffer [^MutableDirectBuffer bf x]
  (let [h (hash x)
        size (* (.capacity bf) Byte/SIZE)]
    (dotimes [n bloom-filter-hashes]
      (let [bit (long-mod (mix-collection-hash h n) size)
            byte-idx (bit-shift-right bit 3)
            bit-idx (bit-shift-left 1 (bit-and 0x7 bit))]
        (.putByte bf byte-idx (unchecked-byte (bit-or (.getByte bf byte-idx) bit-idx)))))
    bf))

(defn bloom-filter-might-contain? [^DirectBuffer bf x]
  (let [h (hash x)
        size (* (.capacity bf) Byte/SIZE)]
    (loop [n 0]
      (if (= n bloom-filter-hashes)
        true
        (let [bit (long-mod (mix-collection-hash h n) size)
              byte-idx (bit-shift-right bit 3)
              bit-idx (bit-shift-left 1 (bit-and 0x7 bit))]
          (if (pos? (bit-and (.getByte bf byte-idx) bit-idx))
            (recur (inc n))
            false))))))
