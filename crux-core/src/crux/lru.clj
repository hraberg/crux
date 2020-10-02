(ns ^:no-doc crux.lru
  (:require [crux.db :as db]
            [crux.io :as cio]
            [crux.kv :as kv])
  (:import [clojure.lang Counted ILookup]
           java.lang.reflect.Field
           java.util.concurrent.locks.StampedLock
           java.util.function.Function
           [java.util ArrayDeque Map Map$Entry]
           java.util.concurrent.ConcurrentHashMap))

(set! *unchecked-math* :warn-on-boxed)

(defprotocol LRUCache
  (compute-if-absent [this k stored-key-fn f])
  ; key-fn sometimes used to copy the key to prevent memory leaks
  (evict [this k]))

(def ^:private ^Field concurrent-map-table-field
  (doto (.getDeclaredField ConcurrentHashMap "table")
    (.setAccessible true)))

(defn- random-entry ^java.util.Map$Entry [^ConcurrentHashMap m]
  (when-not (.isEmpty m)
    (let [table ^objects (.get concurrent-map-table-field m)]
      (loop [i (long (rand-int (alength table)))]
        (if-let [^Map$Entry e (aget table i)]
          e
          (recur (rem (inc i) (alength table))))))))

(defn new-cache [^long size]
  (let [hot (ConcurrentHashMap. size)
        cold-factor 0.1
        cold (ArrayDeque. (long (Math/ceil (* cold-factor size))))]
    (reify
      Object
      (toString [_]
        (str hot " " cold))

      LRUCache
      (compute-if-absent [_ k stored-key-fn f]
        (let [vp ^objects (.getOrDefault hot k ::not-found)]
          (if (= ::not-found vp)
            (let [k (stored-key-fn k)
                  move-to-cold #(let [cold-target-size (long (Math/ceil (* cold-factor (.size hot))))]
                                  (while (< (.size cold) cold-target-size)
                                    (let [e (random-entry hot)]
                                      (when-let [vp ^objects (.getValue e)]
                                        (when (nil? (aget vp 1))
                                          (aset vp 1 (.getKey e))
                                          (.push cold vp))))))]
              (aget ^objects (.computeIfAbsent hot k (reify Function
                                                       (apply [_ k]
                                                         (let [v (f k)
                                                               vp (doto (object-array 2)
                                                                    (aset 0 v)
                                                                    (aset 1 nil))]
                                                           (move-to-cold)
                                                           (while (> (.size hot) size)
                                                             (let [vp ^objects (.poll cold)]
                                                               (when-let [k (aget vp 1)]
                                                                 (.remove hot k)))
                                                             (move-to-cold))
                                                           vp)))) 0))
            (do (aset vp 1 nil)
                (aget vp 0)))))

      (evict [_ k]
        (when-let [vp ^objects (.remove hot k)]
          (aset vp 1 nil)))

      ILookup
      (valAt [_ k]
        (when-let [vp ^objects (.get hot k)]
          (aset vp 1 nil)
          (aget vp 0)))

      (valAt [_ k default]
        (let [vp (.getOrDefault hot k default)]
          (if (= default vp)
            default
            (do (aset ^objects vp 1 nil)
                (aget ^objects vp 0)))))

      Counted
      (count [_]
        (.size hot)))))
