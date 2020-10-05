(ns ^:no-doc crux.lru
  (:require [crux.db :as db]
            [crux.io :as cio]
            [crux.kv :as kv])
  (:import [clojure.lang Counted ILookup]
           java.lang.reflect.Field
           java.util.concurrent.locks.StampedLock
           java.util.function.Function
           [java.util LinkedHashMap Map Map$Entry]
           [java.util.concurrent ArrayBlockingQueue ConcurrentHashMap]
           com.github.benmanes.caffeine.cache.Caffeine))

(set! *unchecked-math* :warn-on-boxed)

(defprotocol LRUCache
  (compute-if-absent [this k stored-key-fn f])
  ; key-fn sometimes used to copy the key to prevent memory leaks
  (evict [this k]))

(defn new-lru-cache [^long size]
  (let [cache (proxy [LinkedHashMap] [size 0.75 true]
                (removeEldestEntry [_]
                  (> (.size ^Map this) size)))
        lock (StampedLock.)]
    (reify
      Object
      (toString [this]
        (.toString cache))

      LRUCache
      (compute-if-absent [this k stored-key-fn f]
        (let [v (.valAt this k ::not-found)] ; use ::not-found as values can be falsy
          (if (= ::not-found v)
            (let [k (stored-key-fn k)
                  v (f k)]
              (cio/with-write-lock lock
                ;; lock the cache only after potentially heavy value and key calculations are done
                (.computeIfAbsent cache k (reify Function
                                            (apply [_ k]
                                              v)))))
            v)))

      (evict [_ k]
        (cio/with-write-lock lock
          (.remove cache k)))

      ILookup
      (valAt [this k]
        (cio/with-write-lock lock
          (.get cache k)))

      (valAt [this k default]
        (cio/with-write-lock lock
          (.getOrDefault cache k default)))

      Counted
      (count [_]
        (.size cache)))))

(defn new-static-cache [^long size]
  (let [cache (ConcurrentHashMap. size)]
    (reify
      Object
      (toString [_]
        (.toString cache))

      LRUCache
      (compute-if-absent [_ k stored-key-fn f]
        (let [v (.getOrDefault cache k ::not-found)]
          (if (= ::not-found v)
            (let [k (stored-key-fn k)
                  v (f k)]
              (.computeIfAbsent cache k (reify Function
                                          (apply [_ k]
                                            v))))
            v)))

      (evict [_ k]
        (.remove cache k))

      ILookup
      (valAt [_ k]
        (.get cache k))

      (valAt [_ k default]
        (.getOrDefault cache k default))

      Counted
      (count [_]
        (.size cache)))))

(defn new-caffeine-cache [^long size]
  (let [cache (-> (Caffeine/newBuilder) (.maximumSize size) (.build))]
    (reify
      Object
      (toString [_]
        (.toString cache))

      LRUCache
      (compute-if-absent [_ k stored-key-fn f]
        (if-let [v (.getIfPresent cache k)]
          v
          (let [k (stored-key-fn k)]
            (.get cache k (reify Function
                            (apply [_ k]
                              (f k)))))))

      (evict [_ k]
        (.invalidate cache k))

      ILookup
      (valAt [_ k]
        (.getIfPresent cache k))

      (valAt [_ k default]
        (or (.getIfPresent cache k) default))

      Counted
      (count [_]
        (.estimatedSize cache)))))

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

(defn new-second-chance-cache [^long size]
  (let [hot (ConcurrentHashMap. size)
        cold-factor 0.1
        cold (ArrayBlockingQueue. (inc (long (Math/ceil (* cold-factor size)))))
        move-to-cold #(let [cold-target-size (long (Math/ceil (* cold-factor (.size hot))))]
                        (while (< (.size cold) cold-target-size)
                          (let [e (random-entry hot)]
                            (when-let [vp ^objects (.getValue e)]
                              (when (nil? (aget vp 1))
                                (when (.offer cold vp)
                                  (aset vp 1 (.getKey e))))))))
        resize-cache #(do (move-to-cold)
                          (while (> (.size hot) size)
                            (when-let [vp ^objects (.poll cold)]
                              (when-let [k (aget vp 1)]
                                (.remove hot k)))
                            (move-to-cold)))]
    (reify
      Object
      (toString [_]
        (str hot " " cold))

      LRUCache
      (compute-if-absent [_ k stored-key-fn f]
        (let [^objects vp (or (.get hot k)
                              (let [k (stored-key-fn k)
                                    v (f k)]
                                (.computeIfAbsent hot k (reify Function
                                                          (apply [_ k]
                                                            (doto (object-array 2)
                                                              (aset 0 v)))))))]
          (resize-cache)
          (aset vp 1 nil)
          (aget vp 0)))

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

(def new-cache new-second-chance-cache)
