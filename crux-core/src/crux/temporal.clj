(ns crux.temporal
  (:refer-clojure :exclude [contains?])
  (:import [java.util Date Map NavigableMap TreeMap TreeSet]))

(defprotocol ITimeSpan
  (beginning [_] "Return the beginning of a span of time")
  (end [_] "Return the end of a span of time"))

(defrecord Interval [^Date beginning ^Date end]
  ITimeSpan
  (beginning [_] (or beginning (Date. Long/MIN_VALUE)))
  (end [_] (or end (Date. Long/MAX_VALUE)))

  Inst
  (inst-ms* [_] (.getTime beginning))

  Comparable
  (compareTo [this other]
    (compare [(crux.temporal/beginning this)
              (crux.temporal/end this)]
             [(crux.temporal/beginning other)
              (crux.temporal/end other)])))

(extend-protocol ITimeSpan
  Date
  (beginning [this] this)
  (end [this] this))

;; Allen's Basic Relations, adapted from https://github.com/juxt/tick

(defn precedes? [x y]
  (neg? (compare (end x) (beginning y))))

(defn equals? [x y]
  (and
    (= (beginning x) (beginning y))
    (= (end x) (end y))))

(defn meets? [x y]
  (= (end x) (beginning y)))

(defn overlaps? [x y]
  (and
   (neg? (compare (beginning x) (beginning y)))
   (pos? (compare (end x) (beginning y)))
   (neg? (compare (end x) (end y)))))

(defn during? [x y]
  (and
   (pos? (compare (beginning x) (beginning y)))
   (neg? (compare (end x) (end y)))))

(defn starts? [x y]
  (and
   (= (beginning x) (beginning y))
   (neg? (compare (end x) (end y)))))

(defn finishes? [x y]
  (and
   (pos? (compare (beginning x) (beginning y)))
   (= (end x) (end y))))

(defn preceded-by? [x y] (precedes? y x))
(defn met-by? [x y] (meets? y x))
(defn overlapped-by? [x y] (overlaps? y x))
(defn finished-by? [x y] (finishes? y x))

(defn contains? [x y] (during? y x))
(defn started-by? [x y] (starts? y x))

;; Useful named general relations

(defn disjoint? [x y]
  (boolean (some #(% x y) [precedes? preceded-by? meets? met-by?])))

(defn concur? [x y]
  (not (disjoint? x y)))

(defn precedes-or-meets? [x y]
  (boolean (some #(% x y) [precedes? meets?])))

(defn entity-history->intervals ^java.util.NavigableMap [history]
  (let [vt-groups (for [vt-group (partition-by :crux.db/valid-time history)]
                    [vt-group
                     (doto (TreeSet.)
                       (.addAll (for [[start end] (partition 2 1 [nil] (map :crux.tx/tx-time vt-group))]
                                  (->Interval start end))))])]
    (reduce
     (fn [^Map acc [k v]]
       (doto acc
         (.put k v)))
     (TreeMap.)
     (for [[[[vt-x] tts] [[vt-y]]] (partition 2 1 [nil] vt-groups)]
           [(->Interval
             (:crux.db/valid-time vt-x)
             (:crux.db/valid-time vt-y))
            tts]))))
