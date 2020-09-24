(ns ^:no-doc crux.query
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [clojure.walk :as w]
            [com.stuartsierra.dependency :as dep]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.fork :as fork]
            [crux.mem-kv :as mem-kv]
            [crux.index :as idx]
            [crux.io :as cio]
            [crux.lru :as lru]
            [crux.memory :as mem]
            [crux.bus :as bus]
            [crux.api :as api]
            [crux.tx :as tx]
            [crux.tx.conform :as txc]
            [taoensso.nippy :as nippy]
            [edn-query-language.core :as eql]
            [clojure.string :as string]
            [crux.system :as sys])
  (:import [clojure.lang Box ExceptionInfo MapEntry]
           (crux.api ICruxDatasource HistoryOptions HistoryOptions$SortOrder NodeOutOfSyncException)
           crux.codec.EntityTx
           crux.index.IndexStoreIndexState
           (java.io Closeable Writer)
           (java.util Comparator Date List UUID TreeMap)
           [java.util.concurrent Future Executors ScheduledExecutorService TimeoutException TimeUnit]))

(defn- logic-var? [x]
  (and (symbol? x)
       (not (contains? '#{... . $ %} x))))

(def ^:private literal? (complement logic-var?))

(declare pred-constraint aggregate)

(defn- expression-spec [sym spec]
  (s/and seq?
         #(= sym (first %))
         (s/conformer next #(cons sym %))
         spec))

(def ^:private built-ins '#{and or or-join not not-join})

(defn- pred-constraint? [x]
  (contains? (methods pred-constraint) x))

(defn- aggregate? [x]
  (contains? (methods aggregate) x))

(s/def ::triple (s/and vector?
                       (s/conformer identity vec)
                       (s/cat :e (some-fn logic-var? c/valid-id? set?)
                              :a (s/and c/valid-id? some?)
                              :v (s/? (some-fn logic-var? literal?)))))

(s/def ::args-list (s/coll-of logic-var? :kind vector? :min-count 1))

(s/def ::pred-fn (s/and symbol?
                        (s/conformer
                         #(or (if (pred-constraint? %)
                                %
                                (some->
                                 (some->> (if (qualified-symbol? %)
                                            (requiring-resolve %)
                                            (ns-resolve 'clojure.core %))
                                          (var-get))
                                 (with-meta {:sym %})))
                              %)
                         #(if (symbol? %)
                            %
                            (get (meta %) :sym %)))
                        (some-fn fn? logic-var?)))
(s/def ::binding (s/or :scalar logic-var?
                       :tuple ::args-list
                       :collection (s/tuple logic-var? '#{...})
                       :relation (s/tuple ::args-list)))
(s/def ::pred (s/and vector?
                     (s/conformer identity vec)
                     (s/cat :pred (s/and seq?
                                         (s/cat :pred-fn ::pred-fn
                                                :args (s/* any?)))
                            :return (s/? ::binding))))

(s/def ::rule (s/and seq? (s/cat :name (s/and symbol? (complement built-ins))
                                 :args (s/+ any?))))

(s/def ::range-op '#{< <= >= > =})
(s/def ::range (s/tuple (s/and seq?
                               (s/or :sym-val (s/cat :op ::range-op
                                                     :sym logic-var?
                                                     :val literal?)
                                     :val-sym (s/cat :op ::range-op
                                                     :val literal?
                                                     :sym logic-var?)
                                     :sym-sym (s/cat :op ::range-op
                                                     :sym-a logic-var?
                                                     :sym-b logic-var?)))))

(s/def ::rule-args (s/cat :bound-args (s/? ::args-list)
                          :free-args (s/* logic-var?)))

(s/def ::not (expression-spec 'not (s/+ ::term)))
(s/def ::not-join (expression-spec 'not-join (s/cat :args ::args-list
                                                    :body (s/+ ::term))))

(s/def ::and (expression-spec 'and (s/+ ::term)))
(s/def ::or-body (s/+ (s/or :term ::term
                            :and ::and)))
(s/def ::or (expression-spec 'or ::or-body))
(s/def ::or-join (expression-spec 'or-join (s/cat :args (s/and vector?
                                                               (s/conformer identity vec)
                                                               ::rule-args)
                                                  :body ::or-body)))
(s/def ::term (s/or :triple ::triple
                    :not ::not
                    :not-join ::not-join
                    :or ::or
                    :or-join ::or-join
                    :range ::range
                    :rule ::rule
                    :pred ::pred))

(s/def ::aggregate (s/cat :aggregate-fn aggregate?
                          :args (s/* literal?)
                          :logic-var logic-var?))

(s/def ::find-arg
  (s/or :logic-var logic-var?
        :project (s/cat :project #{'eql/project}
                        :logic-var logic-var?
                        :project-spec ::eql/query)
        :aggregate ::aggregate))

(s/def ::find (s/coll-of ::find-arg :kind vector? :min-count 1))

(s/def ::where (s/coll-of ::term :kind vector? :min-count 1))

(s/def ::arg-tuple (s/map-of (some-fn logic-var? keyword?) any?))
(s/def ::args (s/coll-of ::arg-tuple :kind vector?))

(s/def ::rule-head (s/and list?
                          (s/cat :name (s/and symbol? (complement built-ins))
                                 :args ::rule-args)))
(s/def ::rule-definition (s/and vector?
                                (s/cat :head ::rule-head
                                       :body (s/+ ::term))))
(s/def ::rules (s/coll-of ::rule-definition :kind vector? :min-count 1))

(s/def ::offset nat-int?)
(s/def ::limit nat-int?)
(s/def ::full-results? boolean?)

(s/def ::order-element (s/and vector?
                              (s/cat :find-arg (s/or :logic-var logic-var?
                                                     :aggregate ::aggregate)
                                     :direction (s/? #{:asc :desc}))))
(s/def ::order-by (s/coll-of ::order-element :kind vector?))

(s/def ::timeout nat-int?)
(s/def ::batch-size pos-int?)

(s/def ::in (s/and vector? (s/cat :source-var (s/? '#{$})
                                  :bindings (s/* ::binding))))

(defmulti pred-args-spec first)

(defmethod pred-args-spec 'q [_]
  (s/cat :pred-fn #{'q}
         :args (s/spec (s/cat :query (s/or :quoted-query (s/cat :quote #{'quote} :query ::query)
                                           :query ::query)
                              :args (s/* any?)))
         :return (s/? ::binding)))

(defmethod pred-args-spec 'get-attr [_]
  (s/cat :pred-fn  #{'get-attr} :args (s/spec (s/cat :e-var logic-var? :attr literal? :not-found (s/? any?))) :return (s/? ::binding)))

(defmethod pred-args-spec '== [_]
  (s/cat :pred-fn #{'==} :args (s/tuple some? some?)))

(defmethod pred-args-spec '!= [_]
  (s/cat :pred-fn #{'!=} :args (s/tuple some? some?)))

(defmethod pred-args-spec :default [_]
  (s/cat :pred-fn (s/or :var logic-var? :fn fn?) :args (s/coll-of any?) :return (s/? ::binding)))

(s/def ::pred-args (s/multi-spec pred-args-spec first))

(defmulti aggregate-args-spec first)

(defmethod aggregate-args-spec 'max [_]
  (s/cat :aggregate-fn '#{max} :args (s/or :zero empty?
                                           :one (s/tuple pos-int?))))

(defmethod aggregate-args-spec 'min [_]
  (s/cat :aggregate-fn '#{min} :args (s/or :zero empty?
                                           :one (s/tuple pos-int?))))

(defmethod aggregate-args-spec 'rand [_]
  (s/cat :aggregate-fn '#{rand} :args (s/tuple pos-int?)))

(defmethod aggregate-args-spec 'sample [_]
  (s/cat :aggregate-fn '#{sample} :args (s/tuple pos-int?)))

(defmethod aggregate-args-spec :default [_]
  (s/cat :aggregate-fn symbol? :args empty?))

(s/def ::aggregate-args (s/multi-spec aggregate-args-spec first))

(defn normalize-query [q]
  (cond
    (vector? q) (into {} (for [[[k] v] (->> (partition-by keyword? q)
                                            (partition-all 2))]
                           [k (if (and (or (nat-int? (first v))
                                           (boolean? (first v)))
                                       (= 1 (count v)))
                                (first v)
                                (vec v))]))
    (string? q) (if-let [q (try
                             (c/read-edn-string-with-readers q)
                             (catch Exception e))]
                  (normalize-query q)
                  q)
    :else q))

(s/def ::query (s/and (s/conformer #'normalize-query)
                      (s/keys :req-un [::find]
                              :opt-un [::where ::in ::args ::rules ::offset ::limit ::order-by ::timeout ::full-results? ::batch-size])))

(defrecord ConformedQuery [q-normalized q-conformed])

(defn- normalize-and-conform-query ^ConformedQuery [conform-cache q]
  (let [{:keys [args] :as q} (try
                               (normalize-query q)
                               (catch Exception e
                                 q))
        conformed-query (lru/compute-if-absent
                         conform-cache
                         (if (map? q)
                           (dissoc q :args)
                           q)
                         identity
                         (fn [q]
                           (when-not (s/valid? ::query q)
                             (throw (ex-info (str "Spec assertion failed\n" (s/explain-str ::query q)) (s/explain-data ::query q))))
                           (let [q (normalize-query q)]
                             (->ConformedQuery q (s/conform ::query q)))))]
    (if args
      (do
        (when-not (s/valid? ::args args)
          (throw (ex-info (str "Spec assertion failed\n" (s/explain-str ::args args)) (s/explain-data ::args args))))
        (-> conformed-query
            (assoc-in [:q-normalized :args] args)
            (assoc-in [:q-conformed :args] args)))
      conformed-query)))

(declare open-index-snapshot build-sub-query)

;; NOTE: :min-count generates boxed math warnings, so this goes below
;; the spec.
(set! *unchecked-math* :warn-on-boxed)

(defmulti pred-constraint
  (fn [{:keys [pred return] {:keys [pred-fn]} :pred :as clause}
       {:keys [encode-value-fn idx-id arg-bindings return-type
               return-vars-tuple-idxs-in-join-order rule-name->rules]}]
    pred-fn))

(defmulti aggregate (fn [name & args] name))

(set! *unchecked-math* true)

(defn- maybe-ratio [n]
  (if (ratio? n)
    (double n)
    n))

(defmethod aggregate 'count [_]
  (fn aggregate-count
    (^long [] 0)
    (^long [^long acc] acc)
    (^long [^long acc _] (inc acc))))

(defmethod aggregate 'count-distinct [_]
  (fn aggregate-count-distinct
    ([] (transient #{}))
    ([acc] (count (persistent! acc)))
    ([acc x] (conj! acc x))))

(defmethod aggregate 'sum [_]
  (fn aggregate-sum
    ([] 0)
    ([acc] acc)
    ([acc x] (+ acc x))))

(defmethod aggregate 'avg [_]
  (let [count (aggregate 'count)
        sum (aggregate 'sum)]
    (fn aggregate-average
      ([] [(count) (sum)])
      ([[c s]] (maybe-ratio (/ s c)))
      ([[c s] x]
       [(count c x) (sum s x)]))))

(defmethod aggregate 'median [_]
  (fn aggregate-median
    ([] (transient []))
    ([acc] (let [acc (persistent! acc)
                 acc (sort acc)
                 n (count acc)
                 half-n (quot n 2)]
             (if (odd? n)
               (nth acc half-n)
               (maybe-ratio (/ (+ (nth acc half-n)
                                  (nth acc (dec half-n))) 2)))))
    ([acc x] (conj! acc x))))

(defmethod aggregate 'variance [_]
  (let [mean (aggregate 'avg)]
    (fn aggregate-variance
      ([] [0 (mean)])
      ([[m2 [c _]]] (maybe-ratio (/ m2 c)))
      ([[m2 [c _ :as m]] x]
       (let [delta (if (zero? c)
                     x
                     (- x (mean m)))
             m (mean m x)
             delta2 (- x (mean m))]
         [(+ m2 (* delta delta2)) m])))))

(defmethod aggregate 'stddev [_]
  (let [variance (aggregate 'variance)]
    (fn aggregate-stddev
      ([] (variance))
      ([v] (Math/sqrt (variance v)))
      ([v x]
       (variance v x)))))

(defmethod aggregate 'distinct [_]
  (fn aggregate-distinct
    ([] (transient #{}))
    ([acc] (persistent! acc))
    ([acc x] (conj! acc x))))

(defmethod aggregate 'rand [_ n]
  (fn aggregate-rand
    ([] (transient []))
    ([acc] (->> (persistent! acc)
                (partial shuffle)
                repeatedly
                (apply concat)
                (take n)
                vec))
    ([acc x] (conj! acc x))))

(defmethod aggregate 'sample [_ n]
  (fn aggregate-sample
    ([] (transient #{}))
    ([acc] (vec (take n (shuffle (persistent! acc)))))
    ([acc x] (conj! acc x))))

(defmethod aggregate 'min
  ([_]
   (fn aggregate-min
     ([])
     ([acc] acc)
     ([acc x]
      (if acc
        (if (pos? (compare acc x))
          x
          acc)
        x))))
  ([_ n]
   (fn aggregate-min-n
     ([] (sorted-set))
     ([acc] (vec acc))
     ([acc x]
      (let [acc (conj acc x)]
        (if (> (count acc) n)
          (disj acc (last acc))
          acc))))))

(defmethod aggregate 'max
  ([_]
   (fn aggregate-max
     ([])
     ([acc] acc)
     ([acc x]
      (if acc
        (if (neg? (compare acc x))
          x
          acc)
        x))))
  ([_ n]
   (fn aggregate-max-n
     ([] (sorted-set))
     ([acc] (vec acc))
     ([acc x]
      (let [acc (conj acc x)]
        (if (> (count acc) n)
          (disj acc (first acc))
          acc))))))

(set! *unchecked-math* :warn-on-boxed)

(defn- blank-var? [v]
  (when (logic-var? v)
    (re-find #"^_\d*$" (name v))))

(defn- normalize-triple-clause [{:keys [e a v] :as clause}]
  (cond-> clause
    (or (blank-var? v)
        (nil? v))
    (assoc :v (gensym "_"))
    (blank-var? e)
    (assoc :e (gensym "_"))
    (nil? a)
    (assoc :a :crux.db/id)))

(def ^:private pred->built-in-range-pred {< (comp neg? compare)
                                          <= (comp not pos? compare)
                                          > (comp pos? compare)
                                          >= (comp not neg? compare)
                                          = =})

(def ^:private range->inverse-range '{< >
                                      <= >=
                                      > <
                                      >= <=
                                      = =})

(defn- maybe-unquote [x]
  (if (and (seq? x) (= 'quote (first x)) (= 2 (count x)))
    (recur (second x))
    x))

(defn- rewrite-self-join-triple-clause [{:keys [e v] :as triple}]
  (let [v-var (gensym (str "self-join_" v "_"))]
    {:triple [(with-meta
                (assoc triple :v v-var)
                {:self-join? true})]
     :pred [{:pred {:pred-fn '== :args [v-var e]}}]}))

(defn- normalize-clauses [clauses]
  (->> (for [[type clause] clauses]
         (if (= :triple type)
           (let [{:keys [e v] :as clause} (normalize-triple-clause clause)]
             (if (and (logic-var? e) (= e v))
               (rewrite-self-join-triple-clause clause)
               {:triple [clause]}))

           (case type
             :pred {:pred [(let [{:keys [pred return]} clause
                                 {:keys [pred-fn args]} pred
                                 clause (if-let [range-pred (and (= 2 (count args))
                                                                 (every? logic-var? args)
                                                                 (get pred->built-in-range-pred pred-fn))]
                                          (assoc-in clause [:pred :pred-fn] range-pred)
                                          clause)]
                             (if return
                               (assoc clause :return (w/postwalk #(if (blank-var? %)
                                                                    (gensym "_")
                                                                    %)
                                                                 return))
                               clause))]}

             :range (let [[order clause] (first clause)
                          [order clause] (if (= :sym-sym order) ;; NOTE: to deal with rule expansion
                                           (let [{:keys [op sym-a sym-b]} clause]
                                             (cond
                                               (literal? sym-a)
                                               [:val-sym {:op op :val sym-a :sym sym-b}]
                                               (literal? sym-b)
                                               [:sym-val {:op op :val sym-b :sym sym-a}]
                                               :else
                                               [order clause]))
                                           [order clause])
                          {:keys [op sym val] :as clause} (cond-> clause
                                                            (= :val-sym order) (update :op range->inverse-range))]
                      (if (and (not= :sym-sym order)
                               (not (logic-var? sym)))
                        {:pred [{:pred {:pred-fn (get pred->built-in-range-pred (var-get (resolve op)))
                                        :args [sym val]}}]}
                        {:range [clause]}))

             {type [clause]})))

       (apply merge-with into)))

(defn- find-binding-vars [binding]
  (some->> binding (vector) (flatten) (filter logic-var?)))

(defn- distinct-vars? [vars]
  (= (count vars) (count (set vars))))

(defn- collect-vars [{triple-clauses :triple
                      not-clauses :not
                      not-join-clauses :not-join
                      or-clauses :or
                      or-join-clauses :or-join
                      pred-clauses :pred
                      range-clauses :range
                      rule-clauses :rule}]
  (let [or-vars (->> (for [or-clause or-clauses
                           [type sub-clauses] or-clause]
                       (collect-vars (normalize-clauses (case type
                                                          :term [sub-clauses]
                                                          :and sub-clauses))))
                     (apply merge-with set/union))
        not-join-vars (set (for [not-join-clause not-join-clauses
                                 arg (:args not-join-clause)]
                             arg))
        not-vars (->> (for [not-clause not-clauses]
                        (collect-vars (normalize-clauses not-clause)))
                      (apply merge-with set/union))
        or-join-vars (set (for [or-join-clause or-join-clauses
                                :let [{:keys [bound-args free-args]} (:args or-join-clause)]
                                arg (concat bound-args free-args)]
                            arg))]
    {:e-vars (set (for [{:keys [e]} triple-clauses
                        :when (logic-var? e)]
                    e))
     :v-vars (set (for [{:keys [v]} triple-clauses
                        :when (logic-var? v)]
                    v))
     :not-vars (->> (vals not-vars)
                    (reduce into not-join-vars))
     :pred-vars (set (for [{:keys [pred return]} pred-clauses
                           :let [return-vars (find-binding-vars return)]
                           var (concat return-vars
                                       (cond->> (:args pred)
                                         (not (pred-constraint? (:pred-fn pred))) (cons (:pred-fn pred))))
                           :when (logic-var? var)]
                       var))
     :pred-return-vars (set (for [{:keys [pred return]} pred-clauses
                                  return-var (find-binding-vars return)]
                              return-var))
     :range-vars (set (for [{:keys [sym sym-a sym-b]} range-clauses
                            sym [sym sym-a sym-b]
                            :when (logic-var? sym)]
                        sym))
     :or-vars (apply set/union (vals or-vars))
     :rule-vars (set/union (set (for [{:keys [args]} rule-clauses
                                      arg args
                                      :when (logic-var? arg)]
                                  arg))
                           or-join-vars)}))

(defn- sort-triple-clauses [stats triple-clauses]
  (sort-by (fn [{:keys [a]}]
             (get stats a 0)) triple-clauses))

(defn- triple-join-order [triple-clauses in-vars range-vars stats]
  (let [var->frequency (->> (concat (map :e triple-clauses)
                                    (map :v triple-clauses)
                                    range-vars)
                            (filter logic-var?)
                            (frequencies))
        triple-clauses (sort-triple-clauses stats triple-clauses)
        literal-clauses (for [{:keys [e v] :as clause} triple-clauses
                              :when (or (literal? e)
                                        (literal? v))]
                          clause)
        literal-join-order (concat (for [{:keys [e v]} literal-clauses]
                                     (if (literal? v)
                                       v
                                       e))
                                   in-vars
                                   (for [{:keys [e v]} literal-clauses]
                                     (if (literal? v)
                                       e
                                       v)))
        self-join-clauses (filter (comp :self-join? meta) triple-clauses)
        self-join-vars (map :v self-join-clauses)
        join-order (loop [join-order (concat literal-join-order self-join-vars)
                          clauses (remove (set literal-clauses) triple-clauses)]
                     (let [join-order-set (set join-order)
                           clause (first (or (seq (for [{:keys [e v] :as clause} clauses
                                                        :when (or (contains? join-order-set e)
                                                                  (contains? join-order-set v))]
                                                    clause))
                                             clauses))]
                       (if-let [{:keys [e a v]} clause]
                         (recur (->> (sort-by var->frequency [v e])
                                     (reverse)
                                     (concat join-order))
                                (remove #{clause} clauses))
                         join-order)))
        join-order (filter logic-var? join-order)]
    (log/debug :triple-joins-var->frequency var->frequency)
    (log/debug :triple-joins-join-order join-order)
    (distinct join-order)))

(defn- pred-joins [pred-clauses]
  (->> pred-clauses
       (reduce
        (fn [[pred-clause+idx-ids var->joins] {:keys [return] :as pred-clause}]
          (if return
            (let [idx-id (gensym "pred-return")
                  return-vars (find-binding-vars return)
                  join {:id idx-id
                        :idx-fn
                        (fn [_ index-snapshot _]
                          (idx/new-relation-virtual-index []
                                                          (count return-vars)
                                                          (partial db/encode-value index-snapshot)))}]
              [(conj pred-clause+idx-ids [pred-clause idx-id])
               (->> return-vars
                    (reduce
                     (fn [var->joins return-var]
                       (->> {return-var [join]}
                            (merge-with into var->joins)))
                     var->joins))])
            [(conj pred-clause+idx-ids [pred-clause])
             var->joins]))
        [[] {}])))

;; TODO: This is a naive, but not totally irrelevant measure. Aims to
;; bind variables as early and cheaply as possible.
(defn- clause-complexity [clause]
  (count (cio/pr-edn-str clause)))

(defn- triple-pred-clauses [triple-clauses range-vars known-vars stats]
  (let [triple-join-order (triple-join-order triple-clauses
                                             known-vars
                                             range-vars
                                             stats)
        literal-clauses (for [{:keys [e v] :as clause} triple-clauses
                              :when (or (literal? e)
                                        (literal? v))]
                          clause)
        literal-vars (set (for [{:keys [e v] :as clause} literal-clauses
                                var [e v]
                                :when (logic-var? var)]
                            var))
        literal-preds (for [{:keys [e a v] :as clause} literal-clauses
                            :let [snapshot-id (gensym 'snapshot-id)]]
                        (cond
                          (and (literal? e) (literal? v))
                          {:pred {:pred-fn (fn literal-ev-triple [{:keys [entity-resolver-fn open-nested-index-snapshot-fn] :as db}]
                                             (let [nested-index-snapshot (open-nested-index-snapshot-fn snapshot-id)]
                                               (some->> (db/aev nested-index-snapshot a e v entity-resolver-fn)
                                                        (not-empty)
                                                        (first)
                                                        (db/decode-value nested-index-snapshot)
                                                        (= v))))
                                  :args ['$]}}

                          (literal? e)
                          {:pred {:pred-fn (let [id (gensym 'snapshot-id)]
                                             (fn literal-e-triple [{:keys [entity-resolver-fn open-nested-index-snapshot-fn] :as db}]
                                               (let [nested-index-snapshot (open-nested-index-snapshot-fn snapshot-id)]
                                                 (idx/new-index-store-index
                                                  (fn [v]
                                                    (db/aev nested-index-snapshot a e v entity-resolver-fn))))))
                                  :args ['$]}
                           :return [:collection [v '...]]}

                          (literal? v)
                          {:pred {:pred-fn (fn literal-v-triple [{:keys [entity-resolver-fn open-nested-index-snapshot-fn] :as db}]
                                             (let [nested-index-snapshot (open-nested-index-snapshot-fn snapshot-id)]
                                               (idx/new-index-store-index
                                                (fn [e]
                                                  (db/ave nested-index-snapshot a v e entity-resolver-fn)))))
                                  :args ['$]}
                           :return [:collection [e '...]]}))
        triple-clauses (set (remove (set literal-clauses) triple-clauses))]
    (first
     (reduce
      (fn [[acc known-vars clauses] var]
        (let [triple-preds (for [{:keys [e a v] :as clause} clauses
                                 :when (or (= e var) (= v var))
                                 :let [snapshot-id (gensym 'snapshot-id)]]
                             (cond
                               (= e var)
                               (if (contains? known-vars v)
                                 {:pred {:pred-fn (fn ave-triple [{:keys [entity-resolver-fn open-nested-index-snapshot-fn] :as db} v]
                                                    (let [nested-index-snapshot (open-nested-index-snapshot-fn snapshot-id)]
                                                      (idx/new-index-store-index
                                                       (fn [e]
                                                         (db/ave nested-index-snapshot a v e entity-resolver-fn)))))
                                         :args ['$ v]}
                                  :return [:collection [e '...]]}
                                 {:pred {:pred-fn (fn ae-triple [{:keys [entity-resolver-fn open-nested-index-snapshot-fn] :as db}]
                                                    (let [nested-index-snapshot (open-nested-index-snapshot-fn snapshot-id)]
                                                      (idx/new-index-store-index
                                                       (fn [e]
                                                         (db/ae nested-index-snapshot a e entity-resolver-fn)))))
                                         :args ['$]}
                                  :return [:collection [e '...]]})

                               (= v var)
                               (if (contains? known-vars e)
                                 {:pred {:pred-fn (fn aev-triple [{:keys [entity-resolver-fn open-nested-index-snapshot-fn] :as db} e]
                                                    (let [nested-index-snapshot (open-nested-index-snapshot-fn snapshot-id)]
                                                      (idx/new-index-store-index
                                                       (fn [v]
                                                         (db/aev nested-index-snapshot a e v entity-resolver-fn)))))
                                         :args ['$ e]}
                                  :return [:collection [v '...]]}
                                 {:pred {:pred-fn (fn av-triple [{:keys [entity-resolver-fn open-nested-index-snapshot-fn] :as db}]
                                                    (let [nested-index-snapshot (open-nested-index-snapshot-fn snapshot-id)]
                                                      (idx/new-index-store-index
                                                       (fn [v]
                                                         (db/av nested-index-snapshot a v entity-resolver-fn)))))
                                         :args ['$]}
                                  :return [:collection [v '...]]})))
              known-vars (conj known-vars var)]
          [(into acc triple-preds)
           known-vars
           (for [{:keys [e v] :as clause} clauses
                 :when (not (and (contains? known-vars e)
                                 (contains? known-vars v)))]
             clause)]))
      [literal-preds known-vars triple-clauses]
      triple-join-order))))

(defn- build-or-pred-clause [clause [{:keys [free-vars bound-vars]} :as or-branches]]
  (let [has-free-vars? (boolean (seq free-vars))
        bound-vars (vec bound-vars)
        or-return (gensym 'or-return)
        or-branches (for [[n or-branch] (map-indexed vector or-branches)]
                      (assoc or-branch
                             :branch-index n
                             :branch-return (symbol (str or-return "_" n))))
        or-branch-returns (mapv :branch-return or-branches)
        {:keys [rule-name]} (meta clause)]
    (cons
     (if has-free-vars?
       {:pred {:pred-fn set/union
               :args or-branch-returns}
        :return [:relation [(vec free-vars)]]}
       {:pred {:pred-fn (fn or-exists-check [& args]
                          (boolean (some not-empty args)))
               :args or-branch-returns}})
     (for [{:keys [where branch-return branch-index] :as or-branch} or-branches]
       {:pred
        {:pred-fn 'q
         :args (vec (cons (with-meta
                            (cond-> {:find (if has-free-vars?
                                             (vec free-vars)
                                             [(first bound-vars)])
                                     :in (vec (cons '$ bound-vars))
                                     :where (s/unform ::where where)}
                              (not has-free-vars?) (assoc :limit 1))
                            {:rule-name rule-name
                             :branch-index branch-index})
                          bound-vars))}
        :return [:scalar branch-return]}))))

(defn- or-pred-clauses [or-type or-clauses known-vars]
  (->> (sort-by clause-complexity or-clauses)
       (reduce
        (fn [[acc known-vars] clause]
          (let [or-join? (= :or-join or-type)
                or-branches (for [[type sub-clauses] (case or-type
                                                       :or clause
                                                       :or-join (:body clause))
                                  :let [{:keys [bound-args free-args]} (:args clause)
                                        where (case type
                                                :term [sub-clauses]
                                                :and sub-clauses)
                                        body-vars (->> (collect-vars (normalize-clauses where))
                                                       (vals)
                                                       (reduce into #{}))
                                        body-vars (set (remove blank-var? body-vars))
                                        or-vars (if or-join?
                                                  (set (concat bound-args free-args))
                                                  body-vars)
                                        [free-vars
                                         bound-vars] (if (and or-join? (not (empty? bound-args)))
                                                       [free-args bound-args]
                                                       [(set/difference or-vars known-vars)
                                                        (set/intersection or-vars known-vars)])]]
                              (do (when or-join?
                                    (when-not (distinct-vars? free-args)
                                      (throw (IllegalArgumentException.
                                              (str "Or join free variables not distinct: " (cio/pr-edn-str clause)))))
                                    (doseq [var or-vars
                                            :when (not (contains? body-vars var))]
                                      (throw (IllegalArgumentException.
                                              (str "Or join variable never used: " var " " (cio/pr-edn-str clause))))))
                                  {:or-vars or-vars
                                   :free-vars free-vars
                                   :bound-vars bound-vars
                                   :where where}))
                free-vars (:free-vars (first or-branches))]
            (when (not (apply = (map :or-vars or-branches)))
              (throw (IllegalArgumentException.
                      (str "Or requires same logic variables: " (cio/pr-edn-str clause)))))
            [(into acc (build-or-pred-clause clause or-branches))
             (into known-vars free-vars)]))
        [[] known-vars])))

(defn- not-pred-clauses [not-type not-clauses]
  (reduce
   (fn [acc not-clause]
     (let [[not-vars not-clause] (case not-type
                                   :not [(:not-vars (collect-vars (normalize-clauses [[:not not-clause]])))
                                         not-clause]
                                   :not-join [(:args not-clause)
                                              (:body not-clause)])
           not-vars (vec (remove blank-var? not-vars))
           not-return (gensym 'not-return)]
       (into acc [{:pred
                   {:pred-fn 'q
                    :args (vec (cons {:find not-vars
                                      :in (vec (cons '$ not-vars))
                                      :where (s/unform ::where not-clause)
                                      :limit 1}
                                     not-vars))}
                   :return [:scalar not-return]}
                  {:pred {:pred-fn empty? :args [not-return]}}])))
   []
   not-clauses))

(defn- in-pred-clauses [in-bindings]
  (reduce
   (fn [acc [n in-binding]]
     (conj acc {:pred
                {:pred-fn get-in
                 :args ['$ [:in-args n]]}
                :return in-binding}))
   []
   (map-indexed vector in-bindings)))

(defrecord VarBinding [var result-index])

(defn- build-pred-return-var-bindings [var->values-result-index pred-clauses]
  (->> (for [{:keys [return]} pred-clauses
             return-var (find-binding-vars return)
             :let [result-index (get var->values-result-index return-var)]]
         [return-var (map->VarBinding {:var return-var :result-index result-index})])
       (into {})))

(defn- calculate-constraint-join-depth [var->bindings vars]
  (->> (for [var vars]
         (get-in var->bindings [var :result-index] -1))
       (apply max -1)
       (long)
       (inc)))

(defn- new-range-constraint-wrapper-fn [op ^Box val]
  (case op
    = #(idx/new-equals-virtual-index % val)
    < #(-> (idx/new-less-than-virtual-index % val)
           (idx/new-prefix-equal-virtual-index val c/value-type-id-size))
    <= #(-> (idx/new-less-than-equal-virtual-index % val)
            (idx/new-prefix-equal-virtual-index val c/value-type-id-size))
    > #(-> (idx/new-greater-than-virtual-index % val)
           (idx/new-prefix-equal-virtual-index val c/value-type-id-size))
    >= #(-> (idx/new-greater-than-equal-virtual-index % val)
            (idx/new-prefix-equal-virtual-index val c/value-type-id-size))))

;; TODO: Get rid of assumption that value-buffer-type-id is always one
;; byte. Or better, move construction or handling of ranges to the
;; IndexSnapshot and remove the need for the type-prefix completely.
(defn- build-var-range-constraints [encode-value-fn range-clauses var->bindings]
  (doseq [{:keys [sym sym-a sym-b] :as clause} range-clauses
          var [sym sym-a sym-b]
          :when (logic-var? var)]
    (when-not (contains? var->bindings var)
      (throw (IllegalArgumentException.
              (str "Range constraint refers to unknown variable: " var " " (cio/pr-edn-str clause))))))
  (->> (for [[var clauses] (group-by :sym range-clauses)
             :when (logic-var? var)]
         [var (->> (for [{:keys [op val sym]} clauses
                         :let [val (encode-value-fn val)]]
                     (new-range-constraint-wrapper-fn op (Box. val)))
                   (apply comp))])
       (into {})))

(defn- build-logic-var-range-constraint-fns [encode-value-fn range-clauses var->bindings]
  (->> (for [{:keys [op sym-a sym-b] :as clause} range-clauses
             :when (and (logic-var? sym-a)
                        (logic-var? sym-b))
             :let [sym+index [[sym-a (.result-index ^VarBinding (get var->bindings sym-a))]
                              [sym-b (.result-index ^VarBinding (get var->bindings sym-b))]]
                   [[first-sym first-index]
                    [second-sym second-index] :as sym+index] (sort-by second sym+index)
                   op (if (= sym-a first-sym)
                        (get range->inverse-range op)
                        op)]]
         {second-sym
          [(fn []
             (let [range-join-depth (calculate-constraint-join-depth var->bindings [first-sym])
                   val (Box. mem/empty-buffer)]
               {:join-depth range-join-depth
                :range-constraint-wrapper-fn (new-range-constraint-wrapper-fn op val)
                :constraint-fn (fn range-constraint [index-snapshot db idx-id->idx ^List join-keys]
                                 (set! (.-val val) (.get join-keys first-index))
                                 true)}))]})
       (apply merge-with into {})))

(defn- bound-result-for-var [index-snapshot ^VarBinding var-binding ^List join-keys]
  (->> (.get join-keys (.result-index var-binding))
       (db/decode-value index-snapshot)))

(defn- validate-existing-vars [var->bindings clause vars]
  (doseq [var vars
          :when (not (or (pred-constraint? var)
                         (contains? var->bindings var)))]
    (throw (IllegalArgumentException.
            (str "Clause refers to unknown variable: "
                 var " " (cio/pr-edn-str clause))))))

(defn- bind-binding [bind-type idx result]
  (case bind-type
    :scalar
    (do (idx/update-relation-virtual-index! idx [[result]])
        true)

    :collection
    (if (satisfies? db/Index result)
      (do (idx/update-relation-virtual-index! idx result identity true)
          true)
      (do (idx/update-relation-virtual-index! idx (mapv vector result))
          (not-empty result)))

    (:tuple :relation)
    (throw (IllegalStateException. "Should have flattened away all tuple and relation bindings."))

    result))

(defmethod pred-constraint 'get-attr [_ {:keys [idx-id arg-bindings return-type] :as pred-ctx}]
  (let [arg-bindings (rest arg-bindings)
        [e-var attr not-found] arg-bindings
        not-found? (= 3 (count arg-bindings))
        e-result-index (.result-index ^VarBinding e-var)]
    (fn pred-get-attr-constraint [index-snapshot {:keys [entity-resolver-fn] :as db} idx-id->idx ^List join-keys]
      (let [e (.get join-keys e-result-index)
            vs (db/aev index-snapshot attr e nil entity-resolver-fn)]
        (if (and (seq vs) (= :collection return-type))
          (do (idx/update-relation-virtual-index! (get idx-id->idx idx-id) vs identity true)
              true)
          (let [values (if (and (empty? vs) not-found?)
                         [not-found]
                         (mapv #(db/decode-value index-snapshot %) vs))]
            (bind-binding return-type (get idx-id->idx idx-id) (not-empty values))))))))

(def ^:private ^:dynamic *recursion-table* {})

(defmethod pred-constraint 'q [_ {:keys [idx-id arg-bindings rule-name->rules return-type]
                                  :as pred-ctx}]
  (let [query (normalize-query (second arg-bindings))
        {:keys [rule-name branch-index]} (meta query)
        parent-rules (:rules (meta rule-name->rules))]
    (fn pred-constraint [index-snapshot db idx-id->idx join-keys]
      (let [[_ _ & args] (for [arg-binding arg-bindings]
                           (if (instance? VarBinding arg-binding)
                             (bound-result-for-var index-snapshot arg-binding join-keys)
                             arg-binding))
            cache-key (when rule-name
                        [rule-name branch-index args])
            query (cond-> query
                    (seq parent-rules) (update :rules (comp vec concat) parent-rules)
                    (nil? return-type) (assoc :limit 1))]
        (if-let [cached-result (when cache-key
                                 (get *recursion-table* cache-key))]
          cached-result
          (binding [*recursion-table* (if cache-key
                                        (assoc *recursion-table* cache-key [])
                                        *recursion-table*)]
            (with-open [pred-result (.openQuery ^ICruxDatasource db query (object-array args))]
              (bind-binding return-type (get idx-id->idx idx-id) (iterator-seq pred-result)))))))))

(defn- built-in-unification-pred [unifier-fn {:keys [encode-value-fn arg-bindings]}]
  (let [arg-bindings (vec (for [arg-binding (rest arg-bindings)]
                            (if (instance? VarBinding arg-binding)
                              arg-binding
                              (->> (map encode-value-fn (c/vectorize-value arg-binding))
                                   (into (sorted-set-by mem/buffer-comparator))))))]
    (fn unification-constraint [index-snapshot db idx-id->idx ^List join-keys]
      (let [values (for [arg-binding arg-bindings]
                     (if (instance? VarBinding arg-binding)
                       (sorted-set-by mem/buffer-comparator (.get join-keys (.result-index ^VarBinding arg-binding)))
                       arg-binding))]
        (unifier-fn values)))))

(defmethod pred-constraint '== [_ pred-ctx]
  (built-in-unification-pred #(boolean (not-empty (apply set/intersection %))) pred-ctx))

(defmethod pred-constraint '!= [_ pred-ctx]
  (built-in-unification-pred #(empty? (apply set/intersection %)) pred-ctx))

(defmethod pred-constraint :default [_ {:keys [idx-id arg-bindings return-type]
                                        :as pred-ctx}]
  (fn pred-constraint [index-snapshot db idx-id->idx join-keys]
    (let [[pred-fn & args] (for [arg-binding arg-bindings]
                             (cond
                               (instance? VarBinding arg-binding)
                               (bound-result-for-var index-snapshot arg-binding join-keys)

                               (= '$ arg-binding)
                               db

                               :else
                               arg-binding))
          pred-result (apply pred-fn args)]
      (binding [nippy/*freeze-fallback* :write-unfreezable]
        (bind-binding return-type (get idx-id->idx idx-id) pred-result)))))

(defn- build-pred-constraints [rule-name->rules encode-value-fn pred-clause+idx-ids var->bindings vars-in-join-order]
  (for [[{:keys [pred return] :as clause} idx-id] pred-clause+idx-ids
        :let [{:keys [pred-fn args]} pred
              pred-vars (filter logic-var? (cons pred-fn args))
              pred-join-depth (calculate-constraint-join-depth var->bindings pred-vars)
              arg-bindings (for [arg (cons pred-fn args)]
                             (if (and (logic-var? arg)
                                      (not (pred-constraint? arg)))
                               (get var->bindings arg)
                               (maybe-unquote arg)))
              return-vars (find-binding-vars return)
              return-type (first return)
              pred-ctx {:encode-value-fn encode-value-fn
                        :idx-id idx-id
                        :arg-bindings arg-bindings
                        :return-type return-type
                        :rule-name->rules rule-name->rules}]]
    (do (validate-existing-vars var->bindings clause pred-vars)
        (when-not (distinct-vars? return-vars)
          (throw (IllegalArgumentException.
                  (str "Return variables not distinct: " (cio/pr-edn-str clause)))))
        (s/assert ::pred-args (cond-> [pred-fn (vec args)]
                                return (conj (second return))))
        {:join-depth pred-join-depth
         :constraint-fn (pred-constraint clause pred-ctx)})))

(defn- constrain-join-result-by-constraints [index-snapshot db idx-id->idx depth->constraints join-keys]
  (->> (get depth->constraints (count join-keys))
       (every? (fn [f]
                 (f index-snapshot db idx-id->idx join-keys)))))

(defn- calculate-join-order [pred-clauses]
  (let [g (reduce
           (fn [g {:keys [pred return] :as pred-clause}]
             (let [pred-vars (filter logic-var? (:args pred))]
               (->> (for [pred-var (cons ::root pred-vars)
                          :when return
                          return-var (find-binding-vars return)]
                      [return-var pred-var])
                    (reduce
                     (fn [g [r a]]
                       (dep/depend g r a))
                     g))))
           (dep/graph)
           pred-clauses)
        join-order (dep/topo-sort g)]
    (vec (remove #{::root} join-order))))

(defn- rule-name->rules [rules]
  (group-by (comp :name :head) rules))

(defn- expand-rules [where rule-name->rules recursion-cache]
  (->> (for [[type clause :as sub-clause] where]
         (if (= :rule type)
           (let [rule-name (:name clause)
                 rules (get rule-name->rules rule-name)]
             (when-not rules
               (throw (IllegalArgumentException.
                       (str "Unknown rule: " (cio/pr-edn-str sub-clause)))))
             (let [rule-args+num-bound-args+body (for [{:keys [head body]} rules
                                                       :let [{:keys [bound-args free-args]} (:args head)]]
                                                   [(vec (concat bound-args free-args))
                                                    (count bound-args)
                                                    body])
                   [arity :as arities] (->> rule-args+num-bound-args+body
                                            (map (comp count first))
                                            (distinct))

                   [num-bound-args :as num-bound-args-groups] (->> rule-args+num-bound-args+body
                                                                   (map second)
                                                                   (distinct))]
               (when-not (= 1 (count arities))
                 (throw (IllegalArgumentException. (str "Rule definitions require same arity: " (cio/pr-edn-str rules)))))
               (when-not (= 1 (count num-bound-args-groups))
                 (throw (IllegalArgumentException. (str "Rule definitions require same number of bound args: " (cio/pr-edn-str rules)))))
               (when-not (= arity (count (:args clause)))
                 (throw (IllegalArgumentException.
                         (str "Rule invocation has wrong arity, expected: " arity " " (cio/pr-edn-str sub-clause)))))
               ;; TODO: the caches and expansion here needs
               ;; revisiting.
               (let [expanded-rules (for [[branch-index [args _ body]] (map-indexed vector rule-args+num-bound-args+body)
                                          :let [rule-arg->query-arg (zipmap args (:args clause))
                                                body-vars (->> (collect-vars (normalize-clauses body))
                                                               (vals)
                                                               (reduce into #{}))
                                                body-var->hidden-var (zipmap body-vars
                                                                             (map gensym body-vars))]]
                                      (w/postwalk-replace (merge body-var->hidden-var rule-arg->query-arg) body))
                     cache-key [:seen-rules rule-name]
                     ;; TODO: Understand this, does this really work
                     ;; in the general case?
                     expanded-rules (if (zero? (long (get-in recursion-cache cache-key 0)))
                                      (for [expanded-rule expanded-rules
                                            :let [expanded-rule (expand-rules expanded-rule rule-name->rules
                                                                              (update-in recursion-cache cache-key (fnil inc 0)))]
                                            :when (seq expanded-rule)]
                                        expanded-rule)
                                      expanded-rules)]
                 (if (= 1 (count expanded-rules))
                   (first expanded-rules)
                   (when (seq expanded-rules)
                     (let [[bound-args free-args] (split-at num-bound-args (:args clause))]
                       [[:or-join
                         (with-meta
                           {:args (cond-> {:free-args (vec (filter logic-var? free-args))}
                                    (seq (filter logic-var? bound-args)) (assoc :bound-args (vec (filter logic-var? bound-args))))
                            :body (vec (for [expanded-rule expanded-rules]
                                         [:and expanded-rule]))}
                           {:rule-name rule-name})]]))))))
           [sub-clause]))
       (reduce into [])))

;; NOTE: this isn't exact, used to detect vars that can be bound
;; before an or sub query. Is there a better way to incrementally
;; build up the join order? Done until no new vars are found to catch
;; all vars, doesn't care about cyclic dependencies, these will be
;; caught by the real dependency check later.
(defn- add-pred-returns-bound-at-top-level [known-vars pred-clauses]
  (let [new-known-vars (->> pred-clauses
                            (reduce
                             (fn [acc {:keys [pred return]}]
                               (if (->> (cond->> (:args pred)
                                          (not (pred-constraint? (:pred-fn pred))) (cons (:pred-fn pred)))
                                        (filter logic-var?)
                                        (set)
                                        (set/superset? acc))
                                 (apply conj acc (find-binding-vars return))
                                 acc))
                             known-vars))]
    (if (= new-known-vars known-vars)
      new-known-vars
      (recur new-known-vars pred-clauses))))

(defn- expand-leaf-preds [{triple-clauses :triple
                           pred-clauses :pred
                           :as type->clauses}
                          in-vars
                          stats]
  (let [collected-vars (collect-vars type->clauses)
        invalid-leaf-vars (set (concat in-vars (:e-vars collected-vars)))
        non-leaf-v-vars (set (for [[v-var non-leaf-group] (group-by :v triple-clauses)
                                   :when (> (count non-leaf-group) 1)]
                               v-var))
        potential-leaf-v-vars (set/difference (:v-vars collected-vars) invalid-leaf-vars non-leaf-v-vars)
        leaf-groups (->> (for [[e-var leaf-group] (group-by :e (filter (comp potential-leaf-v-vars :v) triple-clauses))
                               :when (> (count leaf-group) 1)]
                           [e-var (sort-triple-clauses stats leaf-group)])
                         (into {}))
        leaf-triple-clauses (set (mapcat next (vals leaf-groups)))
        triple-clauses (remove leaf-triple-clauses triple-clauses)
        leaf-preds (for [{:keys [e a v]} leaf-triple-clauses]
                     {:pred {:pred-fn 'get-attr :args [e a]}
                      :return [:collection [v '...]]})]
    (assoc type->clauses
           :triple triple-clauses
           :pred (vec (concat pred-clauses leaf-preds)))))

(defn- update-depth->constraints [depth->join-depth constraints]
  (reduce
   (fn [acc {:keys [join-depth constraint-fn]}]
     (update acc join-depth (fnil conj []) constraint-fn))
   depth->join-depth
   constraints))

(defn- flatten-pred-clauses [pred-clauses join-order]
  (->> (for [{:keys [return pred] :as clause} pred-clauses
             :let [[return-type return-binding] return
                   return-vars (find-binding-vars return-binding)]]
         (if-not (distinct-vars? return-vars)
           (throw (IllegalArgumentException.
                   (str "Return variables not distinct: " (cio/pr-edn-str clause))))
           (case return-type
             :scalar
             (let [scalar-var (gensym (str "scalar_" return-binding))]
               [{:pred pred
                 :return [:scalar scalar-var]}
                {:pred {:pred-fn (fn scalar->collection [{:keys [index-snapshot] :as db} scalar]
                                   (binding [nippy/*freeze-fallback* :write-unfreezable]
                                     (idx/new-singleton-virtual-index scalar (partial db/encode-value index-snapshot))))
                        :args ['$ scalar-var]}
                 :return [:collection [return-binding '...]]}])

             :tuple
             (let [tuple-var (gensym (str "tuple_" (string/join "_" return-binding)))]
               (cons {:pred pred
                      :return [:scalar tuple-var]}
                     (for [[idx var] (map-indexed vector return-binding)]
                       {:pred {:pred-fn
                               (fn scalar-tuple->collection [{:keys [index-snapshot] :as db} tuple idx]
                                 (binding [nippy/*freeze-fallback* :write-unfreezable]
                                   (idx/new-singleton-virtual-index (nth tuple idx nil) (partial db/encode-value index-snapshot))))
                               :args ['$ tuple-var idx]}
                        :return [:collection [var '...]]})))

             :relation
             (let [return-binding (first return-binding)
                   relation-var (gensym (str "relation_" (string/join "_" return-binding)))
                   reordered-relation-var (gensym (str "reordered-relation_" (string/join "_" return-binding)))
                   tuple-vars-in-join-order (keep (set return-binding) join-order)
                   tuple-idxs-in-join-order (mapv (zipmap return-binding (range))
                                                  tuple-vars-in-join-order)]
               (concat
                [{:pred pred
                  :return [:scalar relation-var]}
                 {:pred {:pred-fn (fn scalar-relation->scalar-reordered-relation [{:keys [index-snapshot] :as db} relation]
                                    (binding [nippy/*freeze-fallback* :write-unfreezable]
                                      (->> (for [tuple relation]
                                             (mapv #(db/encode-value index-snapshot (nth tuple % nil)) tuple-idxs-in-join-order))
                                           (reduce
                                            (fn [acc tuple]
                                              (idx/tree-map-put-in acc tuple nil))
                                            (TreeMap. mem/buffer-comparator)))))
                         :args ['$ relation-var]}
                  :return [:scalar reordered-relation-var]}]
                (first
                 (reduce
                  (fn [[acc path] var]
                    [(conj acc {:pred {:pred-fn (fn scalar-reordered-relation->collection [{:keys [index-snapshot] :as db} relation & path]
                                                  (let [path (mapv #(db/encode-value index-snapshot %) path)]
                                                    (idx/new-sorted-virtual-index (get-in relation path))))
                                       :args (vec (cons '$ (cons reordered-relation-var path)))}
                                :return [:collection [var '...]]})
                     (conj path var)])
                  [[] []]
                  tuple-vars-in-join-order))))
             :collection
             [clause]
             [clause])))
       (reduce into [])))

(defn- compile-sub-query [encode-value-fn where in rule-name->rules stats]
  (let [where (expand-rules where rule-name->rules {})
        in-vars (set (find-binding-vars (:bindings in)))
        {triple-clauses :triple
         range-clauses :range
         pred-clauses :pred
         not-clauses :not
         not-join-clauses :not-join
         or-clauses :or
         or-join-clauses :or-join
         :as type->clauses} (expand-leaf-preds (normalize-clauses where) in-vars stats)
        {:keys [e-vars
                v-vars
                range-vars]} (collect-vars type->clauses)
        top-level-known-vars (add-pred-returns-bound-at-top-level in-vars pred-clauses)
        known-vars (set/union e-vars v-vars in-vars)
        known-vars (add-pred-returns-bound-at-top-level known-vars pred-clauses)
        [or-preds known-vars] (or-pred-clauses :or or-clauses known-vars)
        [or-join-preds known-vars] (or-pred-clauses :or-join or-join-clauses known-vars)
        pred-clauses (concat pred-clauses
                             (in-pred-clauses (:bindings in))
                             or-preds
                             or-join-preds
                             (not-pred-clauses :not not-clauses)
                             (not-pred-clauses :not-join not-join-clauses)
                             (triple-pred-clauses triple-clauses range-vars top-level-known-vars stats))
        pred-clauses (flatten-pred-clauses pred-clauses (calculate-join-order pred-clauses))
        [pred-clause+idx-ids var->joins] (pred-joins pred-clauses)
        join-depth (count var->joins)
        vars-in-join-order (calculate-join-order pred-clauses)
        var->values-result-index (zipmap vars-in-join-order (range))
        var->bindings (build-pred-return-var-bindings var->values-result-index pred-clauses)
        var->range-constraints (build-var-range-constraints encode-value-fn range-clauses var->bindings)
        var->logic-var-range-constraint-fns (build-logic-var-range-constraint-fns encode-value-fn range-clauses var->bindings)
        pred-constraints (build-pred-constraints rule-name->rules encode-value-fn pred-clause+idx-ids var->bindings vars-in-join-order)
        depth->constraints (update-depth->constraints (vec (repeat join-depth nil)) pred-constraints)]
    {:depth->constraints depth->constraints
     :var->range-constraints var->range-constraints
     :var->logic-var-range-constraint-fns var->logic-var-range-constraint-fns
     :vars-in-join-order vars-in-join-order
     :var->joins var->joins
     :var->bindings var->bindings}))

(defn- build-idx-id->idx [db index-snapshot {:keys [var->joins] :as compiled-query}]
  (->> (for [[_ joins] var->joins
             {:keys [id idx-fn] :as join} joins
             :when id]
         [id idx-fn])
       (reduce
        (fn [acc [id idx-fn]]
          (if (contains? acc id)
            acc
            (assoc acc id (idx-fn db index-snapshot compiled-query))))
        {})))

(defn- add-logic-var-constraints [{:keys [var->logic-var-range-constraint-fns]
                                   :as compiled-query}]
  (if (seq var->logic-var-range-constraint-fns)
    (let [logic-var+range-constraint (for [[v fs] var->logic-var-range-constraint-fns
                                           f fs]
                                       [v (f)])]
      (-> compiled-query
          (update :depth->constraints update-depth->constraints (map second logic-var+range-constraint))
          (update :var->range-constraints (fn [var->range-constraints]
                                            (reduce (fn [acc [v {:keys [range-constraint-wrapper-fn]}]]
                                                      (update acc v (fn [x]
                                                                      (cond-> range-constraint-wrapper-fn
                                                                        x (comp x)))))
                                                    var->range-constraints
                                                    logic-var+range-constraint)))))
    compiled-query))

(defn- build-sub-query [index-snapshot {:keys [query-cache] :as db} where in rule-name->rules stats]
  (let [encode-value-fn (partial db/encode-value index-snapshot)
        {:keys [depth->constraints
                vars-in-join-order
                var->range-constraints
                var->logic-var-range-constraint-fns
                var->joins
                var->bindings]
         :as compiled-query} (-> (lru/compute-if-absent
                                  query-cache
                                  [where in rule-name->rules]
                                  identity
                                  (fn [_]
                                    (compile-sub-query encode-value-fn where in rule-name->rules stats)))
                                 (add-logic-var-constraints))
        idx-id->idx (build-idx-id->idx db index-snapshot compiled-query)
        unary-join-indexes (for [v vars-in-join-order]
                             (-> (idx/new-unary-join-virtual-index
                                  (vec (for [{:keys [id idx-fn] :as join} (get var->joins v)]
                                         (or (get idx-id->idx id)
                                             (idx-fn db index-snapshot compiled-query)))))
                                 (idx/wrap-with-range-constraints (get var->range-constraints v))))
        constrain-result-fn (fn [join-keys]
                              (constrain-join-result-by-constraints index-snapshot db idx-id->idx depth->constraints join-keys))]
    (log/debug :where (cio/pr-edn-str where))
    (log/debug :vars-in-join-order vars-in-join-order)
    (log/debug :attr-stats (cio/pr-edn-str stats))
    (log/debug :var->bindings (cio/pr-edn-str var->bindings))
    {:n-ary-join (when (constrain-result-fn [])
                   (-> (idx/new-n-ary-join-layered-virtual-index unary-join-indexes)
                       (idx/new-n-ary-constraining-layered-virtual-index constrain-result-fn)))
     :var->bindings var->bindings}))

(defn- open-index-snapshot ^java.io.Closeable [{:keys [index-store index-snapshot] :as db}]
  (if index-snapshot
    (db/open-nested-index-snapshot index-snapshot)
    (db/open-index-snapshot index-store)))

(defn- with-entity-resolver-cache [entity-resolver-fn {:keys [entity-cache-size]}]
  (let [entity-cache (lru/new-cache entity-cache-size)]
    (fn [k]
      (lru/compute-if-absent entity-cache k mem/copy-to-unpooled-buffer entity-resolver-fn))))

(defn- new-entity-resolver-fn [{:keys [valid-time transact-time index-snapshot entity-cache?] :as db}]
  (cond-> #(db/entity-as-of-resolver index-snapshot % valid-time transact-time)
    entity-cache? (with-entity-resolver-cache db)))

(defn- validate-args [args]
  (let [ks (keys (first args))]
    (doseq [m args]
      (when-not (every? #(contains? m %) ks)
        (throw (IllegalArgumentException.
                (str "Argument maps need to contain the same keys as first map: " ks " " (keys m))))))))

(defn- validate-in [in]
  (doseq [binding (:bindings in)
          :let [binding-vars (find-binding-vars binding)]]
    (when-not (distinct-vars? binding-vars)
      (throw (IllegalArgumentException.
              (str "In binding variables not distinct: " (cio/pr-edn-str binding)))))))

;; NOTE: For ascending sort, it might be possible to pick the right
;; join order so the resulting seq is already sorted, by ensuring the
;; first vars of the join order overlap with the ones in order
;; by. Depending on the query this might not be possible. For example,
;; when using or-join/rules the order from the sub queries cannot be
;; guaranteed. The order by vars must be in the set of bound vars for
;; all or statements in the query for this to work. This is somewhat
;; related to embedding or in the main query. Also, this sort is based
;; on the actual values, and not the byte arrays, which would give
;; different sort order for example for ids, where the hash used in
;; the indexes won't sort the same as the actual value. For this to
;; work well this would need to be revisited.
(defn- order-by-comparator [find order-by]
  (let [find-arg->index (zipmap find (range))]
    (reify Comparator
      (compare [_ a b]
        (loop [diff 0
               [{:keys [find-arg direction]} & order-by] order-by]
          (if (or (not (zero? diff))
                  (nil? find-arg))
            diff
            (let [index (get find-arg->index find-arg)]
              (recur (long (cond-> (compare (get a index)
                                            (get b index))
                             (= :desc direction) -))
                     order-by))))))))

(defn- replace-docs [v docs]
  (if (not-empty (::hashes (meta v)))
    (v docs)
    v))

(defn- lookup-docs [v {:keys [document-store]}]
  (when-let [hashes (not-empty (::hashes (meta v)))]
    (db/fetch-docs document-store hashes)))

(defmacro ^:private let-docs [[binding hashes] & body]
  `(-> (fn ~'let-docs [~binding]
         ~@body)
       (with-meta {::hashes ~hashes})))

(defn- after-doc-lookup [f lookup]
  (if-let [hashes (::hashes (meta lookup))]
    (let-docs [docs hashes]
      (let [res (replace-docs lookup docs)]
        (if (::hashes (meta res))
          (after-doc-lookup f res)
          (f res))))
    (f lookup)))

(defn- raise-doc-lookup-out-of-coll
  "turns a vector/set where each of the values could be doc lookups into a single doc lookup returning a vector/set"
  [coll]
  (if-let [hashes (not-empty (into #{} (mapcat (comp ::hashes meta)) coll))]
    (let-docs [docs hashes]
      (->> coll
           (into (empty coll) (map #(replace-docs % docs)))
           raise-doc-lookup-out-of-coll))
    coll))

(defn- project-child [v child-fns db]
  (->> (mapv (fn [f]
               (f v db))
             child-fns)
       (raise-doc-lookup-out-of-coll)
       (after-doc-lookup (fn [res]
                           (into {} (mapcat identity) res)))))

(defn- project-child-fns [{:keys [children] :as project-spec}]
  (let [{special :special, props :prop, joins :join} (->> (:children project-spec)
                                                          (group-by (some-fn :type (constantly :special))))
        {forward-joins false, reverse-joins true}
        (group-by (comp #(string/starts-with? % "_") name :dispatch-key) joins)]

    (into [(let [join-child-fns (into {} (map (juxt :dispatch-key project-child-fns)) forward-joins)
                 prop-dispatch-keys (into #{} (map :dispatch-key) props)
                 special-dispatch-keys (into #{} (map :dispatch-key) special)]
             (fn [value {:keys [document-store index-snapshot entity-resolver-fn] :as db}]
               (when-not (and (empty? prop-dispatch-keys)
                              (empty? special-dispatch-keys)
                              (empty? join-child-fns))
                 (let [content-hash (entity-resolver-fn (c/->id-buffer value))]
                   (let-docs [docs #{content-hash}]
                     (let [doc (get docs (c/new-id content-hash))]
                       (->> (mapv (fn [[dispatch-key child-fns]]
                                    (let [v (get doc dispatch-key)]
                                      (->> (if (or (vector? v) (set? v))
                                             (->> (into [] (map #(project-child % child-fns db)) v)
                                                  (raise-doc-lookup-out-of-coll))
                                             (project-child v child-fns db))
                                           (after-doc-lookup (fn [res]
                                                               (MapEntry/create dispatch-key res))))))
                                  join-child-fns)
                            (raise-doc-lookup-out-of-coll)
                            (after-doc-lookup (fn [res]
                                                (into (if (contains? special-dispatch-keys '*)
                                                        doc
                                                        (select-keys doc prop-dispatch-keys))
                                                      res))))))))))]

          (for [{:keys [dispatch-key] :as join} reverse-joins]
            (let [child-fns (project-child-fns join)
                  forward-key (keyword (namespace dispatch-key)
                                       (subs (name dispatch-key) 1))
                  one? (= :one (get-in join [:params :crux/cardinality]))]
              (fn [value {:keys [document-store index-snapshot entity-resolver-fn] :as db}]
                (->> (vec (for [v (cond->> (db/ave index-snapshot (c/->id-buffer forward-key) (c/->value-buffer value) nil entity-resolver-fn)
                                    one? (take 1)
                                    :always vec)]
                            (project-child (db/decode-value index-snapshot v) child-fns db)))
                     (raise-doc-lookup-out-of-coll)
                     (after-doc-lookup (fn [res]
                                         [(MapEntry/create dispatch-key
                                                           (cond->> res
                                                             one? first))])))))))))

(defn- compile-project-spec [project-spec]
  (let [root-fns (project-child-fns (eql/query->ast project-spec))]
    (fn [value db]
      (project-child value root-fns db))))

(defn- compile-find [conformed-find {:keys [var->bindings full-results?]} {:keys [projection-cache]}]
  (for [[var-type arg] conformed-find]
    (case var-type
      :logic-var {:logic-var arg
                  :var-type :logic-var
                  :var-binding (var->bindings arg)
                  :->result (if full-results?
                              (fn [value {:keys [entity-resolver-fn]}]
                                (or (when-let [hash (some-> (entity-resolver-fn (c/->id-buffer value)) (c/new-id))]
                                      (let-docs [docs #{hash}]
                                        (get docs (c/new-id hash))))
                                    value))
                              (fn [value _]
                                value))}
      :project {:logic-var (:logic-var arg)
                :var-type :project
                :var-binding (var->bindings (:logic-var arg))
                :->result (lru/compute-if-absent projection-cache (:project-spec arg)
                                                 identity
                                                 (fn [spec]
                                                   (compile-project-spec (s/unform ::eql/query spec))))}
      :aggregate (do (s/assert ::aggregate-args [(:aggregate-fn arg) (vec (:args arg))])
                     {:logic-var (:logic-var arg)
                      :var-type :aggregate
                      :var-binding (var->bindings (:logic-var arg))
                      :aggregate-fn (apply aggregate (:aggregate-fn arg) (:args arg))
                      :->result (fn [value _]
                                  value)}))))

(defn- aggregate-result [compiled-find result]
  (let [indexed-compiled-find (map-indexed vector compiled-find)
        grouping-var-idxs (vec (for [[n {:keys [var-type]}] indexed-compiled-find
                                     :when (not= :aggregate var-type)]
                                 n))
        idx->aggregate (->> (for [[n {:keys [aggregate-fn]}] indexed-compiled-find
                                  :when aggregate-fn]
                              [n aggregate-fn])
                            (into {}))
        groups (persistent!
                (reduce
                 (fn [acc tuple]
                   (let [group (mapv tuple grouping-var-idxs)
                         group-acc (or (get acc group)
                                       (reduce-kv
                                        (fn [acc n aggregate-fn]
                                          (assoc acc n (aggregate-fn)))
                                        tuple
                                        idx->aggregate))]
                     (assoc! acc group (reduce-kv
                                        (fn [acc n aggregate-fn]
                                          (update acc n #(aggregate-fn % (get tuple n))))
                                        group-acc
                                        idx->aggregate))))
                 (transient {})
                 result))]
    (for [[_ group-acc] groups]
      (reduce-kv
       (fn [acc n aggregate-fn]
         (update acc n aggregate-fn))
       group-acc
       idx->aggregate))))

(defn- arg-for-var [arg var]
  (second
   (or (find arg (symbol (name var)))
       (find arg (keyword (name var))))))

(defn- find-arg-vars [args]
  (let [ks (keys (first args))]
    (set (for [k ks]
           (symbol (name k))))))

(defn- add-legacy-args [{:keys [args in] :as query} in-args]
  (if-let [arg-vars (not-empty (find-arg-vars args))]
    (let [arg-vars (vec arg-vars)]
      [(update in :bindings #(vec (cons [:relation [arg-vars]] %)))
       (vec (cons (vec (for [arg-tuple args]
                         (mapv #(arg-for-var arg-tuple %) arg-vars)))
                  in-args))])
    [in in-args]))

(defn query-plan-for [q encode-value-fn stats]
  (s/assert ::query q)
  (let [{:keys [where in rules]} (s/conform ::query q)
        [in in-args] (add-legacy-args q [])]
    (compile-sub-query encode-value-fn where in (rule-name->rules rules) stats)))

(defn query [{:keys [valid-time transact-time document-store index-store index-snapshot] :as db} ^ConformedQuery conformed-q in-args]
  (let [q (.q-normalized conformed-q)
        q-conformed (.q-conformed conformed-q)
        {:keys [find where in rules offset limit order-by full-results?]} q-conformed
        stats (db/read-index-meta index-store :crux/attribute-stats)
        [in in-args] (add-legacy-args q-conformed in-args)]
    (log/debug :query (cio/pr-edn-str (-> q
                                          (assoc :in in)
                                          (dissoc :args))))
    (validate-in in)
    (let [rule-name->rules (with-meta (rule-name->rules rules) {:rules (:rules q)})
          db (assoc db :index-snapshot index-snapshot :in-args in-args)
          entity-resolver-fn (or (:entity-resolver-fn db)
                                 (new-entity-resolver-fn db))
          open-nested-index-snapshot-fn (memoize
                                         (fn [id]
                                           (db/open-nested-index-snapshot index-snapshot)))
          db (assoc db :entity-resolver-fn entity-resolver-fn :open-nested-index-snapshot-fn open-nested-index-snapshot-fn)
          {:keys [n-ary-join var->bindings] :as built-query} (build-sub-query index-snapshot db where in rule-name->rules stats)
          compiled-find (compile-find find (assoc built-query :full-results? full-results?) db)
          find-logic-vars (mapv :logic-var compiled-find)
          var-types (set (map :var-type compiled-find))
          aggregate? (contains? var-types :aggregate)
          project? (or (contains? var-types :project) full-results?)
          var-bindings (mapv :var-binding compiled-find)
          ->result-fns (mapv :->result compiled-find)]
      (doseq [{:keys [logic-var var-binding]} compiled-find
              :when (nil? var-binding)]
        (throw (IllegalArgumentException.
                (str "Find refers to unknown variable: " logic-var))))
      (doseq [{:keys [find-arg]} order-by
              :when (not (some #{find-arg} find))]
        (throw (IllegalArgumentException.
                (str "Order by requires an element from :find. unreturned element: " find-arg))))

      (lazy-seq
       (cond->> (for [join-keys (idx/layered-idx->seq n-ary-join)]
                  (mapv (fn [var-binding]
                          (bound-result-for-var index-snapshot var-binding join-keys))
                        var-bindings))

         aggregate? (aggregate-result compiled-find)
         order-by (cio/external-sort (order-by-comparator find order-by))
         offset (drop offset)
         limit (take limit)
         project? (map (fn [row]
                         (mapv (fn [value ->result]
                                 (->result value db))
                               row ->result-fns)))
         project? (partition-all (or (:batch-size q-conformed)
                                     (:batch-size db)
                                     100))
         project? (map (fn [results]
                         (->> results
                              (mapv raise-doc-lookup-out-of-coll)
                              raise-doc-lookup-out-of-coll)))
         project? (mapcat (fn [lookup]
                            (if (::hashes (meta lookup))
                              (recur (replace-docs lookup (lookup-docs lookup db)))
                              lookup))))))))

(defn entity-tx [{:keys [valid-time transact-time] :as db} index-snapshot eid]
  (some-> (db/entity-as-of index-snapshot eid valid-time transact-time)
          (c/entity-tx->edn)))

(defn- entity [{:keys [document-store] :as db} index-snapshot eid]
  (when-let [content-hash (some-> (entity-tx db index-snapshot eid)
                                  :crux.db/content-hash)]
    (-> (db/fetch-docs document-store #{content-hash})
        (get content-hash)
        (c/keep-non-evicted-doc))))

(defrecord QueryDatasource [document-store index-store bus tx-ingester
                            ^Date valid-time ^Date transact-time
                            ^ScheduledExecutorService interrupt-executor
                            conform-cache query-cache projection-cache
                            index-snapshot
                            entity-resolver-fn]
  Closeable
  (close [_]
    (when index-snapshot
      (.close ^Closeable index-snapshot)))

  ICruxDatasource
  (entity [this eid]
    (with-open [index-snapshot (open-index-snapshot this)]
      (entity this index-snapshot eid)))

  (entityTx [this eid]
    (with-open [index-snapshot (open-index-snapshot this)]
      (entity-tx this index-snapshot eid)))

  (query [this query args]
    (with-open [res (.openQuery this query args)]
      (let [result-coll-fn (if (some (normalize-query query) [:order-by :limit :offset]) vec set)
            ^Future
            interrupt-job (when-let [timeout-ms (get query :timeout (:query-timeout this))]
                            (let [caller-thread (Thread/currentThread)]
                              (.schedule interrupt-executor
                                         ^Runnable
                                         (fn []
                                           (.interrupt caller-thread))
                                         ^long timeout-ms
                                         TimeUnit/MILLISECONDS)))]
        (try
          (result-coll-fn (iterator-seq res))
          (finally
            (when interrupt-job
              (.cancel interrupt-job false)))))))

  (openQuery [db query args]
    (let [index-snapshot (open-index-snapshot db)
          db (assoc db :index-snapshot index-snapshot)
          entity-resolver-fn (or entity-resolver-fn (new-entity-resolver-fn db))
          db (assoc db :entity-resolver-fn entity-resolver-fn)

          conformed-query (normalize-and-conform-query conform-cache query)
          query-id (str (UUID/randomUUID))
          safe-query (-> conformed-query .q-normalized (dissoc :args))]

      (when bus
        (bus/send bus {:crux/event-type ::submitted-query
                       ::query safe-query
                       ::query-id query-id}))
      (->> (try
             (crux.query/query db conformed-query args)
             (catch Exception e
               (when bus
                 (bus/send bus {:crux/event-type ::failed-query
                                ::query safe-query
                                ::query-id query-id
                                ::error {:type (cio/pr-edn-str (type e))
                                         :message (.getMessage e)}}))
               (throw e)))
           (cio/->cursor (fn []
                           (cio/try-close index-snapshot)
                           (when bus
                             (bus/send bus {:crux/event-type ::completed-query
                                            ::query safe-query
                                            ::query-id query-id})))))))

  (entityHistory [this eid opts]
    (with-open [history (.openEntityHistory this eid opts)]
      (into [] (iterator-seq history))))

  (openEntityHistory [this eid opts]
    (letfn [(inc-date [^Date d] (Date. (inc (.getTime d))))
            (with-upper-bound [^Date d, ^Date upper-bound]
              (if (and d (neg? (compare d upper-bound))) d upper-bound))]
      (let [sort-order (condp = (.sortOrder opts)
                         HistoryOptions$SortOrder/ASC :asc
                         HistoryOptions$SortOrder/DESC :desc)
            with-docs? (.withDocs opts)
            index-snapshot (db/open-index-snapshot index-store)

            opts (cond-> {:with-corrections? (.withCorrections opts)
                          :with-docs? with-docs?
                          :start {:crux.db/valid-time (.startValidTime opts)
                                  :crux.tx/tx-time (.startTransactionTime opts)}
                          :end {:crux.db/valid-time (.endValidTime opts)
                                :crux.tx/tx-time (.endTransactionTime opts)}}
                   (= sort-order :asc)
                   (-> (update-in [:end :crux.db/valid-time]
                                  with-upper-bound (inc-date valid-time))
                       (update-in [:end :crux.tx/tx-time]
                                  with-upper-bound (inc-date transact-time)))

                   (= sort-order :desc)
                   (-> (update-in [:start :crux.db/valid-time]
                                  with-upper-bound valid-time)
                       (update-in [:start :crux.tx/tx-time]
                                  with-upper-bound transact-time)))]

        (cio/->cursor #(.close index-snapshot)
                      (->> (db/entity-history index-snapshot eid sort-order opts)
                           (map (fn [^EntityTx etx]
                                  (cond-> {:crux.tx/tx-time (.tt etx)
                                           :crux.tx/tx-id (.tx-id etx)
                                           :crux.db/valid-time (.vt etx)
                                           :crux.db/content-hash (.content-hash etx)}
                                    with-docs? (assoc :crux.db/doc (-> (db/fetch-docs document-store #{(.content-hash etx)})
                                                                       (get (.content-hash etx))))))))))))

  (validTime [_] valid-time)
  (transactionTime [_] transact-time)

  (withTx [this tx-ops]
    (let [tx (merge {:fork-at {::db/valid-time valid-time
                               ::tx/tx-time transact-time}}
                    (if-let [latest-completed-tx (db/latest-completed-tx index-store)]
                      {::tx/tx-id (inc (long (::tx/tx-id latest-completed-tx)))
                       ::tx/tx-time (Date. (max (System/currentTimeMillis)
                                                (inc (.getTime ^Date (::tx/tx-time latest-completed-tx)))))
                       ::db/valid-time valid-time}
                      {::tx/tx-time (Date.)
                       ::tx/tx-id 0}))
          conformed-tx-ops (map txc/conform-tx-op tx-ops)
          in-flight-tx (db/begin-tx tx-ingester tx)]

      (db/submit-docs in-flight-tx (into {} (mapcat :docs) conformed-tx-ops))

      (when (db/index-tx-events in-flight-tx (map txc/->tx-event conformed-tx-ops))
        (api/db in-flight-tx valid-time)))))

(defmethod print-method QueryDatasource [{:keys [valid-time transact-time]} ^Writer w]
  (.write w (format "#<CruxDB %s>" (cio/pr-edn-str {:crux.db/valid-time valid-time, :crux.tx/tx-time transact-time}))))

(defrecord QueryEngine [^ScheduledExecutorService interrupt-executor document-store
                        index-store bus
                        query-cache conform-cache projection-cache]
  api/DBProvider
  (db [this] (api/db this nil nil))
  (db [this valid-time] (api/db this valid-time nil))
  (db [this valid-time tx-time]
    (let [latest-tx-time (:crux.tx/tx-time (db/latest-completed-tx index-store))
          _ (when (and tx-time (or (nil? latest-tx-time) (pos? (compare tx-time latest-tx-time))))
              (throw (NodeOutOfSyncException. (format "node hasn't indexed the requested transaction: requested: %s, available: %s"
                                                      tx-time latest-tx-time)
                                              tx-time latest-tx-time)))
          valid-time (or valid-time (cio/next-monotonic-date))
          tx-time (or tx-time latest-tx-time valid-time)]

      ;; we create a new tx-ingester mainly so that it doesn't share state with the main one (!error)
      ;; we couldn't have QueryEngine depend on the main one anyway, because of a cyclic dependency
      (map->QueryDatasource (assoc this
                                   :tx-ingester (tx/->tx-ingester {:index-store index-store
                                                                   :document-store document-store
                                                                   :bus bus
                                                                   :query-engine this})
                                   :valid-time valid-time
                                   :transact-time tx-time))))

  (open-db [this] (api/open-db this nil nil))
  (open-db [this valid-time] (api/open-db this valid-time nil))
  (open-db [this valid-time tx-time]
    (let [db (api/db this valid-time tx-time)
          index-snapshot (open-index-snapshot db)
          db (assoc db :index-snapshot index-snapshot)
          entity-resolver-fn (new-entity-resolver-fn db)]
      (assoc db :entity-resolver-fn entity-resolver-fn)))

  Closeable
  (close [_]
    (when interrupt-executor
      (doto interrupt-executor
        (.shutdown)
        (.awaitTermination 60000 TimeUnit/MILLISECONDS)))))

(defn ->query-engine {::sys/deps {:index-store :crux/index-store
                                  :bus :crux/bus
                                  :document-store :crux/document-store}
                      ::sys/args {:query-cache-size {:doc "Compiled Query Cache Size"
                                                     :default 10240
                                                     :spec ::sys/nat-int}
                                  :conform-cache-size {:doc "Conformed Query Cache Size"
                                                       :default 10240
                                                       :spec ::sys/nat-int}
                                  :projection-cache-size {:doc "Projection Cache Size"
                                                          :default 10240
                                                          :spec ::sys/nat-int}
                                  :entity-cache? {:doc "Enable Query Entity Cache"
                                                  :default true
                                                  :spec ::sys/boolean}
                                  :entity-cache-size {:doc "Query Entity Cache Size"
                                                      :default 10000
                                                      :spec ::sys/nat-int}
                                  :query-timeout {:doc "Query Timeout ms"
                                                  :default 30000
                                                  :spec ::sys/nat-int}
                                  :batch-size {:doc "Batch size of results"
                                               :default 100
                                               :required? true
                                               :spec ::sys/pos-int}}}
  [{:keys [query-cache-size conform-cache-size projection-cache-size] :as opts}]
  (map->QueryEngine (merge opts {:conform-cache (lru/new-cache conform-cache-size)
                                 :query-cache (lru/new-cache query-cache-size)
                                 :projection-cache (lru/new-cache projection-cache-size)
                                 :interrupt-executor (Executors/newSingleThreadScheduledExecutor (cio/thread-factory "crux-query-interrupter"))})))
