(ns crux.query.parser
  (:require [clojure.spec.alpha :as s]
            [edn-query-language.core :as eql]
            [clojure.string :as str]))

(s/def ::query (s/and (s/conformer identity vec)
                      (s/cat :find-spec ::find-spec
                             :return-map (s/alt :return-map ::return-map :none (s/cat))
                             :with-clause (s/alt :with-clause ::with-clause :none (s/cat))
                             :inputs (s/alt :inputs ::inputs :none (s/cat))
                             :where-clauses (s/alt :where-clauses ::where-clauses :none (s/cat)))))

(s/def ::find-spec (s/cat :find #{:find}
                          :find-spec (s/alt :find-rel ::find-rel
                                            :find-coll ::find-coll
                                            :find-tuple ::find-tuple
                                            :find-scalar ::find-scalar)))

(s/def ::return-map (s/alt :return-keys ::return-keys
                           :return-syms ::return-syms
                           :return-strs ::return-strs))

(s/def ::find-rel (s/+ ::find-elem))
(s/def ::find-coll (s/tuple ::find-elem '#{...}))
(s/def ::find-scalar (s/cat :find-elem ::find-elem :period '#{.}))
(s/def ::find-tuple (s/coll-of ::find-elem :min-count 1))

(s/def ::find-elem (s/or :variable ::variable :pull-expr ::pull-expr :aggregate ::aggregate))

(s/def ::return-keys (s/cat :keys #{:keys} :symbols (s/+ symbol?)))
(s/def ::return-syms (s/cat :syms #{:syms} :symbols (s/+ symbol?)))
(s/def ::return-strs (s/cat :strs #{:strs} :symbols (s/+ symbol?)))

(s/def ::pull-expr (s/cat :pull '#{pull} :variable ::variable :pattern ::pattern))
(s/def ::pattern (s/or :pattern-name ::pattern-name :pattern-data-literal ::pattern-data-literal))
(s/def ::pattern-data-literal ::eql/query)
(s/def ::aggregate (s/cat :aggregate-fn-name ::plain-symbol :fn-args (s/+ ::fn-arg)))
(s/def ::fn-arg (s/or :variable ::variable :constant ::constant :src-var ::src-var))

(s/def ::with-clause (s/cat :with #{:with} :variables (s/+ ::variable)))
(s/def ::where-clauses (s/cat :where #{:where} :clauses (s/+ ::clause)))

(s/def ::inputs (s/cat :in #{:in} :inputs (s/+ (s/or :src-var ::src-var :binding ::binding :pattern-name ::pattern-name :rules-var ::rules-var))))
(s/def ::src-var (s/and symbol? #(str/starts-with? (name %) "$")))
(s/def ::variable (s/and symbol? #(str/starts-with? (name %) "?")))
(s/def ::rules-var '#{%})

(s/def ::plain-symbol (s/and symbol? #(not (or (str/starts-with? (name %) "$")
                                               (str/starts-with? (name %) "?")))))

(s/def ::pattern-name ::plain-symbol)

(s/def ::and-clause (s/cat :and '#{and} :clauses (s/+ ::clause)))
(s/def ::expression-clause (s/or :data-pattern ::data-pattern
                                 :pred-expr ::pred-expr
                                 :fn-expr ::fn-expr
                                 :rule-expr ::rule-expr))

(s/def ::rule-expr (s/cat :src-var (s/? ::src-var)
                          :rule-name ::rule-name
                          :args (s/+ (s/or :variable ::variable
                                           :constant ::constant
                                           :blank-var '#{_}))))

(s/def ::not-clause (s/cat :src-var (s/? ::src-var)
                           :not '#{not}
                           :clauses (s/+ ::clause)))

(s/def ::not-join-clause (s/cat :src-var (s/? ::src-var)
                                :not-join '#{not-join}
                                :args (s/coll-of ::variable :kind vector? :min-count 1)
                                :clauses (s/+ ::clause)))

(s/def ::or-clause (s/cat :src-var (s/? ::src-var)
                          :or '#{or}
                          :clauses (s/+ (s/or :clause ::clause
                                              :and-clause ::and-clause))))

(s/def ::or-join-clause (s/cat :src-var (s/? ::src-var)
                               :or-join '#{or-join}
                               :args (s/tuple ::rule-vars)
                               :clauses (s/+ (s/or :clause ::clause
                                                   :and-clause ::and-clause))))

(s/def ::rule-vars  (s/and (s/conformer identity vec)
                           (s/cat :bound-vars (s/? (s/coll-of ::variable :kind vector? :min-count 1))
                                  :free-vars (s/* ::variable))))

(s/def ::clause (s/or :not-clause ::not-clause
                      :not-join-clause ::not-join-clause
                      :or-clause ::or-clause
                      :or-join-clause ::or-join-clause
                      :expression-clause ::expression-clause))

(s/def ::data-pattern (s/and (s/conformer identity vec)
                             (s/cat :src-var (s/? ::src-var)
                                    :pattern (s/+ (s/or :variable ::variable
                                                        :constant ::constant
                                                        :blank-var '#{_})))))

(s/def ::constant (s/and any? #(not (or (symbol? %) (list? %)))))

(s/def ::pred-expr (s/tuple (s/cat :pred ::pred
                                   :args (s/* ::fn-arg))))
(s/def ::pred ::plain-symbol)

(s/def ::fn-expr (s/tuple (s/cat :fn ::fn
                                 :args (s/* ::fn-arg))
                          ::binding))
(s/def ::fn ::plain-symbol)

(s/def ::binding (s/or :bind-scalar ::bind-scalar
                       :bind-tuple ::bind-tuple
                       :bind-coll ::bind-coll
                       :bind-rel ::bind-rel))

(s/def ::bind-scalar ::variable)
(s/def ::bind-tuple (s/coll-of (s/or :variable ::variable
                                     :blank-var '#{_})
                               :kind vector? :min-count 1))
(s/def ::bind-coll (s/tuple ::variable '#{...}))
(s/def ::bind-rel (s/tuple (s/coll-of (s/or :variable ::variable
                                            :blank-var '#{_})
                                      :kind vector? :min-count 1)))

(s/def ::rule (s/coll-of
               (s/and (s/conformer identity vec)
                      (s/cat :rule-head ::rule-head
                             :clauses (s/+ ::clause)))
               :kind vector?
               :min-count 1))
(s/def ::rule-head (s/and list? (s/cat :rule-name ::rule-name
                                       :rule-vars ::rule-vars)))
(s/def ::unqualified-plain-symbol (s/and ::plain-symbol #(and (not= 'and %)
                                                              (nil? (namespace %)))))
(s/def ::rule-name ::unqualified-plain-symbol)

;; query                      = [find-spec return-map-spec? with-clause? inputs? where-clauses?]
;; find-spec                  = ':find' (find-rel | find-coll | find-tuple | find-scalar)
;; return-map                 = (return-keys | return-syms | return-strs)
;; find-rel                   = find-elem+
;; find-coll                  = [find-elem '...']
;; find-scalar                = find-elem '.'
;; find-tuple                 = [find-elem+]
;; find-elem                  = (variable | pull-expr | aggregate)
;; return-keys                = ':keys' symbol+
;; return-syms                = ':syms' symbol+
;; return-strs                = ':strs' symbol+
;; pull-expr                  = ['pull' variable pattern]
;; pattern                    = (pattern-name | pattern-data-literal)
;; aggregate                  = [aggregate-fn-name fn-arg+]
;; fn-arg                     = (variable | constant | src-var)
;; with-clause                = ':with' variable+
;; where-clauses              = ':where' clause+
;; inputs                     = ':in' (src-var | binding | pattern-name | rules-var)+
;; src-var                    = symbol starting with "$"
;; variable                   = symbol starting with "?"
;; rules-var                  = the symbol "%"
;; plain-symbol               = symbol that does not begin with "$" or "?"
;; pattern-name               = plain-symbol
;; and-clause                 = [ 'and' clause+ ]
;; expression-clause          = (data-pattern | pred-expr | fn-expr | rule-expr)
;; rule-expr                  = [ src-var? rule-name (variable | constant | '_')+]
;; not-clause                 = [ src-var? 'not' clause+ ]
;; not-join-clause            = [ src-var? 'not-join' [variable+] clause+ ]
;; or-clause                  = [ src-var? 'or' (clause | and-clause)+]
;; or-join-clause             = [ src-var? 'or-join' rule-vars (clause | and-clause)+ ]
;; rule-vars                  = [variable+ | ([variable+] variable*)]
;; clause                     = (not-clause | not-join-clause | or-clause | or-join-clause | expression-clause)
;; data-pattern               = [ src-var? (variable | constant | '_')+ ]
;; constant                   = any non-variable data literal
;; pred-expr                  = [ [pred fn-arg+] ]
;; fn-expr                    = [ [fn fn-arg+] binding]
;; binding                    = (bind-scalar | bind-tuple | bind-coll | bind-rel)
;; bind-scalar                = variable
;; bind-tuple                 = [ (variable | '_')+]
;; bind-coll                  = [variable '...']
;; bind-rel                   = [ [(variable | '_')+] ]

;; rule                       = [ [rule-head clause+]+ ]
;; rule-head                  = [rule-name rule-vars]
;; rule-name                  = unqualified plain-symbol
