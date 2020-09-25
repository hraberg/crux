(ns crux.index-test
  (:require [clojure.test :as t]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.fixtures :as f]
            [crux.index :as idx])
  (:import clojure.lang.Box))

(t/deftest test-can-perform-unary-join
  (let [a-idx (idx/new-sorted-virtual-index [0
                                             1
                                             3
                                             4
                                             5
                                             6
                                             7
                                             8
                                             9
                                             11
                                             12]
                                            c/->value-buffer)
        b-idx (idx/new-sorted-virtual-index [0
                                             2
                                             6
                                             7
                                             8
                                             9
                                             12]
                                            c/->value-buffer)
        c-idx (idx/new-sorted-virtual-index [2
                                             4
                                             5
                                             8
                                             10
                                             12]
                                            c/->value-buffer)]

    (t/is (= [8
              12]
             (->> (idx/new-unary-join-virtual-index [a-idx b-idx c-idx])
                  (idx/idx->seq)
                  (map c/decode-value-buffer))))))

(t/deftest test-range-predicates
  (let [r (idx/new-sorted-virtual-index [1
                                         2
                                         3
                                         4
                                         5]
                                        c/->value-buffer)]

    (t/is (= [1 2 3 4 5]
             (->> (idx/idx->seq r)
                  (map c/decode-value-buffer))))

    (t/is (= [1 2 3]
             (->> (idx/idx->seq (idx/new-less-than-virtual-index r (Box. (c/->value-buffer 4))))
                  (map c/decode-value-buffer))))

    (t/is (= [1 2 3 4]
             (->> (idx/idx->seq (idx/new-less-than-equal-virtual-index r (Box. (c/->value-buffer 4))))
                  (map c/decode-value-buffer))))

    (t/is (= [3 4 5]
             (->> (idx/idx->seq (idx/new-greater-than-virtual-index r (Box. (c/->value-buffer 2))))
                  (map c/decode-value-buffer))))

    (t/is (= [2 3 4 5]
             (->> (idx/idx->seq (idx/new-greater-than-equal-virtual-index r (Box. (c/->value-buffer 2))))
                  (map c/decode-value-buffer))))

    (t/is (= [2]
             (->> (idx/idx->seq (idx/new-equals-virtual-index r (Box. (c/->value-buffer 2))))
                  (map c/decode-value-buffer))))

    (t/is (empty? (idx/idx->seq (idx/new-equals-virtual-index r (Box. (c/->value-buffer 0))))))
    (t/is (empty? (idx/idx->seq (idx/new-equals-virtual-index r (Box. (c/->value-buffer 6))))))

    (t/testing "seek skips to lower range"
      (t/is (= 2 (c/decode-value-buffer (db/seek-values (idx/new-greater-than-equal-virtual-index r (Box. (c/->value-buffer 2))) (c/->value-buffer nil)))))
      (t/is (= 3 (c/decode-value-buffer (db/seek-values (idx/new-greater-than-virtual-index r (Box. (c/->value-buffer 2))) (c/->value-buffer 1))))))

    (t/testing "combining indexes"
      (t/is (= [2 3 4]
               (->> (idx/idx->seq (-> r
                                      (idx/new-greater-than-equal-virtual-index (Box. (c/->value-buffer 2)))
                                      (idx/new-less-than-virtual-index (Box. (c/->value-buffer 5)))))
                    (map c/decode-value-buffer)))))

    (t/testing "incompatible type"
      (t/is (empty? (->> (idx/idx->seq (-> (idx/new-greater-than-equal-virtual-index r (Box. (c/->value-buffer "foo")))))
                         (map c/decode-value-buffer)))))))
