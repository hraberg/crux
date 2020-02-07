(ns crux.rocksdb-microbench
  (:require [crux.bench.ts-weather :as ts-weather]
            [clojure.test :as t]
            [crux.api :as api]
            [crux.db :as db]
            [crux.codec :as c]
            [crux.kv :as kv]
            [crux.fixtures :as f]
            [crux.bench :as bench]
            [clojure.java.io :as io]))

(defn with-rocksdb-node* [f]
  (f/with-tmp-dir "dev-storage" [data-dir]
    (with-open [node (api/start-node {:crux.node/topology '[crux.standalone/topology]
                                      :crux.node/kv-store 'crux.kv.rocksdb/kv
                                      :crux.kv/db-dir (str (io/file data-dir "db-dir-1"))
                                      :crux.standalone/event-log-dir (str (io/file data-dir "eventlog-1"))})]
      (f node))))

(defmacro with-rocksdb-node [[node-binding] & body]
  `(with-rocksdb-node* (fn [~node-binding] ~@body)))

(t/deftest test-weather-ingest
  (t/is :test-weather-ingest)

  (when ts-weather/conditions-csv-resource
    (with-rocksdb-node [node]
      (bench/with-bench-ns :rocksdb-microbench
        (ts-weather/with-condition-docs
          (fn [[first-doc :as condition-docs]]
            (time
             (doseq [doc-batch (->> (take 10000 condition-docs)
                                    (partition-all 100))]
               (db/index-docs (:indexer node) (->> doc-batch (into {} (map (juxt c/new-id identity)))))))

            (with-open [snapshot (kv/new-snapshot (:kv-store node))]
              (t/is (= first-doc (db/get-single-object (:object-store node) snapshot (c/new-id first-doc)))))))))))
