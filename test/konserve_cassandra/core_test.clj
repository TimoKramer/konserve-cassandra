(ns konserve-cassandra.core-test
  (:require [clojure.test :refer :all]
            [konserve.compliance-test :refer [compliance-test]]
            [konserve-cassandra.core :refer [delete-store connect-store release]]))

(def cassandra-config {:session-keyspace "alia"
                       :contact-points ["127.0.0.1"]})

(deftest cassandra-compliance-sync-test
  (let [_ (delete-store cassandra-config :table "compliance_test" {:sync? true})
        store (connect-store cassandra-config :table "compliance_test" {:sync? true})]
    (compliance-test store)
    (release store {:sync? true})
    (delete-store cassandra-config :table "compliance_test" {:sync? true})))

(deftest cassandra-compliance-async-test
  (let [_ (delete-store cassandra-config :table "compliance_test" :opts {:sync? false})
        store (connect-store cassandra-config :table "compliance_test" :opts {:sync? false})]
    (compliance-test store)
    (release store {:sync? false})
    (delete-store cassandra-config :table "compliance_test" {:sync? false})))

(comment
  (def store (connect-store cassandra-config :table "compliance_test"))
  (require '[clojure.core.async :refer [#?(:clj <!!) <! go]]
           '[konserve.core :as k]
           '[clojure.test :refer [are is testing]])
  (def opts {:sync? true})
  (k/append store :foolog {:bar 42} opts)
  (k/append store :foolog {:bar 43} opts)
  (is (= (k/log store :foolog opts)
         '({:bar 42}
           {:bar 43})))
  )
