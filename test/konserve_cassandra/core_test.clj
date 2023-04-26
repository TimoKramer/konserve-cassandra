(ns konserve-cassandra.core-test
  (:require [clojure.test :refer :all]
            [konserve.compliance-test :refer [compliance-test]]
            [konserve-cassandra.core :refer [delete-store connect-store release]]))

(def cassandra-config {:session-keyspace "alia"
                       :contact-points ["127.0.0.1"]})

(deftest cassandra-compliance-test
  (let [_ (delete-store cassandra-config :table "compliance_test")
        store (connect-store cassandra-config :table "compliance_test")]
    (compliance-test store)
    (release store)
    (delete-store store :table "compliance_test")))
