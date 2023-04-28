(ns konserve-cassandra.core-test
  (:require [clojure.test :refer [deftest are is]]
            [clojure.core.async :refer [<!!]]
            [konserve.compliance-test :refer [compliance-test]]
            [konserve-cassandra.core :as sut :refer [delete-store connect-store release]])
  (:import [com.datastax.oss.driver.api.core.servererrors InvalidQueryException]
           [clojure.lang ExceptionInfo]))

(def cassandra-config {:session-keyspace "konserve_test"})

(deftest ^:integration cassandra-compliance-sync-test
  (let [_ (try (delete-store cassandra-config :table "compliance_sync_test" {:sync? true})
               (catch ExceptionInfo _e))
        store (connect-store cassandra-config :table "compliance_sync_test" {:sync? true})]
    (compliance-test store)
    (release store {:sync? true})
    (delete-store cassandra-config :table "compliance_sync_test" {:sync? true})))

(deftest ^:integration cassandra-compliance-async-test
  (let [_ (<!! (delete-store cassandra-config :table "compliance_async_test" :opts {:sync? false}))
        store (<!! (connect-store cassandra-config :table "compliance_async_test" :opts {:sync? false}))]
    (compliance-test store)
    (release store {:sync? false})
    (delete-store cassandra-config :table "compliance_async_test" {:sync? false})))
