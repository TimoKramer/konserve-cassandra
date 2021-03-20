(ns konserve-cassandra.io
  (:require [qbits.alia :as qa]
            [qbits.alia.async :as qasync]
            [qbits.hayt :as qh]
            [clojure.core.async :as async]
            [konserve-cassandra.core :as core])
  (:import  [java.io ByteArrayInputStream ByteArrayOutputStream]))

(defn split-header [bytes-or-blob]
  (when (some? bytes-or-blob)
    (let [data  (->> bytes-or-blob vec (split-at 4))
          streamer (fn [header data] (list (byte-array header) (-> data byte-array (ByteArrayInputStream.))))]
      (apply streamer data))))

(defn cluster [cluster-config]
  (qa/cluster cluster-config))

(defn connect [cluster keyspace]
  (qa/connect cluster keyspace))

(defn exists?
  "Check if the Cassandra connection has any tables in its keyspace

   Arguments: session, table, id"
  [{:keys [session table]} id]
  (qasync/execute-chan session
                       (qh/select table
                                  (qh/where {:id id}))))

(defn get-all [{:keys [session table]} id]
  (qasync/execute-chan session
                       (qh/select table
                         (qh/where {:id id}))
                       {:result-set-fn #(first %)}))

(defn get-data [{:keys [session table]} id]
  (qasync/execute-chan session
                       (qh/select table
                                  (qh/columns :id :data)
                                  (qh/where {:id id}))
                       {:result-set-fn #(first %)}))

(defn get-meta [{:keys [session table]} id]
  (qasync/execute-chan session
                       (qh/select table
                                  (qh/columns :id :meta)
                                  (qh/where {:id id}))
                       {:result-set-fn #(first %)}))

(defn update [{:keys [session table]} id data]
  (qa/execute session
              (qh/update table
                         (qh/set-columns {:meta (first data)
                                          :data (second data)})
                         (qh/where {:id id}))))

(defn insert [{:keys [session table]} id data]
  (qa/execute session
              (qh/insert table
                         (qh/values {:id id
                                     :meta (first data)
                                     :data (second data)}))))

(defn delete [{:keys [session table]} id]
  (qasync/execute-chan session
                       (qh/delete table
                                  (qh/where {:id id}))))

(defn create-table [{:keys [session table]}]
  (qa/execute session
              (qh/create-table table
                               (qh/if-not-exists)
                               (qh/column-definitions {:id :varchar
                                                       :meta :blob
                                                       :data :blob
                                                       :primary-key [:id]}))))

(defn drop-table [{:keys [session table]}]
  (qasync/execute-chan session
                       (qh/drop-table table)))

(comment
  (def cluster (qa/cluster {:session-keyspace "alia"
                            :contact-points ["127.0.0.1"]}))

  (def conn {:session (qa/connect cluster "alia")
             :table :foo})

  (.getLoggedKeyspace (:session conn))

  (create-table conn)
  (async/<!! (exists? conn "1"))
  (drop-table conn)

  (async/<!! (qasync/execute-chan (:session conn) "SELECT * FROM foo WHERE id = '1'"))

  (require '[konserve-cassandra.core :as core])
  (def ^ByteArrayOutputStream test-meta (ByteArrayOutputStream.))
  (.write test-meta 1)
  (def ^ByteArrayOutputStream test-data (ByteArrayOutputStream.))
  (.write test-data 3)
  (update conn (core/str-uuid [:foo :bar]) [(.toByteArray test-meta) (.toByteArray test-data)])
  (qa/execute (:session conn)
              (qh/select :foo))

  (:data (async/<!! (get-all conn "02d2b7ff-609b-50c4-9437-9c7eb6e4e98f")))
  (async/<!! (get-data conn "02d2b7ff-609b-50c4-9437-9c7eb6e4e98f"))
  (qa/execute (:session conn)
              (qh/select (:table conn)
                (qh/where {:id "02d2b7ff-609b-50c4-9437-9c7eb6e4e98f"}))
              {:result-set-fn #(first %)}))
