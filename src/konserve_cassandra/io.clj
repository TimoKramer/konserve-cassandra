(ns konserve-cassandra.io
  (:require [qbits.alia :as qa]
            [qbits.hayt :as qh])
  (:import  [java.io ByteArrayInputStream ByteArrayOutputStream]))

(defn split-header [byte-buffer]
  (when (some? byte-buffer)
    (let [data  (->> byte-buffer
                     .array
                     vec
                     (split-at 4))
          streamer (fn [header data] (list (byte-array header) (-> data byte-array (ByteArrayInputStream.))))]
      (apply streamer data))))

(defn cluster [cluster-config]
  (qa/cluster cluster-config))

(defn connect [cluster keyspace]
  (qa/connect cluster keyspace))

(defn exists?
  "Check if the Cassandra connection has any tables in its keyspace

   Arguments: conn, id"
  [{:keys [session table]} id]
  (-> (qa/execute session
                  (qh/select table
                             (qh/where {:id id}))
                  {:result-set-fn #(first %)})
      empty?
      not))

(defn get-both [{:keys [session table]} id]
  (let [{:keys [data meta id]} (qa/execute session
                                           (qh/select table
                                             (qh/where {:id id}))
                                           {:result-set-fn #(first %)})]
    (if (and meta data)
      [(split-header meta) (split-header data)]
      [nil nil])))

(defn get-data [{:keys [session table]} id]
  (let [{:keys [data]} (qa/execute session
                                   (qh/select table
                                              (qh/columns :data)
                                              (qh/where {:id id}))
                                   {:result-set-fn #(first %)})]
    (when data (split-header data))))

(defn get-meta [{:keys [session table]} id]
  (let [{:keys [meta]} (qa/execute session
                                   (qh/select table
                                              (qh/columns :meta)
                                              (qh/where {:id id}))
                                   {:result-set-fn #(first %)})]
    (when meta (split-header meta))))

(defn update-both [{:keys [session table]} id data]
  (qa/execute session
              (qh/update table
                         (qh/set-columns {:meta (first data)
                                          :data (second data)})
                         (qh/where {:id id}))))

(defn insert-both [{:keys [session table]} id data]
  (qa/execute session
              (qh/insert table
                         (qh/values {:id id
                                     :meta (first data)
                                     :data (second data)}))))

(defn delete-entry [{:keys [session table]} id]
  (qa/execute session
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
  (qa/execute session
              (qh/drop-table table)))

(comment
  (require '[konserve-cassandra.core :as core])
  (require '[clojure.core.async :refer [<!!]])
  (def config {:cluster {:session-keyspace "alia"
                         :contact-points ["127.0.0.1"]}})

  (def cluster (cluster (:cluster config)))
  (def conn (qa/connect cluster))
  (qa/execute conn (qh/create-keyspace :alia (qh/with {:replication {:class "SimpleStrategy" :replication_factor 1}})))

  (def store (<!! (core/new-cassandra-store config)))

  (.getLoggedKeyspace (:session (:conn store)))

  (create-table conn)
  (<!! (exists? conn "1"))
  (drop-table conn)

  (require '[konserve-cassandra.core :as core])
  (def ^ByteArrayOutputStream test-meta (ByteArrayOutputStream.))
  (.write test-meta 1)
  (def ^ByteArrayOutputStream test-data (ByteArrayOutputStream.))
  (.write test-data 3)
  (update conn (core/str-uuid [:foo :bar]) [(.toByteArray test-meta) (.toByteArray test-data)])
  (def foo (qa/execute (:session conn)
                       (qh/select :foo)))

  (get-data (:conn store) "06cee0d3-72f7-5bfd-ae3b-af7c2f0fef8f")
  (def data (:data (qa/execute (:session (:conn store))
                               (qh/select (:table (:conn store))
                                          (qh/columns :id :data)
                                          (qh/where {:id "06cee0d3-72f7-5bfd-ae3b-af7c2f0fef8f"}))
                               {:result-set-fn #(first %)})))
  (split-header data))
