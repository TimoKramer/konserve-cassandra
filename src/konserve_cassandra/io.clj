(ns konserve-cassandra.io
  (:require [qbits.alia :as qa]
            [qbits.hayt :as qh])
  (:import  [java.io ByteArrayInputStream ByteArrayOutputStream]))

(defn split-header [byte-obj]
  (when (some? byte-obj)
    (let [data (->> byte-obj
                    vec
                    (split-at 4))
          streamer (fn [header data] (list (byte-array header) (-> data byte-array (ByteArrayInputStream.))))]
      (apply streamer data))))

(defn cluster [cluster-config]
  (qa/cluster cluster-config))

(defn connect
  ([cluster]
   (qa/connect cluster))
  ([cluster keyspace]
   (qa/connect cluster keyspace)))

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

(defn raw-get-both [{:keys [session table]} id]
  (let [{:keys [data meta]} (qa/execute session
                                        (qh/select table
                                          (qh/where {:id id}))
                                        {:result-set-fn #(first %)})]
    (when (and meta data)
      {:data (.array data)
       :meta (.array meta)})))

(defn get-both [conn id]
  (let [{:keys [data meta]} (raw-get-both conn id)]
    (if (and meta data)
      [(split-header meta) (split-header data)]
      [nil nil])))

(defn raw-get-data [{:keys [session table]} id]
  (let [{:keys [data]} (qa/execute session
                                   (qh/select table
                                              (qh/columns :data)
                                              (qh/where {:id id}))
                                   {:result-set-fn #(first %)})]
    (when data
      (.array data))))

(defn get-data [conn id]
  (let [data (raw-get-data conn id)]
    (when data (split-header data))))

(defn raw-get-meta [{:keys [session table]} id]
  (let [{:keys [meta]} (qa/execute session
                                   (qh/select table
                                              (qh/columns :meta)
                                              (qh/where {:id id}))
                                   {:result-set-fn #(first %)})]
    (when meta
      (.array meta))))

(defn get-meta [conn id]
  (let [meta (raw-get-meta conn id)]
    (when meta (split-header meta))))

(defn get-keys [{:keys [session table]}]
  (let [res' (qa/execute session
                         (qh/select table
                                    (qh/columns :id :meta)))]
    (doall (map #(-> %
                     :meta
                     (.array)
                     (split-header))
                res'))))

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

(defn raw-update-data [{:keys [session table]} id blob]
  (qa/execute session
              (qh/update table
                         (qh/set-columns {:data blob})
                         (qh/where {:id id}))))

(defn raw-update-meta [{:keys [session table]} id blob]
  (qa/execute session
              (qh/update table
                         (qh/set-columns {:meta blob})
                         (qh/where {:id id}))))

(defn raw-insert-data [{:keys [session table]} id blob]
  (qa/execute session
              (qh/insert table
                         (qh/values {:id id
                                     :data blob}))))

(defn raw-insert-meta [{:keys [session table]} id blob]
  (qa/execute session
              (qh/insert table
                         (qh/values {:id id
                                     :meta blob}))))

(defn delete-entry [{:keys [session table]} id]
  (qa/execute session
              (qh/delete table
                         (qh/where {:id id}))))

(defn create-keyspace
  "Takes a session object from connect fn"
  ([session]
   (create-keyspace session nil))
  ([session config]
   (let [session-keyspace (or (-> config :cluster :session-keyspace)
                              "konserve")
         keyspace-config (or (-> config :keyspace)
                             {:replication {:class "SimpleStrategy" :replication_factor 1}})]
     (qa/execute session
                 (qh/create-keyspace session-keyspace
                                     (qh/with keyspace-config))))))

(defn drop-keyspace
  "Takes a session object from connect fn"
  ([session]
   (drop-keyspace session nil))
  ([session config]
   (let [session-keyspace (or (-> config :cluster :session-keyspace)
                              "konserve")]
     (qa/execute session
                 (qh/drop-keyspace session-keyspace)))))

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

  (def mycluster (cluster (:cluster config)))
  (def session (qa/connect mycluster))

  (create-keyspace session config)
  (drop-keyspace session config)
  (qa/execute session
              (qh/select "system_schema.keyspaces"))

  (def store (<!! (core/new-store config)))
  (qa/shutdown (:session (:conn store)))

  (.getLoggedKeyspace (:session (:conn store)))

  (create-table (:conn store))
  (exists? (:conn store) "1")
  (drop-table (:conn store))

  (def ^ByteArrayOutputStream test-meta (ByteArrayOutputStream.))
  (.write test-meta 1)
  (def ^ByteArrayOutputStream test-data (ByteArrayOutputStream.))
  (.write test-data 3)
  (update-both (:conn store) (core/str-uuid [:foo :bar]) [(.toByteArray test-meta) (.toByteArray test-data)])
  (get-both (:conn store) (core/str-uuid [:foo :bar]))
  (def foo (qa/execute (:session (:conn store))
                       (qh/select :foo)))

  (get-data (:conn store) "06cee0d3-72f7-5bfd-ae3b-af7c2f0fef8f")
  (def data (:data (qa/execute (:session (:conn store))
                               (qh/select (:table (:conn store))
                                          (qh/columns :id :data)
                                          (qh/where {:id "06cee0d3-72f7-5bfd-ae3b-af7c2f0fef8f"}))
                               {:result-set-fn #(first %)})))
  (split-header data))
