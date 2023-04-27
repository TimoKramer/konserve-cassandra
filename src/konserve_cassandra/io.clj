(ns konserve-cassandra.io
  (:require [qbits.alia :as qa]
            [qbits.hayt :as qh]
            [taoensso.timbre :as log])
  (:import [java.nio HeapByteBuffer]
           [java.io ByteArrayInputStream]
           [java.util Arrays]
           [com.datastax.oss.driver.api.core InvalidKeyspaceException]
           [clojure.lang ExceptionInfo]))

(defn connect
  ;; Config argument needs to be a map of at least
  ;; :session-keyspace and :contact-points
  ;; See https://docs.datastax.com/en/developer/java-driver/4.0/manual/core/configuration/
  [config]
  (try
   (qa/session config)
   (catch InvalidKeyspaceException e
     (log/error (ex-message e) {:config config})
     e)
   (catch ExceptionInfo e
     (log/error "WHUUUUUUUUUUT!" #_(ex-message e ) {:config config})
     e)))

(defn row-exists? [session table id]
  (try
    (->> (qh/select table
           (qh/columns :id)
           (qh/where {:id id})
           (qh/limit 1))
         (qa/execute session)
         seq?)
    (catch ExceptionInfo _e
      false)))

(defn parse-result [res]
  (map (fn [{:keys [id header meta val]}]
         (cond-> {:id id}
           header (assoc :header (-> ^HeapByteBuffer header .array slurp))
           meta (assoc :meta (-> ^HeapByteBuffer meta .array slurp))
           val (assoc :val (-> ^HeapByteBuffer val .array slurp))))
       res))

(defn select [session table id column & {:keys [binary? locked-cb] :or {binary? false}}]
  (let [res (->> (qh/select table
                   (qh/columns :id column)
                   (qh/where {:id id}))
                 (qa/execute session)
                 first
                 column
                 .array)]
    (if binary?
      (locked-cb {:input-stream (when res (ByteArrayInputStream. res))
                  :size nil})
      res)))

(type (byte-array 2))

(defn insert [session table id header meta val]
  (->> (qh/insert table
                  (qh/values {:id id
                              :header header
                              :meta meta
                              :val val}))
       (qa/execute session)))

(defn delete [session table id]
  (->> (qh/delete table
                  (qh/where {:id id}))
       (qa/execute session)))

(defn copy [session table from to]
  ;; TODO very bad. not possible to make this consistent with cassandra?
  (let [{:keys [header meta val]}
        (->> (qh/select table
                        (qh/columns :id :header :meta :val)
                        (qh/where {:id from}))
             (qa/execute session))]
    (->> (qh/insert table
                    (qh/values {:id to
                                :header header
                                :meta meta
                                :val val}))
         (qa/execute session))))

(defn move [session table from to]
  (let [{:keys [header meta val] :as res}
        (->> (qh/select table
                        (qh/columns :id :header :meta :val)
                        (qh/where {:id from}))
             (qa/execute session))]
    (->> (qa/batch [(qh/insert table
                               (qh/values {:id to
                                           :header header
                                           :meta meta
                                           :val val}))
                    (qh/delete table
                               (qh/where {:id from}))])
         (qa/execute session))))

(defn keys [session table]
  (->> (qh/select table
                  (qh/columns :id))
       (qa/execute session)
       (map :id)))

(defn create-table [session table]
  (->> (qh/create-table table
                       (qh/if-not-exists)
                       (qh/column-definitions {:id :varchar
                                               :header :blob
                                               :meta :blob
                                               :val :blob
                                               :primary-key [:id]}))
       (qa/execute session)))

(defn drop-table [session table]
  (qa/execute session
              (qh/drop-table table)))

(defn close [session]
  (qa/close session))

(comment
  (def keyspace "alia")
  (let [session (qa/session {})]
    (->> (qh/create-keyspace keyspace {:with {:replication {:class "SimpleStrategy" :replication_factor 1}}})
         (qa/execute session)))
  (qa/execute session (qdsl/drop-keyspace keyspace))
  (def my-session (qa/session {:session-keyspace keyspace
                               :contact-points ["127.0.0.1"]}))

  (drop-table my-session :foo)
  (create-table my-session :foo)
  (insert my-session :foo "myid" (byte-array (map byte "header")) (byte-array (mapv byte "meta")) (byte-array (mapv byte "value")))
  (copy my-session :foo "myid" "myid2")
  (move my-session :foo "myid" "myid3")
  (keys my-session :foo)
  (delete my-session :foo "myid3")
  (select my-session :foo "myid2" :meta)
  (row-exists? my-session :foo "myid2")
  (close my-session)
  (->> (qh/select :foo
                  (qh/columns :id :header :meta :value))
       ((fn [query] (qa/execute my-session query {:result-set-fn
                                                  (fn [res]
                                                    (let [parse (fn [{:keys [id header meta value]}]
                                                                  {:id id
                                                                   :header (when bytes? (-> header .array slurp))
                                                                   :meta (when bytes? (-> meta .array slurp))
                                                                   :value (when bytes? (-> value .array slurp))})]
                                                      (map parse res)))})))))
