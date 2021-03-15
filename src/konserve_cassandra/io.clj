(ns konserve-cassandra.io
  (:require [qbits.alia :as qa]
            [qbits.alia.async :as qasync]
            [qbits.hayt :as qh]
            [clojure.core.async :as async])
  (:import  [java.io ByteArrayInputStream]))

(defn split-header [bytes-or-blob dbtype]
  (when (some? bytes-or-blob) 
    (let [data  (->> bytes vec (split-at 4))
          streamer (fn [header data] (list (byte-array header) (-> data byte-array (ByteArrayInputStream.))))]
      (apply streamer data))))

(defn cluster [cluster-config]
  (qa/cluster cluster-config))

(defn connect [cluster]
  (qa/connect cluster))

; TODO enough to check on any table?
(defn it-exists?
  "Check if the Cassandra connection has any tables in its keyspace
     
   Argument: connection map"
  [{:keys [session]}]
  (-> (qa/execute session (qh/select :system_schema.tables
                                     (qh/columns :table_name)
                                     (qh/where {:keyspace_name (.getLoggedKeyspace session)})))
      (empty?)
      (not))) ; returns empty list so checking on not emtpy

(defn get-it [conn id]
  (let [res' (qa/execute (:session conn) (qh/select (:table conn) (qh/where {:id id})))
        data (:data res')
        meta (:meta res')
        res (if (and meta data)
              [(split-header meta (-> conn :db :dbtype)) (split-header data  (-> conn :db :dbtype))]
              [nil nil])]
    res))

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

  (.getLoggedKeyspace (qa/connect cluster))

  (create-table conn)
  (it-exists? conn)
  (drop-table conn)

  (qa/execute session "CREATE KEYSPACE alia WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};")

  (qa/execute session "CREATE TABLE users (user_name varchar,
                                           first_name varchar,
                                           last_name varchar,
                                           auuid uuid,
                                           birth_year bigint,
                                           created timestamp,
                                           valid boolean,
                                           emails set<text>,
                                           tags list<bigint>,
                                           amap map<varchar, bigint>,
                                           PRIMARY KEY (user_name));")

  (qa/execute session "INSERT INTO users
                       (user_name, first_name, last_name, emails, birth_year, amap, tags, auuid,valid)
                       VALUES('frodo', 'Frodo', 'Baggins',
                       {'f@baggins.com', 'baggins@gmail.com'}, 1,
                       {'foo': 1, 'bar': 2}, [4, 5, 6],
                       1f84b56b-5481-4ee4-8236-8a3831ee5892, true);")

  (def prepared-statement (qa/prepare session "select * from users where user_name=?;"))

  (qa/execute session prepared-statement {:values ["frodo"]})

  (def prepared-statement (qa/prepare session "select * from users where user_name= :name limit :lmt;"))

  (qa/execute session prepared-statement {:values {:name "frodo" :lmt (int 1)}}))
