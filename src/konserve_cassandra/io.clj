(ns konserve-cassandra.io
  (:require [qbits.alia :as qa]
            [qbits.hayt :as qh]
            [taoensso.timbre :as log])
  (:import [java.io ByteArrayInputStream]
           [java.nio HeapByteBuffer]
           [com.datastax.oss.driver.internal.core.session DefaultSession]
           [com.datastax.oss.driver.api.core InvalidKeyspaceException]
           [clojure.lang ExceptionInfo]))

(defn split-header [bytes-or-blob dbtype]
  (when (some? bytes-or-blob)
    (let [data  (->> bytes vec (split-at 4))
          streamer (fn [header data] (list (byte-array header) (-> data byte-array (ByteArrayInputStream.))))]
      (apply streamer data))))

(defn connect
  ;; Config argument needs to be a map of at least
  ;; :session-keyspace and :contact-points
  ;; See https://docs.datastax.com/en/developer/java-driver/4.0/manual/core/configuration/
  [config]
  (try
   (qa/session config)
   (catch InvalidKeyspaceException e
     (log/error (ex-message e) {:config config})
     e)))

(defn table-exists?
  "Check if the Cassandra connection has any tables in its keyspace
   Argument: connection map with :session set to session"
  [session table]
  (->> (qh/select table
        (qh/columns :system_schema.tables)
        (qh/where {:keyspace_name (-> ^DefaultSession session .getKeyspace .get .toString)}))
       (qa/execute session)
       empty?
       not))

(defn row-exists? [session table id]
  (try
    (->> (qh/select :id
           (qh/columns table)
           (qh/where {:id id})
           (qh/limit 1))
         (qa/execute session))
    (catch ExceptionInfo _e
      false)))

(defn parse-result [res]
  (map (fn [{:keys [id header meta value]}]
         {:id id
          :header (when bytes? (-> ^HeapByteBuffer header .array slurp))
          :meta (when bytes? (-> ^HeapByteBuffer meta .array slurp))
          :value (when bytes? (-> ^HeapByteBuffer value .array slurp))})
       res))

(defn select [session table id column & {:keys [binary? locked-cb] :or {binary? false}}]
  (let [res (->> (qh/select :id column
                   (qh/columns table)
                   (qh/where {:id id}))
                 (#(qa/execute session % {:result-set-fn parse-result})))]
    res))

(defn insert [session table id header meta value]
  (->> (qh/insert table
                  (qh/values {:id id
                              :header header
                              :meta meta
                              :value value}))
       (qa/execute session)))

(defn delete [session table id]
  (->> (qh/delete table
                  (qh/where {:id id}))
       (qa/execute session)))

(defn copy [session table from to]
  ;; TODO very bad. not possible to make this consistent with cassandra?
  (let [{:keys [header meta value] :as res}
        (->> (qh/select table
                        (qh/columns :id :header :meta :value)
                        (qh/where {:id from}))
             (qa/execute session))]
    (->> (qh/insert table
                    (qh/values {:id to
                                :header header
                                :meta meta
                                :value value}))
         (qa/execute session))))

(defn move [session table from to]
  (let [{:keys [header meta value] :as res}
        (->> (qh/select table
                        (qh/columns :id :header :meta :value)
                        (qh/where {:id from}))
             (qa/execute session))]
    (qa/batch [(qh/insert table
                          (qh/values {:id to
                                      :header header
                                      :meta meta
                                      :value value}))
               (qh/delete table
                          (qh/where {:id from}))])))

(defn keys [session table]
  (->> (qh/select table
                  (qh/columns :id))
       (#(qa/execute session % {:result-set-fn parse-result}))))

(defn create-table [session table]
  (->> (qh/create-table table
                       (qh/if-not-exists)
                       (qh/column-definitions {:id :varchar
                                               :header :blob
                                               :meta :blob
                                               :value :blob
                                               :primary-key [:id]}))
       (qa/execute session)))

(defn drop-table [session table]
  (qa/execute-async session
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

  (create-table my-session :foo)
  (insert my-session :foo "myid" (byte-array (map byte "header")) (byte-array (mapv byte "meta")) (byte-array (mapv byte "value")))
  (copy my-session :foo "myid" "myid2")
  (->> (move my-session :foo "myid" "myid3") (qa/execute my-session))
  (->> (qh/select :foo
                  (qh/columns :id :header :meta :value))
       ((fn [query] (qa/execute my-session query {:result-set-fn
                                                  (fn [res]
                                                    (let [parse (fn [{:keys [id header meta value]}]
                                                                  {:id id
                                                                   :header (when bytes? (-> header .array slurp))
                                                                   :meta (when bytes? (-> meta .array slurp))
                                                                   :value (when bytes? (-> value .array slurp))})]
                                                      (map parse res)))}))))

  (qa/execute my-session "CREATE TABLE users (user_name varchar,
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

  (qa/execute my-session "INSERT INTO users
                          (user_name, first_name, last_name, emails, birth_year, amap, tags, auuid,valid)
                          VALUES('frodo', 'Frodo', 'Baggins',
                          {'f@baggins.com', 'baggins@gmail.com'}, 1,
                          {'foo': 1, 'bar': 2}, [4, 5, 6],
                          1f84b56b-5481-4ee4-8236-8a3831ee5892, true);")

  (def prepared-statement (qa/prepare my-session "select * from users where user_name=?;"))

  (qa/execute my-session prepared-statement {:values ["frodo"]})

  (def prepared-statement (qa/prepare my-session "select * from users where user_name= :name limit :lmt;"))

  (qa/execute my-session prepared-statement {:values {:name "frodo" :lmt (int 1)}}))
