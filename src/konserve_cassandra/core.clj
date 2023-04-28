(ns konserve-cassandra.core
  "Address globally aggregated immutable key-value store(s)."
  (:require [konserve.compressor :refer [null-compressor]]
            [konserve.encryptor :refer [null-encryptor]]
            [konserve.impl.defaults :refer [connect-default-store]]
            [konserve.impl.storage-layout :refer [PBackingStore PBackingBlob PBackingLock -delete-store]]
            [konserve.utils :refer [async+sync *default-sync-translation*]]
            [superv.async :refer [go-try-]]
            [konserve-cassandra.io :as io]))

(set! *warn-on-reflection* 1)

(def ^:const default-table "konserve")

(extend-protocol PBackingLock
  Boolean
  (-release [_ env]
    (if (:sync? env) nil (go-try- nil))))

(defrecord CassandraBlob [store key data]
  PBackingBlob
  (-sync [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (let [{:keys [header meta value]} @data]
                           (if (and header meta value)
                             (io/insert (:session store) (:table store) key header meta value)
                             (throw (ex-info "Updating a row is only possible if header, meta and value are set." {:data @data})))
                           (reset! data {})))))
  (-close [_ env]
    (if (:sync? env) nil (go-try- nil)))
  (-get-lock [_ env]
    (if (:sync? env) true (go-try- true))) ;; May not return nil, otherwise eternal retries
  (-read-header [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (io/select (:session store) (:table store) key :header))))
  (-read-meta [_ _meta-size env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (io/select (:session store) (:table store) key :meta))))
  (-read-value [_ _meta-size env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (io/select (:session store) (:table store) key :val))))
  (-read-binary [_ _meta-size locked-cb env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (io/select (:session store) (:table store) key :val :binary? true :locked-cb locked-cb))))
  (-write-header [_ header env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :header header))))
  (-write-meta [_ meta env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :meta meta))))
  (-write-value [_ value _meta-size env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :value value))))
  (-write-binary [_ _meta-size blob env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :value blob)))))

(defrecord CassandraStore [session table]
  PBackingStore
  (-create-blob [this id env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (CassandraBlob. this id (atom {})))))
  (-delete-blob [_ id env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (io/delete session table id))))
  (-blob-exists? [_ id env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (io/row-exists? session table id))))
  (-copy [_ from to env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (io/copy session table from to))))
  (-atomic-move [_ from to env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (io/move session table from to))))
  (-migratable [_ _key _id env]
    (if (:sync? env) nil (go-try- nil)))
  (-migrate [_ _migration-key _key-vec _serializer _read-handlers _write-handlers env]
    (if (:sync? env) nil (go-try- nil)))
  (-create-store [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (io/create-table session table))))
  (-sync-store [_ env]
    (if (:sync? env) nil (go-try- nil)))
  (-delete-store [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (io/drop-table session table))))
  (-keys [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (io/keys session table)))))

(defn connect-store
  "Creates an new Cassandra konserve store."
  [cassandra-config & {:keys [table opts]
                       :or {table default-table}
                       :as params}]
  (let [complete-opts (merge {:sync? true} opts)
        conn (io/connect cassandra-config)
        backing (CassandraStore. conn table)
        config (merge {:opts               complete-opts
                       :config             {:sync-blob? true
                                            :in-place? true
                                            :lock-blob? true}
                       :default-serializer :FressianSerializer
                       :compressor         null-compressor
                       :encryptor          null-encryptor
                       :buffer-size        (* 1024 1024)}
                      (dissoc params :opts :config)
                      cassandra-config)]
    (connect-default-store backing config)))

(defn delete-store [cassandra-config & {:keys [table opts] :or {table default-table}}]
  (let [complete-opts (merge {:sync? true} opts)
        conn (io/connect cassandra-config)
        backing (CassandraStore. conn table)]
    (-delete-store backing complete-opts)))

(defn release
  "Must be called after work on database has finished in order to close connection"
  [{{session :session} :backing} env]
  (async+sync (:sync? env) *default-sync-translation*
              (go-try- (io/close session))))

(comment
  (require '[konserve.core :as k]
           '[clojure.core.async :refer [<!!]])

  (def store-conf {:session-keyspace "alia"})

  (delete-store store-conf :opts {:sync? true})
  (def store (connect-store store-conf
                            :opts {:sync? true}))
  (k/assoc-in store ["foo" :bar] {:foo "baz"} {:sync? true})
  (k/get-in store ["foo"] nil {:sync? true})
  (k/exists? store "foo" {:sync? true})
  (k/assoc-in store [:bar] 42 {:sync? true})
  (k/update-in store [:bar] inc {:sync? true})
  (k/get-in store [:bar] nil {:sync? true})
  (k/dissoc store :bar {:sync? true})
  (k/append store :error-log {:type :horrible} {:sync? true})
  (k/log store :error-log {:sync? true})
  (k/keys store {:sync? true})
  (k/bassoc store :binbar (byte-array (range 10)) {:sync? true})
  (k/bget store :binbar (fn [{:keys [input-stream]}]
                          (map byte (slurp input-stream)))
          {:sync? true})
  (release store {:sync? true})

  (<!! (delete-store store-conf :opts {:sync? false}))
  (def store (<!! (connect-store store-conf
                                 :opts {:sync? false})))
  (<!! (k/assoc-in store ["foo" :bar] {:foo "baz"} {:sync? false}))
  (<!! (k/get-in store ["foo"] nil {:sync? false}))
  (<!! (k/exists? store "foo" {:sync? false}))
  (<!! (k/assoc-in store [:bar] 42 {:sync? false}))
  (<!! (k/update-in store [:bar] inc {:sync? false}))
  (<!! (k/get-in store [:bar] nil {:sync? false}))
  (<!! (k/dissoc store :bar {:sync? false}))
  (<!! (k/append store :error-log {:type :horrible} {:sync? false}))
  (<!! (k/log store :error-log {:sync? false}))
  (<!! (k/keys store {:sync? false}))
  (<!! (k/bassoc store :binbar (byte-array (range 10)) {:sync? false}))
  (<!! (k/bget store :binbar (fn [{:keys [input-stream]}]
                               (map byte (slurp input-stream)))
               {:sync? false}))
  (<!! (release store {:sync? false})))
