(ns konserve-cassandra.core
  "Address globally aggregated immutable key-value store(s)."
  (:require [clojure.core.async :as async]
            [hasch.core :as hasch]
            [konserve.protocols :refer [PEDNAsyncKeyValueStore
                                        -exists? -get -get-meta
                                        -update-in -assoc-in -dissoc
                                        PBinaryAsyncKeyValueStore
                                        -bassoc -bget
                                        -serialize -deserialize
                                        PKeyIterable
                                        -keys]]
            [konserve.serializers :as ser]
            [konserve.compressor :as com]
            [konserve.encryptor :as encr]
            [incognito.edn :refer [read-string-safe]]
            [konserve-cassandra.io :as io]))

(set! *warn-on-reflection* 1)

(defn prep-ex
  [^String message ^Exception e]
  ; (.printStackTrace e)
  (ex-info message {:error (.getMessage e) :cause (.getCause e) :trace (.getStackTrace e)}))

(defn new-cassandra-store
  "Creates an new Cassandra konserve store."
  [config & {:keys [table serializer compressor encryptor read-handlers write-handlers]
             :or   {table "konserve"
                    serializer :FressianSerializer
                    compressor com/lz4-compressor
                    encryptor encr/null-encryptor
                    read-handlers (atom {})
                    write-handlers (atom {})}}]
  (let [res-ch (async/chan 1)]
    (async/thread
      (try
        (let [cluster (io/cluster (:cluster config))
              connection (io/connect cluster)]
          (async/put! res-ch
                      #_(map->CassandraStore)
                      {:connection connection
                       :cluster cluster
                       :table table
                       :serializer serializer
                       :compressor compressor
                       :encryptor encryptor
                       :read-handlers read-handlers
                       :write-handlers write-handlers
                       :locks (atom {})}))
        (catch Exception e
          (async/put! res-ch
                      (prep-ex "Failed to connect to store." e)))))
    res-ch))


(defn delete-store [conn]
  (io/drop-table conn))

(comment
  (async/<!! (new-cassandra-store {:cluster {:session-keyspace "alia"
                                             :contact-points ["127.0.0.1"]}})))
