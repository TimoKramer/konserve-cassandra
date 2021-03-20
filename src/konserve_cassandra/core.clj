(ns konserve-cassandra.core
  "Address globally aggregated immutable key-value store(s)."
  (:require [clojure.core.async :refer [<!! thread chan put!]]
            [hasch.core :as h]
            [konserve
             [protocols :refer [PEDNAsyncKeyValueStore
                                -exists? -get -get-meta
                                -update-in -assoc-in -dissoc
                                PBinaryAsyncKeyValueStore
                                -bassoc -bget
                                -serialize -deserialize
                                PKeyIterable
                                -keys]]
             [serializers :as ks]
             [compressor :as kc]
             [core :as k]]
            [incognito.edn :refer [read-string-safe]]
            [konserve-cassandra.io :as io])
  (:import [java.io ByteArrayOutputStream ByteArrayInputStream]
           [java.nio ByteBuffer]))

(set! *warn-on-reflection* 1)

(def version 1)

(def encrypt 0)

(defn prep-ex
  [^String message ^Exception e]
  ; (.printStackTrace e)
  (ex-info message {:error (.getMessage e) :cause (.getCause e) :trace (.getStackTrace e)}))

(defn str-uuid
  [key]
  (str (h/uuid key)))

(defn to-byte-array
  "Return 4 Byte Array with following content
     1th Byte = Version of Konserve
     2th Byte = Serializer Type
     3th Byte = Compressor Type
     4th Byte = Encryptor Type"
  [version serializer compressor encryptor]
  (let [env-array     (byte-array [version serializer compressor encryptor])
        return-buffer (ByteBuffer/allocate 4)
        _             (.put return-buffer env-array)
        return-array  (.array return-buffer)]
    (.clear return-buffer)
    return-array))

(defn write-edn [edn serializer compressor write-handlers]
  (let [bos           (ByteArrayOutputStream.)
        serializer-id (get ks/serializer-class->byte (type serializer))
        compressor-id (get kc/compressor->byte compressor)
        _             (.write bos (to-byte-array version serializer-id compressor-id encrypt))
        _             (-serialize (compressor serializer) bos write-handlers edn)]
    (.toByteArray bos)))

(defn read-edn [ba serializers read-handlers]
  (let [serializer-id  (aget ba 1)
        compressor-id  (aget ba 2)
        serializer-key (ks/byte->key serializer-id)
        serializer     (get serializers serializer-key)
        compressor     (kc/byte->compressor compressor-id)
        bis            (ByteArrayInputStream. ba 4 (count ba))
        edn            (-deserialize (compressor serializer) read-handlers bis)]
    [edn serializer]))

(comment
  (def bos (ByteArrayOutputStream.))
  (def serializer-id 1)
  (def compressor-id 1)
  (def write-handler (atom {}))
  (.write bos (to-byte-array 1 1 1 0))
  (-serialize (kc/lz4-compressor (ks/fressian-serializer)) bos write-handler {:foo :bar})
  (def ba (.toByteArray bos))

  (def serializers (merge ks/key->serializer nil))
  (def serializer-key (ks/byte->key serializer-id))
  (def serializer (get serializers serializer-key))
  (def read-handlers (atom {}))
  (def compressor (kc/byte->compressor compressor-id))
  (def bis (ByteArrayInputStream. ba 4 (count ba)))
  (def edn (-deserialize (kc/lz4-compressor serializer) read-handlers bis)))

(defrecord CassandraStore [connection cluster default-serializer serializers compressor encryptor read-handlers write-handlers]
  PEDNAsyncKeyValueStore

  (-exists? [_ k]
    (io/exists? connection (str-uuid k)))

  (-get [_ k]
    (let [result (<!! (io/get-data connection (str-uuid k)))]
      (read-edn result serializers read-handlers)))

  (-get-meta [_ k]
    (let [result (<!! (io/get-meta connection (str-uuid k)))]
      (read-edn result serializers read-handlers)))

  (-update-in [_ key-vec meta-update-fn update-fn args]
    (let [[fkey & rkey] key-vec
          id (str-uuid fkey)
          {:keys [id data meta]} (<!! (io/get-all connection id))
          old-data (read-edn data serializers read-handlers)
          old-meta (read-edn meta serializers read-handlers)
          new-data (if (empty? rkey)
                     (apply update-fn old-data args)
                     (apply update-in old-data rkey update-fn args))
          new-meta (meta-update-fn old-meta)
          ba-data (write-edn new-data serializer compressor write-handlers)
          ba-meta (write-edn new-meta serializer compressor write-handlers)]
      (io/insert connection id [ba-meta ba-data])))

  (-assoc-in [this key-vec meta val]
    (-update-in this key-vec meta (fn [_] val) []))

  (-dissoc [_ k]
    (io/delete connection (str-uuid k))))

(comment
  (def ba2 (:data (<!! (io/get-data io/conn (str-uuid :foo)))))
  (def bis2 (ByteArrayInputStream. ba2))
  (def bis2 (ByteArrayInputStream. ba2 4 (count ba2))))

(defn new-cassandra-store
  "Creates an new Cassandra konserve store."
  [config & {:keys [table default-serializer serializers compressor encryptor read-handlers write-handlers]
             :or   {default-serializer :FressianSerializer
                    table "konserve"
                    compressor com/lz4-compressor
                    encryptor enc/null-encryptor
                    read-handlers (atom {})
                    write-handlers (atom {})}}]
  (let [res-ch (chan 1)]
    (thread
      (try
        (let [cluster (io/cluster (:cluster config))
              session (io/connect cluster (-> config
                                              :cluster
                                              :session-keyspace))]
          (put! res-ch
                      (map->CassandraStore
                        {:connection {:session session :table table}
                         :cluster cluster
                         :default-serializer default-serializer
                         :serializers (merge ser/key->serializer serializers)
                         :compressor compressor
                         :encryptor encryptor
                         :read-handlers read-handlers
                         :write-handlers write-handlers})))
        (catch Exception e
          (put! res-ch
                      (prep-ex "Failed to connect to store." e)))))
    res-ch))


(defn delete-store [conn]
  (io/drop-table conn))

(comment
  (require '[konserve.core :as k])
  (<!! (k/assoc io/conn :foo :bar))
  (<!! (k/get io/conn :foo))
  (<!! (k/assoc-in io/conn [:baz] {:bar 42}))
  (<!! (k/update-in io/conn [:foo] :name))
  (<!! (k/get io/conn :foo))
  (io/insert io/conn (str-uuid :foo) [(write-edn :data serializer compressor write-handler) (write-edn :meta serializer compressor write-handler)]))
