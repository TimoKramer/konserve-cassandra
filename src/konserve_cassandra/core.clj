(ns konserve-cassandra.core
  (:require [clojure.core.async :refer [<!! thread put! close! chan] :as async]
            [konserve.serializers :as ser]
            [konserve.compressor :as comp]
            [konserve.encryptor :as encr]
            [hasch.core :as hasch]
            [konserve.protocols :refer [PEDNAsyncKeyValueStore
                                        -exists? -get -get-meta
                                        -update-in -assoc-in -dissoc
                                        PBinaryAsyncKeyValueStore
                                        -bassoc -bget
                                        -serialize -deserialize
                                        PKeyIterable
                                        -keys]]
            [konserve.storage-layout :refer [SplitLayout]]
            [konserve-cassandra.io :as io])
  (:import  [java.io ByteArrayOutputStream]))

(set! *warn-on-reflection* 1)
(def store-layout 1)

(defn str-uuid
  [key]
  (str (hasch/uuid key)))

(defn prep-ex
  [^String message ^Exception e]
  ; (.printStackTrace e)
  (ex-info message {:error (.getMessage e) :cause (.getCause e) :trace (.getStackTrace e)}))

(defn prep-stream
  [stream]
  { :input-stream stream
    :size nil})

(defrecord CassandraStore [conn default-serializer serializers compressor encryptor read-handlers write-handlers locks]
  PEDNAsyncKeyValueStore
  (-exists?
    [_ key]
    (let [res-ch (chan 1)]
      (thread
        (try
          (put! res-ch (io/exists? conn (str-uuid key)))
          (catch Exception e (put! res-ch (prep-ex "Failed to determine if item exists" e)))))
      res-ch))

  (-get
    [_ key]
    (let [res-ch (chan 1)]
      (thread
        (try
          (let [[header res] (io/get-data conn (str-uuid key))]
            (if (some? res)
              (let [rserializer (ser/byte->serializer (get header 1))
                    rcompressor (comp/byte->compressor (get header 2))
                    rencryptor  (encr/byte->encryptor  (get header 3))
                    reader (-> rserializer rencryptor rcompressor)
                    data (-deserialize reader read-handlers res)]
                (put! res-ch data))
              (close! res-ch)))
          (catch Exception e (put! res-ch (prep-ex "Failed to retrieve value from store" e)))))
      res-ch))

  (-get-meta
    [_ key]
    (let [res-ch (chan 1)]
      (thread
        (try
          (let [[header res] (io/get-meta conn (str-uuid key))]
            (if (some? res)
              (let [rserializer (ser/byte->serializer (get header 1))
                    rcompressor (comp/byte->compressor (get header 2))
                    rencryptor  (encr/byte->encryptor  (get header 3))
                    reader (-> rserializer rencryptor rcompressor)
                    data (-deserialize reader read-handlers res)]
                (put! res-ch data))
              (close! res-ch)))
          (catch Exception e (put! res-ch (prep-ex "Failed to retrieve metadata from store" e)))))
      res-ch))

  (-update-in
    [_ key-vec meta-up-fn up-fn args]
    (let [res-ch (chan 1)]
      (thread
        (try
          (let [[fkey & rkey] key-vec
                [[mheader ometa'] [vheader oval']] (io/get-both conn (str-uuid fkey))
                old-val [(when ometa'
                            (let [mserializer (ser/byte->serializer  (get mheader 1))
                                  mcompressor (comp/byte->compressor (get mheader 2))
                                  mencryptor  (encr/byte->encryptor  (get mheader 3))
                                  reader (-> mserializer mencryptor mcompressor)]
                              (-deserialize reader read-handlers ometa')))
                         (when oval'
                            (let [vserializer (ser/byte->serializer  (get vheader 1))
                                  vcompressor (comp/byte->compressor (get vheader 2))
                                  vencryptor  (encr/byte->encryptor  (get vheader 3))
                                  reader (-> vserializer vencryptor vcompressor)]
                              (-deserialize reader read-handlers oval')))]
                [nmeta nval] [(meta-up-fn (first old-val))
                              (if rkey (apply update-in (second old-val) rkey up-fn args) (apply up-fn (second old-val) args))]
                serializer (get serializers default-serializer)
                writer (-> serializer compressor encryptor)
                ^ByteArrayOutputStream mbaos (ByteArrayOutputStream.)
                ^ByteArrayOutputStream vbaos (ByteArrayOutputStream.)]
            (when nmeta
              (.write mbaos ^byte store-layout)
              (.write mbaos ^byte (ser/serializer-class->byte (type serializer)))
              (.write mbaos ^byte (comp/compressor->byte compressor))
              (.write mbaos ^byte (encr/encryptor->byte encryptor))
              (-serialize writer mbaos write-handlers nmeta))
            (when nval
              (.write vbaos ^byte store-layout)
              (.write vbaos ^byte (ser/serializer-class->byte (type serializer)))
              (.write vbaos ^byte (comp/compressor->byte compressor))
              (.write vbaos ^byte (encr/encryptor->byte encryptor))
              (-serialize writer vbaos write-handlers nval))
            (io/update-both conn (str-uuid fkey) [(.toByteArray mbaos) (.toByteArray vbaos)])
            (put! res-ch [(second old-val) nval]))
          (catch Exception e (put! res-ch (prep-ex "Failed to update/write value in store" e)))))
      res-ch))

  (-assoc-in [this key-vec meta val] (-update-in this key-vec meta (fn [_] val) []))

  (-dissoc
    [_ key]
    (let [res-ch (chan 1)]
      (thread
        (try
          (io/delete-entry conn (str-uuid key))
          (close! res-ch)
          (catch Exception e (put! res-ch (prep-ex "Failed to delete key-value pair from store" e)))))
      res-ch))

  PBinaryAsyncKeyValueStore
  (-bget
    [_ key locked-cb]
    (let [res-ch (chan 1)]
      (thread
        (try
          (let [[header res] (io/get-data conn (str-uuid key))]
            (if (some? res)
              (let [rserializer (ser/byte->serializer (get header 1))
                    rcompressor (comp/byte->compressor (get header 2))
                    rencryptor (encr/byte->encryptor  (get header 3))
                    reader (-> rserializer rencryptor rcompressor)
                    data (-deserialize reader read-handlers res)]
                (put! res-ch (locked-cb (prep-stream data))))
              (close! res-ch)))
          (catch Exception e (put! res-ch (prep-ex "Failed to retrieve binary value from store" e)))))
     res-ch))

  (-bassoc
    [_ key meta-up-fn input]
    (let [res-ch (chan 1)]
      (thread
        (try
          (let [[[mheader old-meta'] [_ old-val]] (io/get-both conn (str-uuid key))
                old-meta (when old-meta'
                            (let [mserializer (ser/byte->serializer  (get mheader 1))
                                  mcompressor (comp/byte->compressor (get mheader 2))
                                  mencryptor (encr/byte->encryptor  (get mheader 3))
                                  reader (-> mserializer mencryptor mcompressor)]
                              (-deserialize reader read-handlers old-meta')))
                new-meta (meta-up-fn old-meta)
                serializer (get serializers default-serializer)
                writer (-> serializer compressor encryptor)
                ^ByteArrayOutputStream mbaos (ByteArrayOutputStream.)
                ^ByteArrayOutputStream vbaos (ByteArrayOutputStream.)]
            (when new-meta
              (.write mbaos ^byte store-layout)
              (.write mbaos ^byte (ser/serializer-class->byte (type serializer)))
              (.write mbaos ^byte (comp/compressor->byte compressor))
              (.write mbaos ^byte (encr/encryptor->byte encryptor))
              (-serialize writer mbaos write-handlers new-meta))
            (when input
              (.write vbaos ^byte store-layout)
              (.write vbaos ^byte (ser/serializer-class->byte (type serializer)))
              (.write vbaos ^byte (comp/compressor->byte compressor))
              (.write vbaos ^byte (encr/encryptor->byte encryptor))
              (-serialize writer vbaos write-handlers input))
            (if old-meta
              (io/update-both conn (str-uuid key) [(.toByteArray mbaos) (.toByteArray vbaos)])
              (io/insert-both conn (str-uuid key) [(.toByteArray mbaos) (.toByteArray vbaos)]))
            (put! res-ch [old-val input]))
          (catch Exception e (put! res-ch (prep-ex "Failed to write binary value in store" e)))))
      res-ch))

  PKeyIterable
  (-keys
    [_]
    (let [res-ch (chan)]
      (thread
        (try
          (let [key-stream (io/get-keys conn)
                keys' (when key-stream
                        (for [[header k] key-stream]
                          (let [rserializer (ser/byte->serializer (get header 1))
                                rcompressor (comp/byte->compressor (get header 2))
                                rencryptor  (encr/byte->encryptor (get header 3))
                                reader (-> rserializer rencryptor rcompressor)]
                            (-deserialize reader read-handlers k))))]
            (doall (map #(put! res-ch (:key %)) keys'))
            (close! res-ch))
          (catch Exception e (put! res-ch (ex-info "Failed to retrieve keys from store"
                                                   {:error (.getMessage e) :cause (.getCause e) :trace (.getStackTrace e)})))))
      res-ch))

  SplitLayout
  (-get-raw-meta [_ key]
    (let [res-ch (chan 1)]
      (thread
        (try
          (let [res (io/raw-get-meta conn (str-uuid key))]
            (if res
              (put! res-ch res)
              (close! res-ch)))
          (catch Exception e (put! res-ch (prep-ex "Failed to retrieve raw metadata from store" e)))))
      res-ch))

  (-put-raw-meta [_ key blob]
    (let [res-ch (chan 1)]
      (thread
        (try
          (if (io/exists? conn (str-uuid key))
            (io/raw-update-meta conn (str-uuid key) blob)
            (io/raw-insert-meta conn (str-uuid key) blob))
          (close! res-ch)
          (catch Exception e (put! res-ch (prep-ex "Failed to write raw metadata to store" e)))))
      res-ch))

  (-get-raw-value [_ key]
    (let [res-ch (chan 1)]
      (thread
        (try
          (let [res (io/raw-get-data conn (str-uuid key))]
            (if res
              (put! res-ch res)
              (close! res-ch)))
          (catch Exception e (put! res-ch (prep-ex "Failed to retrieve raw value from store" e)))))
      res-ch))

  (-put-raw-value [_ key blob]
    (let [res-ch (chan 1)]
      (thread
        (try
          (if (io/exists? conn (str-uuid key))
            (io/raw-update-data conn (str-uuid key) blob)
            (io/raw-insert-data conn (str-uuid key) blob))
          (close! res-ch)
          (catch Exception e (put! res-ch (prep-ex "Failed to write raw value to store" e)))))
      res-ch)))

(defn new-store
  [config & {:keys [table default-serializer serializers compressor encryptor read-handlers write-handlers]
             :or {default-serializer :FressianSerializer
                  table "konserve"
                  compressor comp/lz4-compressor
                  encryptor encr/null-encryptor
                  read-handlers (atom {})
                  write-handlers (atom {})}}]
  (let [res-ch (chan 1)]
    (thread
      (try
        (let [cluster (io/cluster (:cluster config))
              session (io/connect cluster (-> config
                                              :cluster
                                              :session-keyspace))
              _ (io/create-table {:session session :table table})]
            (put! res-ch
              (map->CassandraStore {:conn {:session session :table table}
                                    :default-serializer default-serializer
                                    :serializers (merge ser/key->serializer serializers)
                                    :compressor compressor
                                    :encryptor encryptor
                                    :read-handlers read-handlers
                                    :write-handlers write-handlers
                                    :locks (atom {})})))
        (catch Exception e
          (put! res-ch (prep-ex "Failed to connect to store" e)))))
    res-ch))

(defn delete-store [{:keys [conn]}]
  (let [res-ch (chan 1)]
    (thread
      (try
        (io/drop-table conn)
        (close! res-ch)
        (catch Exception e (put! res-ch (prep-ex "Failed to delete store" e)))))
    res-ch))

(comment
  (require '[konserve.core :as k])
  (def config {:cluster {:session-keyspace "alia"
                         :contact-points ["127.0.0.1"]}})
  (def store (<!! (new-store config :table "foo")))
  (def conn (:conn store))
  (<!! (delete-store store))
  (io/create-table (-> store
                       :conn))
  (<!! (k/exists? store :foo))
  (<!! (k/assoc store :foo :bar))
  (<!! (k/get store :foo))
  (<!! (k/assoc-in store [:foo] :bar2))
  (<!! (k/get-in store [:fooo] :default))
  (<!! (k/update-in store [:foo] name))

  (def q (<!! (k/keys store)))
  (<!! q)
  (<!! (async/into #{} (k/keys store)))

  (def res-ch (chan))
  (def key-stream (io/get-keys conn))
  (def keys' (when key-stream
               (for [[header k] key-stream]
                 (let [rserializer (ser/byte->serializer (get header 1))
                       rcompressor (comp/byte->compressor (get header 2))
                       rencryptor  (encr/byte->encryptor (get header 3))
                       reader (-> rserializer rencryptor rcompressor)]
                   (-deserialize reader (atom {}) k)))))
  (doall (map :key keys'))
  (doall (map #(put! res-ch (:key %)) keys'))
  (close! res-ch))
