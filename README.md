# konserve-cassandra
A [konserve backend](https://github.com/replikativ/konserve) for [Cassandra](https://cassandra.apache.org/_/index.html).

## Usage

Add to your dependencies:

[![Clojars Project](http://clojars.org/timokramer/konserve-cassandra/latest-version.svg)](http://clojars.org/timokramer/konserve-cassandra)

### Setup Test Environment

```bash
podman run --name cassandra --publish 9042:9042 -d docker.io/cassandra:latest
```

### Synchronous Execution

``` clojure
(require '[konserve-cassandra.core :refer [connect-store]]
         '[konserve.core :as k])

(def cassandra-conf
  {:session-keyspace "alia"})
   
(def store (connect-store cassandra-conf :opts {:sync? true}))

(k/assoc-in store ["foo" :bar] {:foo "baz"} {:sync? true})
(k/get-in store ["foo"] nil {:sync? true})
(k/exists? store "foo" {:sync? true})

(k/assoc-in store [:bar] 42 {:sync? true})
(k/update-in store [:bar] inc {:sync? true})
(k/get-in store [:bar] nil {:sync? true})
(k/dissoc store :bar {:sync? true})

(k/append store :error-log {:type :horrible} {:sync? true})
(k/log store :error-log {:sync? true})

(let [ba (byte-array (* 10 1024 1024) (byte 42))]
  (time (k/bassoc store "banana" ba {:sync? true})))

(k/bassoc store :binbar (byte-array (range 10)) {:sync? true})
(k/bget store :binbar (fn [{:keys [input-stream]}]
                               (map byte (slurp input-stream)))
       {:sync? true})
               
```

### Asynchronous Execution

``` clojure
(ns test-db
  (require '[konserve-cassandra.core :refer [connect-store]]
           '[clojure.core.async :refer [<!]]
           '[konserve.core :as k])

(def cassandra-conf
  {:session-keyspace "alia"})
   
(def store (<! (connect-store cassandra-conf :opts {:sync? false})))

(<! (k/assoc-in store ["foo" :bar] {:foo "baz"}))
(<! (k/get-in store ["foo"]))
(<! (k/exists? store "foo"))

(<! (k/assoc-in store [:bar] 42))
(<! (k/update-in store [:bar] inc))
(<! (k/get-in store [:bar]))
(<! (k/dissoc store :bar))

(<! (k/append store :error-log {:type :horrible}))
(<! (k/log store :error-log))

(<! (k/bassoc store :binbar (byte-array (range 10)) {:sync? false}))
(<! (k/bget store :binbar (fn [{:keys [input-stream]}]
                            (map byte (slurp input-stream)))
            {:sync? false}))
```

## Status

Alpha

Only tested with Cassandra 4.1.1

## License

Copyright © 2023 Timo Kramer

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
