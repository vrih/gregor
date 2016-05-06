<a href="http://www.weft.io">
<img src="http://www.weft.io/prod-assets/weftHorizonLogoTrans-08df1aeb53f624b6d89986fd03628f7b258ae6df90e41bb645dde4ceb5c8b724.png" width="125"/></a>

Current Version:
```clojure
[io.weft/gregor "0.2.0"]
```

[**API**](http://weftio.github.io/gregor/)

# Gregor

Lightweight Clojure bindings for [Apache Kafka](http://kafka.apache.org/) `0.9.X` and up.

Gregor wraps most of the Java API for the Kafka [Producer](http://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html) and [New Consumer](http://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html) and is almost feature complete as of `0.2.0`. The intent of this project is to stay very close to the Kafka API instead of adding more advanced features.

## Example

Here's an example of at-least-once processing (using the excellent [`mount`](https://github.com/tolitius/mount)):

```clojure
(ns gregor-sample-app.core
  (:gen-class)
  (:require [clojure.repl :as repl]
            [gregor.core :as gregor]
            [mount.core :as mount :refer [defstate]]))

(def run (atom true))

(defstate consumer
  :start (gregor/consumer "localhost:9092"
                          "testgroup"
                          ["test-topic"]
                          {"auto.offset.reset" "earliest"
                           "enable.auto.commit" "false"})
  :stop (gregor/close consumer))

(defstate producer
  :start (gregor/producer "localhost:9092")
  :stop (gregor/close producer))

(defn -main
  [& args]
  (mount/start)
  (repl/set-break-handler! (fn [sig] (reset! run false)))
  (while @run
    (let [consumer-records (gregor/poll consumer)
          recs (process-records consumer-records)]
      (doseq [rec recs]
        (gregor/send producer "other-topic" rec))
      (gregor/commit-offsets! consumer)))
  (mount/stop))
```

Any transformations over these records happen in `process-records`. Each record will be a
map with keys `:value :key :partition :topic :offset`.


### Todo

- `.listTopics` consumer
- `.partitionsFor` consumer
