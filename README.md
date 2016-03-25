# gregor

Lightweight Clojure bindings for Kafka 0.9+

[Codox API](https://github.com/weftio/gregor/blob/master/doc/index.html)

## Example

Here's an example of at-least-once processing (using `mount`):

```clojure
(ns gregor-sample-app.core
  (:gen-class)
  (:require [clojure.repl :as repl]
            [gregor.core :as gregor]
            [mount.core :as mount :refer [defstate]]))

(def run (atom true))
(def buffer (atom []))

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
      (let [consumer-records (gregor/poll consumer)]
        (swap! buffer into recs)
        (when (>= (count @buffer) 42)
          (gregor/send producer "other-topic" (process-records @buffer))
          (gregor/commit-offsets! consumer)
          (reset! buffer []))))
  (mount/stop))
```

Any transformations over these records happens in process-records. Each record will be a
map with keys `:value :key :partition :topic :offset`.


### Todo

- .listTopics consumer
- .partitionsFor consumer
