# gregor

A Clojure library for Kafka 0.9+

## Consumer

```clojure
(defn do-something!
    [msgs]
    ...)


(let [conf {"bootstrap.servers" "localhost:9092"
            "key.deserializer"  "org.apache.kafka.common.serialization.StringDeserializer"
            "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"}
      c (consumer conf ["some-topic"] "some-group")
      msgs (messages c)]
  (run! do-something! msgs))
```
