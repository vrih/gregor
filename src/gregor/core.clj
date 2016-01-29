(ns gregor.core
  (:require [clojure.core.async :refer [<! >! <!! >!! chan go-loop put! close!]]
            [taoensso.timbre :as timbre])
  (:import [java.util Properties Arrays ArrayList]
           [org.apache.kafka.clients.consumer KafkaConsumer ConsumerRecords ConsumerRecord]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]))

(timbre/refer-timbre)

(defn- as-properties
  [m]
  (let [props (Properties.)]
    (doseq [[n v] m] (.setProperty props n v))
    props))

(defn record->map
  [record]
  (let [m {:value (.value record)
           :key (.key record)
           :partition (.partition record)
           :topic (.topic record)}]
    (try
      (assoc m :offset (.offset record))
      (catch java.lang.IllegalArgumentException _
        m))))

(defn get-consumer-config
  "Returns Kafka consumer client config.

   Args:
     servers: a comma-separated string list of kafka servers
     group-id: the consumer group that this consumer will belong to
     conf: optionally provide an map of configuration parameters for merging
  "
   [servers group-id & [conf]]
   (merge {"bootstrap.servers"  servers
           "group.id"           group-id
           "key.deserializer"   "org.apache.kafka.common.serialization.StringDeserializer"
           "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"} conf))

(defn consumer
  "Return a KafkaConsumer.
   More info on settings is available here:
   http://kafka.apache.org/documentation.html#newconsumerconfigs

   Required Keys:
     bootstrap.servers      : host1:port1,host2:port2
     key.deserializer       : org.apache.kafka.common.serialization.StringDeserializer
     value.deserializer     : org.apache.kafka.common.serialization.StringDeserializer
     group.id               : my-group-id

   Some Important Keys:
     enable.auto.commit      : true
     auto.commit.interval.ms : 5000
     auto.offset.reset       : latest
  "
  [config topics]
  (let [c (KafkaConsumer. (as-properties config))
        tops (ArrayList.)]
    (doseq [t topics] (.add tops t))
    (.subscribe c tops)
    (debug "new consumer subscribed to topics" topics)
    c))


(defn messages
  "Return the seq of messages currently available to the consumer (a single poll)."
  ([consumer] (messages consumer 100))
  ([consumer timeout]
   (trace "poll consumer for messages with" timeout "ms timeout")
   (let [records (seq (.poll consumer timeout))]
     (when records
       (map record->map records)))))

(defn messages-seq
  "Return a lazy sequence of messages from the consumer. Each element is a seq
  of messages currently available.

  Recommended for use with `doseq' or `run!', e.g.,

  (let [msgs (messages-seq my-consumer)]
    (when msgs
      (run! println msgs))))
  "
  ([consumer] (messages-seq consumer 100))
  ([consumer timeout]
  (repeatedly #(messages consumer timeout))))


(defn commit-offsets
  "Commit the offsets returned by the last poll for all subscribed topics and partitions."
  [consumer]
  (debug "commit offsets " (.assignment consumer))
  (.commitSync consumer))

(defn commit-offsets-async
  "Asynchronously commit the offsets returned by the last poll for all subscribed topics and partitions."
  [consumer]
  (debug "commit offsets " (.assignment consumer))
  (.commitAsync consumer))

(defn get-producer-config
  "Returns a minimum viable Kafka producer config by providing defaults.

  Args:
    brokers: comma-separated string list of kafak servers
    conf: optional additional config map to be merged

  Notes: Not necessary to list all brokers as client will find them,
    but provide at least two in case the first is unavailable.
  "
  [brokers & [conf]]
  (merge {"bootstrap.servers"   brokers
          "key.serializer"      "org.apache.kafka.common.serialization.StringSerializer"
          "value.serializer"    "org.apache.kafka.common.serialization.StringSerializer"}
          conf))

(defn producer
  [config]
  (KafkaProducer. (as-properties config)))

(defn send-message
  "Returns a java.util.ConcurrentFuture."
  [producer topic value]
  (let [pr (ProducerRecord. topic value)]
    (debug "send (" (.topic pr) ":" (.partition pr) ") message" (.value pr))
    (.send producer pr)))


;; (defn topic-partition
;;   [topic partition]
;;   (TopicPartition. topic partition))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Live testing

;; (defn new-consumer
;;   []
;;   (let [conf {"bootstrap.servers" "localhost:9092"
;;               "group.id" "wuuut"
;;               "enable.auto.commit" "false"
;;               "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
;;               "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"}]
;;     (def con (consumer conf ["test"]))))


;; (defn new-producer
;;   []
;;   (let [config {"bootstrap.servers" "localhost:9092"
;;                 "acks" "all"
;;                 "retries" "0"
;;                 "batch.size" "16384"
;;                 "linger.ms" "1"
;;                 "buffer.memory" "33554432"
;;                 "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
;;                 "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"}]
;;     (def prod (producer config))))

;; (defn start-consuming
;;   []
;;   (let [msgs (messages-seq con 100)]
;;     (when msgs
;;       (run! println msgs))))

;; (new-producer)
;; (send-message prod "test" {:a 1})
