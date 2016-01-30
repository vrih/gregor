(ns gregor.core
  (:import [java.util Properties Arrays ArrayList]
           [org.apache.kafka.clients.consumer KafkaConsumer ConsumerRecords ConsumerRecord]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]))

(defn- as-properties
  [m]
  (let [ps (Properties.)]
    (doseq [[k v] m] (.setProperty ps k v))
    ps))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Kafka Consumer
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn consumer-record->map
  [^ConsumerRecord record]
  {:value (.value record)
   :key (.key record)
   :partition (.partition record)
   :topic (.topic record)
   :offset (.offset record)})

(defn consumer
  "Return a KafkaConsumer.

  Set the list of topics to subscribe to dynamically via the topics argument.

  More info on settings is available here:
  http://kafka.apache.org/documentation.html#newconsumerconfigs

  Required config keys:
    bootstrap.servers      : host1:port1,host2:port2
    key.deserializer       : e.g. org.apache.kafka.common.serialization.StringDeserializer
    value.deserializer     : e.g. org.apache.kafka.common.serialization.StringDeserializer"
  ([config] (consumer [] ""))
  ([config topics group-id]
   (let [kc (-> config
                (assoc "group.id" group-id)
                (as-properties)
                (KafkaConsumer.))]
     (when (not-empty topics)
       (.subscribe kc topics))
     kc)))

(defn commit-offsets
  "Commit the offsets returned by the last poll for all subscribed topics and partitions."
  ([consumer] (commit-offsets consumer false))
  ([consumer async?]  
   (if async?
     (.commitAsync consumer)
     (.commitSync consumer))))

(defn poll
  "Return a seq of messages currently available to the consumer (via a single poll).

  timeout - the time, in milliseconds, spent waiting in poll if data is not
  available. If 0, returns immediately with any records that are available now.
  Must not be negative."
  ([consumer] (poll consumer 100))
  ([consumer timeout]
   (trace "poll consumer for messages with" timeout "ms timeout")
   (->> (.poll consumer timeout)
        (map consumer-record->map)
        (seq))))

(defn messages
  "Return a lazy sequence of messages from the consumer. Each element is the seq
  of messages currently available.

  timeout - the time, in milliseconds, spent waiting in poll if data is not
  available. If 0, returns immediately with any records that are available now.
  Must not be negative.

  Recommended for use with doseq or run!:

    (let [msgs (messages my-consumer)]
      (when msgs
        (run! println msgs))))
  "
  ([consumer] (messages consumer 100))
  ([consumer timeout] (repeatedly #(poll consumer timeout))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Kafka Producer
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn producer
  "Return a KafkaProducer.

  The producer is thread safe and sharing a single producer instance across
  threads will generally be faster than having multiple instances.

  More info on settings is available here:
  http://kafka.apache.org/documentation.html#producerconfigs

  Required config keys:
    bootstrap.servers      : host1:port1,host2:port2
    key.deserializer       : e.g. org.apache.kafka.common.serialization.StringDeserializer
    value.deserializer     : e.g. org.apache.kafka.common.serialization.StringDeserializer"
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
