(ns gregor.core
  (:import [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.clients.consumer Consumer KafkaConsumer
            ConsumerRecords ConsumerRecord OffsetAndMetadata OffsetCommitCallback]
           [org.apache.kafka.clients.producer Producer KafkaProducer
            ProducerRecord]))

(defn- as-properties
  [m]
  (let [ps (java.util.Properties.)]
    (doseq [[k v] m] (.setProperty ps k v))
    ps))

(defn topic-partition
  [^String topic ^Integer partition]
  (TopicPartition. topic partition))

(defn offset-and-metadata
  [^Long offset & metadata]
  (if metadata
    (OffsetAndMetadata. offset metadata)
    (OffsetAndMetadata. offset)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Kafka Consumer
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- vectorize
  [msg t p & tps]
  (let [tps (remove nil? tps)]
    (if (even? (count tps))
      (let [l (if (empty? tps)
                []
                (for [[top part] tps] (topic-partition top part)))]
        (-> (topic-partition t p)
            (vector)
            (into l)))
      (throw (IllegalArgumentException. msg)))))

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
  ^KafkaConsumer
  ([config] (consumer [] ""))
  ([config topics group-id]
   (let [kc (-> config
                (assoc "group.id" group-id)
                (as-properties)
                (KafkaConsumer.))]
     (when (not-empty topics)
       (.subscribe kc topics))
     kc)))

(defn assign!
  "Manually assign topic-partition pairs to this consumer."
  [^Consumer consumer topic partition & topic-partitions]
  (let [msg "assign! expects even number of args after partition, found odd number."
        tps (vectorize msg topic partition topic-partitions)]
    (.assign consumer tps)))

(defn commit-offsets-async!
  "Commit offsets returned on the last poll for all the subscribed list of
  topics and partition."
  ([^Consumer consumer] (.commitAsync consumer))
  ([^Consumer consumer ^OffsetCommitCallback callback]
   (.commitAsync consumer callback))
  ([^Consumer consumer offsets ^OffsetCommitCallback callback]
   (.commitAsync consumer offsets callback)))

(defn commit-offsets!
  "Commit offsets returned on the last poll for all the subscribed list of
  topics and partition.
  offsets - a map of offsets by partition with associated metadata."
  ([^Consumer consumer] (.commitSync consumer))
  ([^Consumer consumer offsets] (.commitSync consumer offsets)))

(defn seek!
  "Overrides the fetch offsets that the consumer will use on the next poll."
  [consumer topic partition offset]
  (.seek consumer (topic-partition topic partition) offset))

(defn seek-to!
  "Seek to the :first or :last offset for each of the given partitions."
  [consumer destination topic partition & topic-partitions]
  (let [msg "seek-to expects even number of args after partition, found odd number."
        tps (vectorize msg topic partition topic-partitions)]
    (case destination
      :first (.seekToBeginning consumer tps)
      :last (.seekToEnd consumer tps))))

(defn position
  "Return the offset of the next record that will be fetched (if a record with
  that offset exists)."
  [topic partition]
  (.position (topic-partition topic partition)))

(defn poll
  "Return a seq of messages currently available to the consumer (via a single poll).
  Fetches sequetially from the last consumed offset.

  timeout - the time, in milliseconds, spent waiting in poll if data is not
  available. If 0, returns immediately with any records that are available now.
  Must not be negative."
  ([consumer] (poll consumer 100))
  ([consumer timeout]
   ;; (trace "poll consumer for messages with" timeout "ms timeout")
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
  ([^Consumer consumer] (messages consumer 100))
  ([^Consumer consumer timeout] (repeatedly #(poll consumer timeout))))


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
  ^KafkaProducer
  [config]
  (KafkaProducer. (as-properties config)))

(defn send-message
  "Returns a java.util.ConcurrentFuture."
  [^Producer producer topic value]
  (let [pr (ProducerRecord. topic value)]
    ;; (debug "send (" (.topic pr) ":" (.partition pr) ") message" (.value pr))
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
