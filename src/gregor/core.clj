(ns gregor.core
  (:import [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.clients.consumer Consumer KafkaConsumer
            ConsumerRecords ConsumerRecord OffsetAndMetadata OffsetCommitCallback]
           [org.apache.kafka.clients.producer Producer KafkaProducer
            ProducerRecord])
  (:require [clojure.string :as str]))

;; TODO: OffsetCommitCallback impl / reify?
;; committed (test)
;; commit-offsets (test)
;; listTopics
;; pause
;; position
;; resume 
;; subscribe
;; unsubscribe
;; wakeup
;; producer stuff

;; clojurify java objects returned by fns, e.g. committed ?

;; Not doing:
;; metrics
;; partitionsFor

(def str-deserializer "org.apache.kafka.common.serialization.StringDeserializer")
(def str-serializer "org.apache.kafka.common.serialization.StringSerializer")

(defn- as-properties
  [m]
  (let [ps (java.util.Properties.)]
    (doseq [[k v] m] (.setProperty ps k v))
    ps))

(defn topic-partition
  "A topic name and partition number."
  [^String topic ^Integer partition]
  (TopicPartition. topic partition))

(defn offset-and-metadata
  "Metadata for when an offset is committed."
  ([^Long offset] (OffsetAndMetadata. offset))
  ([^Long offset ^String metadata] (OffsetAndMetadata. offset metadata)))

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

  Args:
    servers: comma-separated host:port strs or list of strs as bootstrap servers
    group-id: str that identifies the consumer group this consumer belongs to
    topics: an optional list of topics to which the consumer will be dynamically
            subscribed.
    config: an optional map of str to str containing additional consumer
            configuration. More info on optional config is available here:
            http://kafka.apache.org/documentation.html#newconsumerconfigs

  The StringDeserializer class is the default for both key.deserializer and
  value.deserializer.
  "
  ^KafkaConsumer
  [servers group-id & [topics config]]
  (let [servers (if (sequential? servers) (str/join "," servers) servers)
         kc (-> config
                (assoc "group.id" group-id
                       "bootstrap.servers" servers
                       "key.deserializer" str-deserializer
                       "value.deserializer" str-deserializer)
                (as-properties)
                (KafkaConsumer.))]
     (when (not-empty topics) (.subscribe kc topics))
     kc))

(defn assign!
  "Manually assign topic-partition pairs to this consumer."
  [^Consumer consumer ^String topic ^Integer partition & topic-partitions]
  (let [msg "assign! expects even number of args after partition, found odd number."
        tps (vectorize msg topic partition topic-partitions)]
    (.assign consumer tps)))

(defn assignment
  "Get the set of partitions currently assigned to this consumer."
  [^Consumer consumer]
  (set (.assignment consumer)))

(defn subscription
  "Get the current subscription for this consumer."
  [^Consumer consumer]
  (set (.subscription consumer)))

(defn close
  "Close the consumer or producer, waiting indefinitely for any needed cleanup."
  [^java.io.Closeable closable]
  (.close closable))

(defn committed
  "Get the last committed offset for the given partition."
  ^OffsetAndMetadata
  [^Consumer consumer ^String topic ^Integer partition]
  (.committed consumer (topic-partition topic partition)))

(defn commit-offsets-async!
  "Commit offsets returned by the last poll for all subscribed topics and partitions."
  ([^Consumer consumer] (.commitAsync consumer))
  ([^Consumer consumer ^OffsetCommitCallback callback]
   (.commitAsync consumer callback))
  ([^Consumer consumer offsets ^OffsetCommitCallback callback]
   (.commitAsync consumer offsets callback)))

(defn commit-offsets!
  "Commit offsets returned by the last poll for all subscribed topics and partitions.

  offsets - commit the specified offsets for the specified topics and partitions by providing a seq
  of vectors of the form [topic partition offset metadata], e.g.

    [[\"my-topic\" 0 307 \"my metadata string\"]
     [\"other-topic\" 1 231 \"other metadata string\"]]
  "
  ([^Consumer consumer] (.commitSync consumer))
  ([^Consumer consumer offsets]
   (let [m (into {} (for [[t p off met] offsets]
                      [(topic-partition t p) (offset-and-metadata off met)]))]
     (.commitSync consumer m))))

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
  "Return a seq of consumer records currently available to the consumer (via a single poll).
  Fetches sequetially from the last consumed offset.

  A consumer record is represented as a clojure map with corresponding keys :value, :key,
  :partition, :topic, :offset
  
  timeout - the time, in milliseconds, spent waiting in poll if data is not
  available. If 0, returns immediately with any records that are available now.
  Must not be negative."
  ([consumer] (poll consumer 100))
  ([consumer timeout]
   (->> (.poll consumer timeout)
        (map consumer-record->map)
        (seq))))

(defn records
  "Return a lazy sequence of sequences of consumer-records by polling the consumer.

  Each element in the returned sequence is the seq of consumer records returned from a
  poll by the consumer. The consumer fetches sequetially from the last consumed offset.

  A consumer record is represented as a clojure map with corresponding keys :value, :key,
  :partition, :topic, :offset

  timeout - the time, in milliseconds, spent waiting in poll if data is not
  available. If 0, returns immediately with any records that are available now.
  Must not be negative."
  ([^Consumer consumer] (records consumer 100))
  ([^Consumer consumer timeout] (repeatedly #(poll consumer timeout))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Kafka Producer
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn producer
  "Return a KafkaProducer.

  The producer is thread safe and sharing a single producer instance across
  threads will generally be faster than having multiple instances.

  Args:
    servers: comma-separated host:port strs or list of strs as bootstrap servers
    config: an optional map of str to str containing additional producer
            configuration. More info on optional config is available here:
            http://kafka.apache.org/documentation.html#newconsumerconfigs

  The StringSerializer class is the default for both key.serializer and value.serializer

  More info on settings is available here:
  http://kafka.apache.org/081/documentation.html#producerconfigs"
  ^KafkaProducer
  [servers & [config]]
  (-> config
      (assoc "bootstrap.servers" servers
             "key.serializer" str-serializer
             "value.serializer" str-serializer)
      (as-properties)
      (KafkaProducer.)))

(defn send-record
  "Returns a java.util.ConcurrentFuture."
  [^Producer producer topic value]
  (let [pr (ProducerRecord. topic value)]
    (.send producer pr)))


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
