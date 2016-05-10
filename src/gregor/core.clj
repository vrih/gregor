(ns gregor.core
  (:import [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.clients.consumer Consumer KafkaConsumer ConsumerRecords
            ConsumerRecord OffsetAndMetadata OffsetCommitCallback
            ConsumerRebalanceListener]
           [org.apache.kafka.clients.producer Producer KafkaProducer Callback
            ProducerRecord])
  (:require [clojure.string :as str]))


(def ^:no-doc str-deserializer "org.apache.kafka.common.serialization.StringDeserializer")
(def ^:no-doc str-serializer "org.apache.kafka.common.serialization.StringSerializer")

(defn- as-properties
  [m]
  (let [ps (java.util.Properties.)]
    (doseq [[k v] m] (.setProperty ps k v))
    ps))

(defn topic-partition
  "A topic name and partition number."
  ^TopicPartition
  [^String topic ^Integer partition]
  (TopicPartition. topic partition))

(defn- arg-pairs
  [fn-name p1 p2 pairs]
  (let [pairs (remove nil? pairs)]
    (if (even? (count pairs))
      (->> pairs
           (concat [p1 p2])
           (partition 2))
      (throw (IllegalArgumentException.
              (str fn-name
                   " expects even number of optional args, found odd number."))))))

(defn- ->tps
  [fn-name topic partition tps]
  (let [pairs (arg-pairs fn-name topic partition tps)]
    (->> pairs
         (map #(apply topic-partition %))
         (into-array TopicPartition))))

(defn- reify-crl
  [assigned-cb revoked-cb]
  (reify ConsumerRebalanceListener
    (onPartitionsAssigned [this partitions]
      (assigned-cb partitions))
    (onPartitionsRevoked [this partitions]
      (revoked-cb partitions))))

(defn offset-and-metadata
  "Metadata for when an offset is committed."
  ([^Long offset] (OffsetAndMetadata. offset))
  ([^Long offset ^String metadata] (OffsetAndMetadata. offset metadata)))

(defn consumer-record->map
  [^ConsumerRecord record]
  {:value     (.value record)
   :key       (.key record)
   :partition (.partition record)
   :topic     (.topic record)
   :offset    (.offset record)})

(defn subscribe
  "Subscribe to the given list of topics to get dynamically assigned partitions. Topic
  subscriptions are not incremental. This list will replace the current assignment (if
  there is one). It is not possible to combine topic subscription with group management
  with manual partition assignment through assign(List). If the given list of topics is
  empty, it is treated the same as unsubscribe.

  topics-or-regex can be a list of topic names or a java.util.regex.Pattern object to
  subscribe to all topics matching a specified pattern.

  the optional functions are a callback interface to trigger custom actions when the set
  of partitions assigned to the consumer changes.
  "
  [^Consumer consumer topics-or-regex & [partitions-assigned-fn partitions-revoked-fn]]
  (.subscribe consumer topics-or-regex
              (reify-crl partitions-assigned-fn partitions-revoked-fn)))

(defn unsubscribe
  "Unsubscribe from topics currently subscribed with subscribe. This also clears any
  partitions directly assigned through assign."
  [^Consumer consumer]
  (.unsubscribe consumer))

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
    (when (not-empty topics) (subscribe kc topics))
     kc))

(defn assign!
  "Manually assign topic-partition pairs to this consumer."
  [^Consumer consumer ^String topic ^Integer partition & tps]
  (->> tps
       (->tps "assign!" topic partition)
       (vec)
       (.assign consumer)))

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
  "Return OffsetAndMetadata of the last committed offset for the given partition. This
  offset will be used as the position for the consumer in the event of a failure."
  ^OffsetAndMetadata
  [^Consumer consumer ^String topic ^Integer partition]
  (.committed consumer (topic-partition topic partition)))

(defn- reify-occ
  [cb]
  (reify OffsetCommitCallback
    (onComplete [this offsets-map ex]
      (cb offsets-map ex))))

(defn commit-offsets-async!
 "Commit offsets returned by the last poll for all subscribed topics and partitions,
  or manually specify offsets to commit.

  This is an asynchronous call and will not block. Any errors encountered are either
  passed to the callback (if provided) or discarded.
  
  offsets (optional) - commit a map of offsets by partition with associate metadata.
  e.g. {(topic-partition 'my-topic' 1) (offset-and-metadata 1)
        (topic-partition 'other-topic' 2) (offset-and-metadata 67 'such meta')}

  The committed offset should be the next message your application will consume,
  i.e. lastProcessedMessageOffset + 1.

  This map will be copied internally, so it is safe to mutate the map after returning."
  ([^Consumer consumer] (.commitAsync consumer))
  ([^Consumer consumer callback]
   (.commitAsync consumer (reify-occ callback)))
  ([^Consumer consumer offsets callback]
   (.commitAsync consumer offsets (reify-occ callback))))

(defn commit-offsets!
  "Commit offsets returned by the last poll for all subscribed topics and partitions.

  offsets (optional) - commit a map of offsets by partition with associated metadata.
  e.g. {(topic-partition 'my-topic' 1) (offset-and-metadata 1)
        (topic-partition 'other-topic' 2) (offset-and-metadata 67 'so metadata')}
  "
  ([^Consumer consumer] (.commitSync consumer))
  ([^Consumer consumer offsets]
   (let [m (into {} (for [[t p off met] offsets]
                      [(topic-partition t p)
                       (offset-and-metadata off met)]))]
     (.commitSync consumer m))))

(defn seek!
  "Overrides the fetch offsets that the consumer will use on the next poll."
  [^Consumer consumer topic partition offset]
  (.seek consumer (topic-partition topic partition) offset))

(defn seek-to!
  "Seek to the :beginning or :end offset for each of the given partitions."
  [consumer offset topic partition & tps]
  (assert (contains? #{:beginning :end} offset) "offset must be :beginning or :end")
  (let [tps (->tps "seek-to!" topic partition tps)]
    (case offset
      :beginning (.seekToBeginning consumer tps)
      :end (.seekToEnd consumer tps))))

(defn position
  "Return the offset of the next record that will be fetched (if a record with that
  offset exists)."
  [^Consumer consumer topic partition]
  (.position consumer (topic-partition topic partition)))

(defn pause
  "Suspend fetching for a seq of topic name, partition number pairs."
  [^Consumer consumer topic partition & tps]
  (->> tps
       (->tps "pause" topic partition)
       (.pause consumer)))

(defn resume
  "Resume specified partitions which have been paused."
  [^Consumer consumer topic partition & tps]
  (->> tps
       (->tps "resume" topic partition)
       (.resume consumer)))

(defn wakeup
  "Wakeup the consumer. This method is thread-safe and is useful in particular to abort a
  long poll. The thread which is blocking in an operation will throw WakeupException."
  [^Consumer consumer]
  (.wakeup consumer))

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

(defn send
  "Asynchronously send a record to a topic, return a java.util.concurrent.Future"
  [^Producer producer topic value & [callback]]
  (let [pr (ProducerRecord. topic value)]
    (if callback
      (.send producer pr (reify Callback
                           (onCompletion [this metadata ex]
                             (callback metadata ex))))
      (.send producer pr))))

(defn flush
  "Invoking this method makes all buffered records immediately available to send (even if
  linger.ms is greater than 0) and blocks on the completion of the requests associated
  with these records."
  [^Producer producer]
  (.flush producer))
