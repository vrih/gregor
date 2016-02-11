(ns gregor.core-test
  (:require [gregor.core :refer :all]
            [clojure.test :refer [is testing deftest]])
  (:import [org.apache.kafka.clients.consumer MockConsumer OffsetResetStrategy ConsumerRecords ConsumerRecord]
           [org.apache.kafka.clients.producer MockProducer]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.common.serialization StringSerializer]
           [java.util ArrayList]))


(deftest producing
  (testing "consumer-record->map and send-message")
  (let [p (MockProducer. true (StringSerializer.) (StringSerializer.))]
    (send-message p "unittest" {:a 1 :b "two"})
    (let [values (.history p)]
      (is (= {:a 1 :b "two"}
             (-> values
                 (first)
                 (.value))))
      (.close p))))

(deftest subscribing
  (let [conf  {"bootstrap.servers" "localhost:9092"
               "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
               "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"}
        c (consumer conf ["unittest"] "testgroup")]
    (is (= #{"unittest"} (subscription c)))
    (close c)))

(deftest consuming
  (testing "messages, topic-partition")
  (let [c (MockConsumer. (OffsetResetStrategy/EARLIEST))
        _ (assign! c "unittest" 0)
        c (doto c
            (.updateBeginningOffsets {(topic-partition "unittest" 0) 0})
            (.addRecord (ConsumerRecord. "unittest" 0 0 0 {:a 1}))
            (.addRecord (ConsumerRecord. "unittest" 0 0 0 {:b 2})))
        ms (messages c)]
    (is (= {:a 1}
           (-> ms (first) (first) (:value))))
    (is (= {:b 2}
           (-> ms (first) (second) (:value))))
    (is (= #{(topic-partition "unittest" 0)}
           (assignment c)))
    (close c)))



;; (deftest commit
;;   (let [c (MockConsumer. (OffsetResetStrategy/EARLIEST))
;;         _ (assign! c "unittest" 0)
;;         c (doto c
;;             (.updateBeginningOffsets {(topic-partition "unittest" 0) 0})
;;             (.addRecord (ConsumerRecord. "unittest" 0 0 0 {:a 1})))
;;         ms (take 1 (messages c))]
;;     (commit-offsets! c)
;;     (is (= (offset-and-metadata 0) (.committed c (topic-partition "unittest" 0))))))


