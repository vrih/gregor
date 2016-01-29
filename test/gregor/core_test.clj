(ns gregor.core-test
  (:require [gregor.core :refer :all]
            [clojure.test :refer [is testing deftest]])
  (:import [org.apache.kafka.clients.consumer MockConsumer OffsetResetStrategy ConsumerRecords ConsumerRecord]
           [org.apache.kafka.clients.producer MockProducer]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.common.serialization StringSerializer]
           [java.util ArrayList]))


(deftest producing
  (testing "record->map and send-message")
  (let [p (MockProducer. true (StringSerializer.) (StringSerializer.))]
    (send-message p "unittest" {:a 1 :b "two"})
    (let [values (.history p)]
      (is (= {:a 1 :b "two"}
             (-> values
                 (first)
                 (record->map)
                 (:value))))
      (.close p))))

(deftest consuming
  (testing "messages-seq")
  (let [topics ["unittest"]
        tp (TopicPartition. "unittest" 0)
        c (doto (MockConsumer. (OffsetResetStrategy/EARLIEST))
            (.assign [tp])
            (.updateBeginningOffsets {(TopicPartition. "unittest" 0) 0})
            (.addRecord (ConsumerRecord. "unittest" 0 0 0 {:a 1}))
            (.addRecord (ConsumerRecord. "unittest" 0 0 0 {:b 2})))
        ms (messages-seq c)]
    (is (= {:a 1}
           (-> ms (first) (first) (:value))))
    (is (= {:b 2}
           (-> ms (first) (second) (:value))))))

