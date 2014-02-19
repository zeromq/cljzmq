(ns zeromq.zmq-test
  (:require [zeromq.zmq :as zmq]
            [zeromq.sendable :as s])
  (:use clojure.test))

(defonce context (zmq/zcontext))

(deftest push-pull-test
  (with-open [push (doto (zmq/socket context :push)
                     (zmq/connect "tcp://127.0.0.1:12349"))
              pull (doto (zmq/socket context :pull)
                     (zmq/bind "tcp://*:12349"))]
    (s/send "hello" push 0)
    (let [actual (String. (zmq/receive pull))]
      (is (= "hello" actual)))))


(deftest receive-str-timeout-test
  (with-open [pull (doto (zmq/socket context :pull)
                     (zmq/set-receive-timeout 100)
                     (zmq/bind "tcp://*:12310"))]
    (let [actual (zmq/receive-str pull)]
      (is (= nil actual)))))


(deftest dealer-router-test
  (with-open [dealer (doto (zmq/socket context :dealer)
                       (zmq/set-receive-timeout 3000)
                       (zmq/bind "tcp://*:12350"))
              router (doto (zmq/socket context :router)
                       (zmq/connect "tcp://127.0.0.1:12350"))]
    (s/send "myid" dealer zmq/send-more)
    (s/send "payload" dealer 0)
    (let [[id & _] (zmq/receive-all router)]
      (zmq/send router id zmq/send-more)
      (zmq/send-str router "ack")
      (let [actual (zmq/receive-str dealer)]
        (is (= "ack" actual))))))
