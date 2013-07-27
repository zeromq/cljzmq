(ns zeromq.zmq-test
  (:require [zeromq.zmq :as zmq]
            [zeromq.sendable :as s])
  (:use clojure.test))

(defonce context (zmq/zcontext))

(deftest push-pull-test
  (with-open [push (-> (zmq/socket context :push)
                       (zmq/connect "tcp://127.0.0.1:12349"))
              pull (-> (zmq/socket context :pull)
                       (zmq/bind "tcp://*:12349"))]
    (s/send "hello" push 0)
    (let [actual (String. (zmq/receive pull))]
      (is (= "hello" actual)))))
