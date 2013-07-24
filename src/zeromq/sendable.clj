(ns zeromq.sendable
  (:refer-clojure :exclude [send])
  (:require [zeromq.zmq :as zmq]))

(defprotocol Sendable
  (send
    [this socket flags]))

(extend-protocol Sendable
  String
  (send [this socket flags]
    (zmq/send socket (.getBytes ^String this) (int flags))))
