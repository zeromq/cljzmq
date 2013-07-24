(ns zeromq.sendable
  (:refer-clojure :exclude [send])
  (:require [zeromq.zmq :as zmq]))

(defprotocol Sendable
  (send
    [this socket]
    [this socket flags]))

(extend-protocol Sendable
  String
  (send [this socket flags]
    (zmq/send socket (.getBytes ^String this) (int flags)))
  (send [this socket]
    (send this socket 0)))
