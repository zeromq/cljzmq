(ns zeromq.receivable
  (:require [zeromq.zmq :as zmq]))

(defprotocol Receivable
  (receive
    [this socket]
    [this socket flags]))

(extend-protocol Receivable
  String
  (receive [this socket flags]
    (-> (zmq/receive socket flags)
        #(String. %)))
  (receive [this socket]
    (receive this socket 0)))
