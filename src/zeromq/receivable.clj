(ns zeromq.receivable)

(defprotocol Receivable
  (receive [this socket flags]))
