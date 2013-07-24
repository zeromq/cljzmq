(ns zeromq.device
  (:import
   [org.zeromq ZContext ZMQ$Socket ZMQQueue]))

(defn bidirectional-connection [^ZContext zctx ^ZMQ$Socket frontend ^ZMQ$Socket backend]
  (let [^ZMQQueue queue (ZMQQueue. (.getContext zctx) frontend backend)]
    (.start (Thread. ^Runnable queue))
    #(.close queue)))
