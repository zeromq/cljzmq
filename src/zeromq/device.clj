(ns zeromq.device
  (:refer-clojure :exclude [proxy])
  (:require [zeromq.zmq :as zmq])
  (:import
   [org.zeromq ZContext ZMQ$Socket]))

(defn forward
  "Forward messages from the frontend socket to the backend and optionally to a
   capture socket"
  [^ZMQ$Socket frontend ^ZMQ$Socket backend ^ZMQ$Socket capture]
  (loop [part (zmq/receive frontend)
         more? (zmq/receive-more? frontend)]
    (zmq/send backend part (if more? zmq/send-more 0))
    (when capture
      (zmq/send capture part (if more? zmq/send-more 0)))
    (when more?
      (recur (zmq/receive frontend)
             (zmq/receive-more? frontend)))))

(defn proxy
  "The proxy function starts the built-in ØMQ proxy in the current application
   thread.

   The proxy connects a frontend socket to a backend socket. Conceptually, data
   flows from frontend to backend. Depending on the socket types, replies may
   flow in the opposite direction. The direction is conceptual only; the proxy
   is fully symmetric and there is no technical difference between frontend and
   backend.

   Before calling proxy you must set any socket options, and connect or bind
   both frontend and backend sockets. The two conventional proxy models are:

   proxy runs in the current thread and returns only if/when the current context
   is closed."
  ([^ZContext context ^ZMQ$Socket frontend ^ZMQ$Socket backend]
     (proxy context frontend backend nil))
  ([^ZContext context ^ZMQ$Socket frontend ^ZMQ$Socket backend ^ZMQ$Socket capture]
     (let [poller (zmq/poller context 2)]
       (zmq/register poller frontend :pollin)
       (zmq/register poller backend :pollin)
       (while (not (.. Thread currentThread isInterrupted))
         (zmq/poll poller 200)
         (cond
          (zmq/check-poller poller 0 :pollin)
          (forward frontend backend capture)
          (zmq/check-poller poller 1 :pollin)
          (forward backend frontend capture))))))
