(ns zeromq.zmq
  (:refer-clojure :exclude [send])
  (:import
   [org.zeromq ZContext ZMQ ZMQ$Context ZMQ$Poller ZMQ$Socket ZMQQueue]
   java.nio.ByteBuffer
   java.net.ServerSocket))

(def ^:const zmq-three?
  (> (ZMQ/getFullVersion) (ZMQ/makeVersion 3 0 0)))

(def ^:const bytes-type (class (byte-array 0)))

(def ^:const socket-options
  {:no-block (ZMQ/NOBLOCK)
   :dont-wait (ZMQ/DONTWAIT)
   :send-more (ZMQ/SNDMORE)})

(def ^:const socket-types
  {:pair (ZMQ/PAIR)
   :pub (ZMQ/PUB)
   :sub (ZMQ/SUB)
   :req (ZMQ/REQ)
   :rep (ZMQ/REP)
   :xreq (ZMQ/XREQ)
   :xrep (ZMQ/XREP)
   :dealer (ZMQ/DEALER)
   :router (ZMQ/ROUTER)
   :xpub (ZMQ/XPUB)
   :xsub (ZMQ/XSUB)
   :pull (ZMQ/PULL)
   :push (ZMQ/PUSH)})

(def ^:const poller-types
  {:pollin (ZMQ$Poller/POLLIN)
   :pollout (ZMQ$Poller/POLLOUT)
   :pollerr (ZMQ$Poller/POLLERR)})

(defn first-free-port []
  (with-open [ss (ServerSocket. 0)]
    (.getLocalPort ss)))

(defn ^ZMQ$Context context
  ([]
     (ZMQ/context 1))
  ([io-threads]
     (ZMQ/context (int io-threads))))

(defmulti socket (fn [a _] (class a)))

(defmethod ^ZMQ$Socket socket ZMQ$Context [^ZMQ$Context context socket-type]
  (if-let [type (socket-types socket-type)]
    (.socket context type)
    (throw (IllegalArgumentException. (format "Unknown socket type: %s" socket-type)))))

(defmethod ^ZMQ$Socket socket ZContext [^ZContext zcontext socket-type]
  (if-let [type (socket-types socket-type)]
    (.createSocket zcontext type)
    (throw (IllegalArgumentException. (format "Unknown socket type: %s" socket-type)))))

(defn ^ZMQ$Socket connect [^ZMQ$Socket socket address]
  (.connect socket address)
  socket)

(defn ^ZMQ$Socket disconnect [^ZMQ$Socket socket address]
  (.disconnect socket address)
  socket)

(defn ^ZMQ$Socket bind [^ZMQ$Socket socket address]
  (.bind socket address)
  socket)

(defn ^ZMQ$Socket unbind [^ZMQ$Socket socket address]
  (.unbind socket address)
  socket)

(defn bind-random-port
  "Bind to the first free port. Address should be of the form:
 <protocol>://address the port will automatically be added."
  [^ZMQ$Socket socket address]
  (let [port (first-free-port)]
    (bind socket (format "%s:%s" address port))
    port))

(defn receive
  "Receive method shall receive a message part from the socket and store it in the
   buffer argument.

   If there are no message parts available on the socket the receive method
   shall block until the request can be satisfied."
  (^bytes [^ZMQ$Socket socket]
     (.recv socket 0))
  (^bytes [^ZMQ$Socket socket flags]
     (.recv socket (int flags)))
  (^bytes [^ZMQ$Socket socket ^bytes buffer offset len flags]
     (.recv socket buffer (int offset) (int len) (int flags))))

(defn send
  "Send method shall queue a message part created from the buffer argument on the socket.

  A successful invocation of send does not indicate that the message has been
  transmitted to the network, only that it has been queued on the socket and ØMQ
  has assumed responsibility for the message."
  ([^ZMQ$Socket socket ^bytes buf]
     (send socket buf 0))
  ([^ZMQ$Socket socket ^bytes buf flags]
     (.send socket buf (int flags)))
  ([^ZMQ$Socket socket ^bytes buf offset length flags]
     (.send socket buf (int offset) (int length) (int flags))))

(defn send-byte-buffer
  ([^ZMQ$Socket socket ^ByteBuffer bb]
     (send-byte-buffer socket bb 0))
  ([^ZMQ$Socket socket ^ByteBuffer bb flags]
     (.sendByteBuffer socket bb (int flags))))

(defn recv-byte-buffer
  ([^ZMQ$Socket socket ^ByteBuffer bb]
     (recv-byte-buffer socket bb 0))
  ([^ZMQ$Socket socket ^ByteBuffer bb flags]
     (.recvByteBuffer socket bb (int flags))))

(defn ^ZMQ$Socket set-send-hwm [^ZMQ$Socket socket ^long size]
  (.setSndHWM socket size)
  socket)

(defn ^ZMQ$Socket set-recv-hwm [^ZMQ$Socket socket ^long size]
  (.setRcvHWM socket size)
  socket)

(defn ^ZMQ$Socket set-hwm [^ZMQ$Socket socket ^long size]
  (if zmq-three?
    (do (set-send-hwm socket size)
        (set-recv-hwm socket size))
    (.setHWM socket size))
  socket)

(defn receive-more? [^ZMQ$Socket socket]
  (.hasReceiveMore socket))

(defn recv-all [^ZMQ$Socket socket]
  (loop [acc (transient [])]
    (let [new-acc (conj! acc (receive socket))]
      (if (receive-more? socket)
        (recur new-acc)
        (persistent! new-acc)))))

(defn ^ZMQ$Socket set-linger [^ZMQ$Socket socket ^long linger-ms]
  (.setLinger socket linger-ms)
  socket)

(defn ^ZMQ$Socket set-identity [^ZMQ$Socket socket ^bytes identity]
  (.setIdentity socket identity)
  socket)

(defmulti subscribe (fn [_ b] (class b)))

(defmethod subscribe String [^ZMQ$Socket socket ^String topic]
  (.subscribe socket (.getBytes topic))
  socket)

(defmethod subscribe bytes-type [^ZMQ$Socket socket ^bytes topic]
  (.subscribe socket topic)
  socket)

(defmulti unsubscribe (fn [_ b] (class b)))

(defmethod unsubscribe String [^ZMQ$Socket socket ^String topic]
  (.unsubscribe socket (.getBytes topic))
  socket)

(defmethod unsubscribe bytes-type [^ZMQ$Socket socket ^bytes topic]
  (.unsubscribe socket topic)
  socket)

(defmulti close class)

(defmethod close ZContext
  [^ZContext zctx]
  (.close zctx))

(defmethod close ZMQ$Socket
  [^ZMQ$Socket s]
  (.close s))

(defn ^ZContext zcontext
  ([]
     (zcontext 1))
  ([io-threads]
     (let [^ZContext zctx (ZContext.)]
       (.setContext zctx (context io-threads))
       zctx)))

(defn destroy-socket [^ZContext zctx ^ZMQ$Socket socket]
  (.destroySocket zctx socket))

(defn destroy [^ZContext zctx]
  (.destroy zctx))

(defn set-router-mandatory [^ZMQ$Socket socket mandatory?]
  (.setRouterMandatory socket mandatory?)
  socket)

(defmulti poller (fn [x & xs] (class x)))

(defmethod poller ZMQ$Context
  ([^ZMQ$Context ctx]
     (.poller ctx 1))
  ([^ZMQ$Context ctx size]
     (.poller ctx (int size))))

(defmethod poller ZContext
  ([^ZContext zctx]
     (.poller ^ZMQ$Context (.getContext zctx) 1))
  ([^ZContext zctx size]
     (.poller ^ZMQ$Context (.getContext zctx) (int size))))

(defn register [^ZMQ$Poller poller ^ZMQ$Socket socket & events]
  (.register poller socket (int (apply bit-or 0 (keep poller-types events)))))

(defn unregister [^ZMQ$Poller poller ^ZMQ$Socket socket]
  (.unregister poller socket))

(defn poll
  ([^ZMQ$Poller poller]
     (.poll poller))
  ([^ZMQ$Poller poller ^long timeout]
     (.poll poller timeout)))

(defn check-poller
  [^ZMQ$Poller poller index type]
  (case type
    :pollin (.pollin poller (int index))
    :pollout (.pollout poller (int index))
    :pollerr (.pollerr poller (int index))))
