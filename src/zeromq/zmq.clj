(ns zeromq.zmq
  (:refer-clojure :exclude [send])
  (:import
   [org.zeromq ZContext ZMQ ZMQ$Context ZMQ$Poller ZMQ$Socket ZMQQueue]
   java.nio.ByteBuffer
   java.net.ServerSocket))

(def ^:const zmq-three?
  (> (ZMQ/getFullVersion) (ZMQ/makeVersion 3 0 0)))

(def ^:const bytes-type (class (byte-array 0)))

(def ^:const no-block (ZMQ/NOBLOCK))

(def ^:const dont-wait (ZMQ/DONTWAIT))

(def ^:const send-more (ZMQ/SNDMORE))

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

(defn first-free-port
  "Returns first free ephemeral port"
  []
  (with-open [ss (ServerSocket. 0)]
    (.getLocalPort ss)))

(defn ^ZMQ$Context context
  "The context function initialises a new ØMQ context.

   The io_threads argument specifies the size of the ØMQ thread pool to handle
   I/O operations. If your application is using only the inproc transport for
   messaging you may set this to zero, otherwise set it to at least one."
  ([]
     (ZMQ/context 1))
  ([io-threads]
     (ZMQ/context (int io-threads))))

(defmulti socket
  "The socket function shall create a ØMQ socket within the specified
   context. The type argument specifies the socket type, which determines the
   semantics of communication over the socket.

   The newly created socket is initially unbound, and not associated with any
   endpoints. In order to establish a message flow a socket must first be
   connected to at least one endpoint with connect, or at least one endpoint
   must be created for accepting incoming connections with bind."
  (fn [a _] (class a)))

(defmethod ^ZMQ$Socket socket ZMQ$Context [^ZMQ$Context context socket-type]
  (if-let [type (socket-types socket-type)]
    (.socket context type)
    (throw (IllegalArgumentException. (format "Unknown socket type: %s" socket-type)))))

(defmethod ^ZMQ$Socket socket ZContext [^ZContext zcontext socket-type]
  (if-let [type (socket-types socket-type)]
    (.createSocket zcontext type)
    (throw (IllegalArgumentException. (format "Unknown socket type: %s" socket-type)))))

(defn ^ZMQ$Socket connect
  "The connect function connects the socket to an endpoint and then accepts
   incoming connections on that endpoint.

   The endpoint is a string consisting of a transport :// followed by an
   address. The transport specifies the underlying protocol to use. The address
   specifies the transport-specific address to connect to.

   ØMQ provides the the following transports: tcp, ipc, inproc, pgm/epgm."

  [^ZMQ$Socket socket endpoint]
  (.connect socket endpoint)
  socket)

(defn ^ZMQ$Socket disconnect
  "The disconnect function shall disconnect a socket specified by the socket
  argument from the endpoint specified by the endpoint argument."
  [^ZMQ$Socket socket endpoint]
  (.disconnect socket endpoint)
  socket)

(defn ^ZMQ$Socket bind
  "The bind function binds the socket to an endpoint and then accepts
   incoming connections on that endpoint.

   The endpoint is a string consisting of a transport :// followed by an
   address. The transport specifies the underlying protocol to use. The address
   specifies the transport-specific address to bind to.

   ØMQ provides the the following transports: tcp, ipc, inproc, pgm/epgm"
  [^ZMQ$Socket socket endpoint]
  (.bind socket endpoint)
  socket)

(defn ^ZMQ$Socket unbind [^ZMQ$Socket socket endpoint]
  (.unbind socket endpoint)
  socket)

(defn bind-random-port
  "Bind to the first free port. Endpoint should be of the form
   <transport>://address the port will automatically be added."
  [^ZMQ$Socket socket endpoint]
  (let [port (first-free-port)]
    (bind socket (format "%s:%s" endpoint port))
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

(defn receive-str
  ([^ZMQ$Socket socket]
     (String. (.recv socket 0)))
  ([^ZMQ$Socket socket flags]
     (String. (.recv socket (int flags)))))

(defn send
  "Send method shall queue a message part created from the buffer argument on
   the socket.

   A successful invocation of send does not indicate that the message has been
   transmitted to the network, only that it has been queued on the socket and
   ØMQ has assumed responsibility for the message."
  ([^ZMQ$Socket socket ^bytes buf]
     (.send socket buf 0))
  ([^ZMQ$Socket socket ^bytes buf flags]
     (.send socket buf (int flags)))
  ([^ZMQ$Socket socket ^bytes buf offset length flags]
     (.send socket buf (int offset) (int length) (int flags))))

(defn send-str
  ([^ZMQ$Socket socket ^String message]
     (.send socket (.getBytes message) 0))
  ([^ZMQ$Socket socket ^String message flags]
     (.send socket (.getBytes message) (int flags))))

(defn ^ZMQ$Socket set-send-hwm
  [^ZMQ$Socket socket ^long size]
  (.setSndHWM socket size)
  socket)

(defn ^ZMQ$Socket set-recv-hwm
  [^ZMQ$Socket socket ^long size]
  (.setRcvHWM socket size)
  socket)

(defn ^ZMQ$Socket set-hwm
  [^ZMQ$Socket socket ^long size]
  (if zmq-three?
    (do (set-send-hwm socket size)
        (set-recv-hwm socket size))
    (.setHWM socket size))
  socket)

(defn receive-more?
  "The receive-more? function shall return true if the message part last
   received from the socket was a data part with more parts to follow. If there
   are no data parts to follow, this function shall return false."
  [^ZMQ$Socket
   socket]
  (.hasReceiveMore socket))

(defn receive-all
  "Receive all data parts from the socket."
  [^ZMQ$Socket socket]
  (loop [acc (transient [])]
    (let [new-acc (conj! acc (receive socket))]
      (if (receive-more? socket)
        (recur new-acc)
        (persistent! new-acc)))))

(defn ^ZMQ$Socket set-linger
  "The linger option shall set the linger period for the specified socket. The
   linger period determines how long pending messages which have yet to be sent
   to a peer shall linger in memory after a socket is closed with close, and
   further affects the termination of the socket's context with close. The
   following outlines the different behaviours:

   The default value of -1 specifies an infinite linger period. Pending messages
   shall not be discarded after a call to close; attempting to terminate the
   socket's context with close shall block until all pending messages have been
   sent to a peer.

   The value of 0 specifies no linger period. Pending messages shall be
   discarded immediately when the socket is closed with close.

   Positive values specify an upper bound for the linger period in
   milliseconds. Pending messages shall not be discarded after a call to close;
   attempting to terminate the socket's context with close shall block until
   either all pending messages have been sent to a peer, or the linger period
   expires, after which any pending messages shall be discarded."
  [^ZMQ$Socket socket ^long linger-ms]
  (.setLinger socket linger-ms)
  socket)

(defn ^ZMQ$Socket set-identity
  "The identity option shall set the identity of the specified socket. Socket
   identity is used only by request/reply pattern. Namely, it can be used in
   tandem with ROUTER socket to route messages to the peer with specific
   identity.

   Identity should be at least one byte and at most 255 bytes long. Identities
   starting with binary zero are reserved for use by ØMQ infrastructure.

   If two peers use the same identity when connecting to a third peer, the
   results shall be undefined."
  [^ZMQ$Socket socket ^bytes identity]
  (.setIdentity socket identity)
  socket)

(defmulti subscribe
  "The subscribe option shall establish a new message filter on a SUB
   socket. Newly created SUB sockets shall filter out all incoming messages,
   therefore you should call this option to establish an initial message filter.

   An empty option_value of length zero shall subscribe to all incoming
   messages. A non-empty option_value shall subscribe to all messages beginning
   with the specified prefix. Multiple filters may be attached to a single SUB
   socket, in which case a message shall be accepted if it matches at least one
   filter."
  (fn [_ b] (class b)))

(defmethod subscribe String [^ZMQ$Socket socket ^String topic]
  (.subscribe socket (.getBytes topic))
  socket)

(defmethod subscribe bytes-type [^ZMQ$Socket socket ^bytes topic]
  (.subscribe socket topic)
  socket)

(defmulti unsubscribe
  "The unsubscribe option shall remove an existing message filter on a SUB
   socket. The filter specified must match an existing filter previously
   established with the subscribe option. If the socket has several instances of
   the same filter attached the unsubscribe option shall remove only one
   instance, leaving the rest in place and functional."
  (fn [_ b] (class b)))

(defmethod unsubscribe String [^ZMQ$Socket socket ^String topic]
  (.unsubscribe socket (.getBytes topic))
  socket)

(defmethod unsubscribe bytes-type [^ZMQ$Socket socket ^bytes topic]
  (.unsubscribe socket topic)
  socket)

(defmulti close
  "Shall attempt to call the close option on either a ZContext or Socket"
  class)

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

(defn set-router-mandatory
  "Sets the ROUTER socket behavior when an unroutable message is encountered. A
   value of 0 is the default and discards the message silently when it cannot be
   routed. A value of 1 returns an EHOSTUNREACH error code if the message cannot
   be routed."
  [^ZMQ$Socket socket mandatory?]
  (.setRouterMandatory socket mandatory?)
  socket)

(defmulti poller
  "The poller function provides a mechanism for applications to multiplex
   input/output events in a level-triggered fashion over a set of sockets."
  (fn [x & xs] (class x)))

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
