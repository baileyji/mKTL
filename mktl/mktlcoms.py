"""mktlcoms.py

Implements the MKTLComs communication layer for the mKTL protocol.

This module defines a decentralized messaging system built on ZeroMQ, supporting
request/response, publish/subscribe, and registry-based service discovery.

Typical usage:

    coms = MKTLComs(identity="my.service", authoritative_keys={"my.key": my_handler})
    coms.bind("tcp://*:5571")
    coms.bind_pub("tcp://*:5572")
    coms.connect_registry("tcp://registry:5570")
    coms.start()

    val, blob = coms.get("another.service.key")
"""
from logging import getLogger
import itertools
import zmq
import threading
import uuid
import json
import queue
import time
import logging
from typing import Callable, Dict, Optional, Any, List, Tuple, Union, Set


class MKTLMessage:
    VALID_TYPES = {'get', 'set', 'ack', 'response', 'error', 'publish'}
    REPLY_TYPES = {'ack', 'response', 'error'}
    REQUEST_TYPES = {'get', 'set'}

    def __init__(self, coms, sender_id, msg_type, req_id, key, json_data, binary_blob=None,
                 destination: Optional[bytes] = None):
        """
        Initialize a new mKTL communications object.

        Args:
            identity: A globally unique string name for this process.
            authoritative_keys: An optional dictionary of key handlers that this node claims ownership of.
            context: Optional ZeroMQ context for socket management.

        This constructor sets up internal data structures, stores key handlers, and prepares
        for future socket binding or connection. Actual communication threads are started
        via `start()`, not automatically on init.
        """
        if msg_type not in self.VALID_TYPES:
            raise ValueError(f'Invalid message type: {msg_type}')

        self.coms = coms
        self.sender_id = sender_id
        self.msg_type = msg_type
        self.req_id = req_id
        self.key = key
        self.json_data = json_data
        self.binary_blob = binary_blob
        self.responded = False
        self.destination = destination or b''

    def __repr__(self):
        return f'<MKTLMessage {self.coms}, {self.sender_id}, {self.msg_type}, {self.req_id}, {self.key}, {self.json_data}, {self.binary_blob}>'

    @classmethod
    def from_frames(cls, coms, msg: List[bytes]):
        """
        Construct a MKTLMessage from a list of 5 or 6 ZeroMQ frames.

        This includes decoding the routing envelope, message type, request ID,
        key name, and the JSON body. Frame 6, if present, is stored as a binary blob.

        Args:
            coms: The parent MKTLComs instance.
            msg: List of frames received via ROUTER or SUB socket.

        Returns:
            An MKTLMessage instance.
        """
        if not msg or len(msg) < 6:
            raise ValueError('Malformed message: insufficient frames')

        sender_id, dest_id, _, msg_type, req_id, key, json_payload = msg[:7]
        extra_frames = msg[7:] if len(msg) > 7 else []
        binary_blob = extra_frames[0] if extra_frames else None

        msg_type = msg_type.decode()
        key = key.decode()
        json_data = json.loads(json_payload.decode())

        return cls(coms, sender_id, msg_type, req_id, key, json_data, binary_blob)

    @staticmethod
    def try_parse(coms, msg: List[bytes]) -> Tuple[Optional['MKTLMessage'], Optional[bytes], Optional[str]]:
        """
        Attempt to parse a multipart message into an MKTLMessage.

        Returns a tuple of (message, router_identity, error). If parsing fails, the
        message and router_identity will be None, and error will contain a string reason.

        Args:
            coms: The parent MKTLComs instance.
            msg: List of ZeroMQ frames.

        Returns:
            Tuple of (MKTLMessage or None, router_identity or None, error string or None)
        """
        try:
            m = MKTLMessage.from_frames(coms, msg)
            return m, None, None
        except Exception as e:
            sender_id = msg[0] if len(msg) >= 1 else None
            return None, sender_id, str(e)

    def is_reply(self):
        """
        Return True if the message type is in REPLY_TYPES
        """
        return self.msg_type in self.REPLY_TYPES

    def is_request(self):
        """
        Return True if the message type is in REQUEST_TYPES
        """
        return self.msg_type in self.REQUEST_TYPES

    def ack(self):
        """
        Send an immediate 'ack' response to the requester.

        This acknowledges receipt of a 'get' or 'set' and signals
        that a full response will follow.
        """
        if not self.responded:
            self._enqueue('ack', {'pending': True}, destination=self.sender_id)

    def respond(self, value, binary_blob: Optional[bytes] = None):
        """
        Send a full 'response' message to the requester.

        Includes a JSON-serializable payload and an optional binary blob.

        Args:
            value: JSON-compatible return value.
            binary_blob: Optional raw bytes for frame 6.
        """
        if not self.responded:
            self._enqueue('response', {'value': value}, binary_blob, destination=self.sender_id)
            self.responded = True

    def fail(self, error_msg):
        """
        Send an 'error' message in response to a failed request.

        Args:
            error_msg: Text string describing the error condition.
        """
        if not self.responded:
            self._enqueue('error', {'error': error_msg}, destination=self.sender_id)
            self.responded = True

    def _enqueue(self, msg_type: str, payload: dict, binary_blob: Optional[bytes] = None,
                 destination: Optional[bytes] = None):
        """
        Internal helper to format and enqueue a reply message.

        Used by `ack`, `respond`, and `fail` to prepare outbound frames.

        Args:
            msg_type: One of 'ack', 'response', or 'error'.
            payload: Dictionary to encode into the JSON body.
            binary_blob: Optional raw bytes to include as a final frame.
        """
        self.msg_type = msg_type
        self.json_data = payload
        self.binary_blob = binary_blob
        if destination is not None:
            self.destination = destination
        self.coms._send_queue.put(self)

    def has_blob(self):
        """
        Returns True if the message includes a binary frame.
        """
        return self.binary_blob is not None

    def get_frames(self) -> List[bytes]:
        """
        Build the ZeroMQ frame list for this message.

        Frames:
            [sender_id, "", msg_type, req_id, key, json, [blob]]

        Returns:
            List of byte strings to send with send_multipart().
        """
        frames = [
            self.destination,
            b'',
            self.msg_type.encode(),
            self.req_id,
            self.key.encode(),
            json.dumps(self.json_data).encode()
        ]
        if self.binary_blob:
            frames.append(self.binary_blob)
        return frames


class MKTLSubscription:

    def __init__(self):
        """
        Initialize a new mKTL communications object.

        Args:
            identity: A globally unique string name for this process.
            authoritative_keys: An optional dictionary of key handlers that this node claims ownership of.
            context: Optional ZeroMQ context for socket management.

        This constructor sets up internal data structures, stores key handlers, and prepares
        for future socket binding or connection. Actual communication threads are started
        via `start()`, not automatically on init.
        """
        self._queue = queue.Queue()
        self._closed = False

    def put(self, msg: MKTLMessage):
        """
        Add a message to the internal queue for consumption.

        Used internally by MKTLComs when a subscribed message arrives.
        """
        if not self._closed:
            self._queue.put(msg)

    def close(self):
        """
        Mark this subscription as closed. Future calls to `get()` will stop blocking.

        Used to cleanly terminate long-lived subscription loops.
        """
        self._closed = True

    def __iter__(self):
        """
        Yield all messages placed in the subscription queue.

        Blocks until the subscription is closed or an error occurs.
        """
        while not self._closed:
            try:
                msg = self._queue.get(timeout=0.5)
                yield msg
            except queue.Empty:
                continue


class MKTLComs:
    """
    The core interface for mKTL-based communication.

    MKTLComs enables both client and server behaviors using a unified asynchronous
    messaging interface. It allows services to bind to ZeroMQ sockets, publish
    telemetry, respond to `get` and `set` requests, and discover peers via an optional
    registry service.

    The class also manages threading, message framing, routing logic, and registry
    announcement via StoreConfig.
    """

    def __init__(self, identity: Optional[str] = None, authoritative_keys: Optional[Dict[str, Callable]] = None,
                 registry_addr: Optional[str] = None, shutdown_callback: Optional[Callable] = None):
        """
        Initialize a new mKTL communications object.

        Args:
            identity: A globally unique string name for this process.
            authoritative_keys: An optional dictionary of key handlers that this node claims ownership of.
            context: Optional ZeroMQ context for socket management.

        This constructor sets up internal data structures, stores key handlers, and prepares
        for future socket binding or connection. Actual communication threads are started
        via `start()`, not automatically on init.
        """
        self.identity = identity or f'mktl-{uuid.uuid4().hex[:8]}'
        self.authoritative_keys = {}
        self.registry_addr = registry_addr

        self._ctx = zmq.Context.instance()
        self._running = False
        self._threads = []

        self._bind_address = None
        # self._connect_addresses = []
        self._send_queue = queue.Queue()

        self._pub_socket = None
        self._pub_address = None
        self._publish_queue = queue.Queue()

        self._router = None
        self._dealer = None
        self._sub_socket = None
        self._sub_address = None

        self._client_lock = threading.Lock()
        self._pending_replies = {}

        self._pending_subscriptions = set()
        self._pending_lock = threading.Lock()

        self._sub_callbacks: Dict[str, List[Callable]] = {}
        self._sub_listeners: Dict[str, List[MKTLSubscription]] = {}

        self._routing_table: Dict[str, Tuple[str, str]] = {}  # key -> (identity, address)
        self._connected_addresses: Set[str] = set()

        self._shutdown_callback = shutdown_callback

        if authoritative_keys:
            for k, h in authoritative_keys.items():
                self.register_key_handler(k, h)
        self._register_internal_handlers()


    def _register_internal_handlers(self):
        """
        Register built-in handlers for mKTL internal keys.

        This includes `.mktl_control` and other diagnostic/control endpoints.
        Called automatically at startup.
        """
        self.register_key_handler(f'{self.identity}.mktl_control', self._handle_control_message)

    def _handle_control_message(self, msg):
        """
        Route and process incoming mKTL control messages.

        Handles `get`, `set`, `ack`, `response`, and `error` messages.
        Separates routing logic and handler dispatch.
        """
        logging.info(f'Control message received: {msg.json_data}')
        msg.respond('ACK')
        if msg.json_data['value'] == 'shutdown' and self._shutdown_callback is not None:
            self._shutdown_callback()

    def _send_registry_config(self):
        """
        Announce this node's authoritative keys to the registry service.

        Encodes the identity, address, and known keys into a StoreConfig payload
        and sends it as a `set` to `registry.config`.
        """
        ctx = zmq.Context.instance()
        s = ctx.socket(zmq.DEALER)
        s.setsockopt(zmq.IDENTITY, self.identity.encode())
        s.connect(self.registry_addr)

        req_id = uuid.uuid4().bytes
        keys = list(self.authoritative_keys.keys())
        payload = {'identity': self.identity,
                   'address': self._bind_address,
                   'keys': keys,
                   }

        # frames1= MKTLMessage(
        #     coms=self,
        #     sender_id=self.identity.encode(),
        #     msg_type='set',
        #     req_id=req_id,
        #     key='registry.config',
        #     json_data=payload,
        #     binary_blob=None,
        #     destination=b'registry').get_frames()
        frames = [b'registry', b'', b'set', req_id, b'registry.config', json.dumps(payload).encode()
                  ]
        s.send_multipart(frames)
        s.close()

    def _query_registry_owner(self, key: str) -> Optional[Tuple[str, str]]:
        """
        Query the registry for the owner of a specific key.

        Sends a `get` request to `registry.owner` and expects a response
        with `identity` and `address`.

        Args:
            key: The key to resolve.

        Returns:
            Tuple of (identity, address), or raises on failure.
        """
        if not self.registry_addr:
            logging.warning('Registry address not configured.')
            return None

        ctx = zmq.Context.instance()
        s = ctx.socket(zmq.DEALER)
        temp_id = f'query-{uuid.uuid4().hex[:8]}'
        s.setsockopt(zmq.IDENTITY, temp_id.encode())
        s.connect(self.registry_addr)

        req_id = uuid.uuid4().bytes
        json_data = json.dumps({'key': key}).encode()

        frames = [
            b'registry', b'', b'get', req_id,
            b'registry.owner', json_data
        ]
        s.send_multipart(frames)

        poller = zmq.Poller()
        poller.register(s, zmq.POLLIN)
        socks = dict(poller.poll(timeout=2000))
        if s in socks:
            reply = s.recv_multipart()
            if len(reply) >= 6:
                payload = json.loads(reply[5].decode())
                identity = payload.get('identity')
                address = payload.get('address')
                if identity and address:
                    self._routing_table[key] = (identity, address)
                    return identity, address

        return None

    def _resolve_destination(self, key: str, destination: Optional[str]) -> str:
        """
        Determine the correct identity to route a request to.

        Uses local overrides or queries the registry as needed.

        Args:
            key: The key to be resolved.
            destination: Optional explicit override.

        Returns:
            String identity of the target service.
        """
        if destination is not None:
            return destination

        identity, _ = self._routing_table.get(key, (None, None))
        if identity is None:
            resolved = self._query_registry_owner(key)
            if not resolved:
                raise RuntimeError(f'Could not resolve destination for key: {key}')
            identity, _ = resolved
        return identity

    def _load_keys_for_prefix(self, prefix: str):
        """
        Query the registry for all keys served by a particular identity.

        Used to support dynamic discovery and telemetry filtering.

        Args:
            identity: Node to query for its authoritative keys.

        Returns:
            List of keys owned by that identity.
        """
        pass

    def bind(self, address: str):
        """
        Bind the ROUTER socket for handling mKTL requests.

        Must be called before `start()`.

        Args:
            address: A ZeroMQ bind address (e.g., 'tcp://*:5571').
        """
        self._bind_address = address

    def _connect_for_key(self, key: str):
        """
        Connect the DEALER socket for sending mKTL requests to the identity.

        Args:
            key: The key for which a connections is required.
        """
        identity, address = self._routing_table[key]
        if address not in self._connected_addresses:
            self._dealer.connect(address)
            self._connected_addresses.add(address)

    def register_key_handler(self, key: str, handler: Callable):
        """
        Register a key handler dynamically after MKTLComs has been created.

        This augments the `authoritative_keys` passed to `__init__`.

        Args:
            key: Key this handler responds to.
            func: Function that accepts MKTLMessage and optionally returns a result.
        """
        self.authoritative_keys[key] = handler

    def on(self, key: str):
        """
        Register a handler function for a specific key.

        This decorator enables a natural way to associate `get`/`set` logic with an mKTL key.
        Intended for use by daemons exposing state or control endpoints.

        Example:
            @coms.on("guiders.state")
            def handle_state(msg): ...

        Args:
            key: The key this handler should respond to.

        Returns:
            A decorator that registers the wrapped function.
        """
        def wrapper(fn):
            self.register_key_handler(key, fn)
            return fn
        return wrapper

    def start(self):
        """
        Start the internal communication and publication loops.

        This method launches background threads to manage ROUTER/DEALER,
        PUB, and SUB socket processing. It should be called after all
        bind/connect addresses have been declared.
        """
        self._running = True

        t = threading.Thread(target=self._serve_loop, daemon=True)
        t.start()
        self._threads.append(t)

        if self._pub_address:
            t_pub = threading.Thread(target=self._publish_loop, daemon=True)
            t_pub.start()
            self._threads.append(t_pub)

        if self._sub_address:
            t_sub = threading.Thread(target=self._listen_loop, daemon=True)
            t_sub.start()
            self._threads.append(t_sub)

        if self.registry_addr:
            self._send_registry_config()

    def _serve_loop(self):
        """
        Main server thread loop for receiving ROUTER messages.

        Handles control-plane traffic: requests, responses, and registry lookups.
        """
        getLogger(__name__).debug(f'Starting server loop for {self.identity} at {self._bind_address}')
        self._router = self._ctx.socket(zmq.ROUTER)
        self._router.setsockopt(zmq.IDENTITY, self.identity.encode())
        if self._bind_address:
            self._router.bind(self._bind_address)

        self._dealer = self._ctx.socket(zmq.DEALER)
        self._dealer.setsockopt(zmq.IDENTITY, self.identity.encode())

        poller = zmq.Poller()
        poller.register(self._router, zmq.POLLIN)
        poller.register(self._dealer, zmq.POLLIN)

        while self._running:
            events = dict(poller.poll(timeout=10))

            for sock in (self._router, self._dealer):
                if sock in events and events[sock] == zmq.POLLIN:
                    msg = sock.recv_multipart()
                    m, sender_id, err = MKTLMessage.try_parse(self, msg)
                    getLogger(__name__).debug(f'Received message: {m}, {sender_id}, {err}')
                    if m:
                        try:
                            self._handle_message(m)
                        except Exception as e:
                            logging.error(f'Exception while handling message: {e}', exc_info=True)
                    elif sender_id:
                        frames = [
                            sender_id,
                            b'',
                            b'error',
                            uuid.uuid4().bytes,
                            b'',
                            json.dumps({'error': f'Malformed message: {err}'}).encode()
                        ]
                        self._router.send_multipart(frames)

            try:
                while True:
                    item = self._send_queue.get_nowait()
                    if item.msg_type in ('ack', 'response', 'error'):
                        getLogger(__name__).debug(f'Sending with router: {item}')
                        self._router.send_multipart([self.identity.encode()]+item.get_frames())
                    else:
                        self._connect_for_key(item.key)
                        getLogger(__name__).debug(f'Sending with dealer: {item}')
                        self._dealer.send_multipart([b'']+item.get_frames())
            except queue.Empty:
                pass

        self._router.close()
        self._dealer.close()

    # def _req_id_gen(self):
    #     max_id = 1000000
    #     min_id = 0
    #     gen = itertools.count(min_id)
    #     lock = threading.Lock()
    #     while True:
    #         with lock:
    #             req_id = next(gen)
    #             if req_id>max_id:
    #                 gen = itertools.count(min_id)
    #                 req_id = next(gen)
    #             yield req_id

    def stop(self):
        """
        Cleanly shut down all communication threads and close sockets.
        """
        self._running = False
        for t in self._threads:
            t.join()

    def get(self, key: str, timeout: float = 2.0, destination: Optional[str] = None) -> Any:
        """
        Send a `get` request to another node and wait for its response.

        This method blocks for the given timeout and will attempt to resolve the key's
        destination either directly or via the registry.

        Args:
            key: The fully-qualified mKTL key to retrieve.
            timeout: Timeout in seconds for the operation.
            destination: Optional identity override for direct routing.

        Returns:
            A tuple (value, binary_blob) from the response.
        """
        resolved = self._resolve_destination(key, destination)
        return self._send_request('get', key, {}, timeout, None, resolved)

    def set(self, key: str, value: Any, timeout: float = 2.0, binary_blob: Optional[bytes] = None,
            destination: Optional[str] = None):
        """
        Send a `set` request with a value and optional binary payload.

        Used to update state or trigger actions on another service. Will resolve
        routing and manage framing transparently.

        Args:
            key: The mKTL key to modify.
            value: JSON-serializable object to send.
            timeout: Maximum time to wait for acknowledgment and response.
            binary_blob: Optional bytes payload (frame 6).
            destination: Override automatic routing.

        Returns:
            A tuple (value, binary_blob) from the response.
        """
        resolved = self._resolve_destination(key, destination)
        return self._send_request('set', key, {'value': value}, timeout, binary_blob, resolved)

    def _send_request(self, msg_type: str, key: str, payload: dict, timeout: float, binary_blob: Optional[bytes] = None,
                      destination: Optional[str] = None):
        """
        Build and transmit a control request to another node, blocking for response.

        Handles request framing, timeout logic, and error propagation.
        """
        if self._dealer is None:
            raise RuntimeError('MKTLComs must be started before using get/set')

        req_id = uuid.uuid4().bytes
        m = MKTLMessage(
            coms=self,
            sender_id=self.identity.encode(),
            msg_type=msg_type,
            req_id=req_id,
            key=key,
            json_data=payload,
            binary_blob=binary_blob,
            destination=destination.encode() if destination else b''
        )
        self._send_queue.put(m)

        start_time = time.time()
        while time.time() - start_time < timeout:
            with self._client_lock:
                if req_id in self._pending_replies:
                    m = self._pending_replies.pop(req_id)
                    if m.msg_type == 'error':
                        raise RuntimeError(m.json_data.get('error', 'Unknown error'))
                    return m.json_data.get('value'), m.binary_blob if m.has_blob() else None
            time.sleep(0.01)

        raise TimeoutError(f'Timeout waiting for {msg_type} response to key: {key}')

    def _handle_message(self, m: MKTLMessage):
        """
        Internal dispatcher for incoming control messages.

        Routes incoming `get` or `set` requests to the correct registered handler
        based on key match, or responds with an error if unmatched.
        """
        if m.key in self.authoritative_keys:
            try:
                result = self.authoritative_keys[m.key](m)
                if result is not None:
                    m.respond(result)
            except Exception as e:
                m.fail(str(e))
        elif m.key.endswith('.mktl_control'):
            m.ack()
        else:
            m.fail('Unknown key')

    def subscribe(self, key_prefix: str, callback: Optional[Callable] = None) -> MKTLSubscription:
        """
        Subscribe to messages whose topic starts with a given prefix.

        This enables both callback-based and iterator-based consumption of published data.

        Args:
            key_prefix: Topic prefix to subscribe to.
            callback: Optional function to call with each received message.

        Returns:
            MKTLSubscription object for manual message retrieval.
        """
        with self._pending_lock:
            self._pending_subscriptions.add(key_prefix)

        if callback:
            self._sub_callbacks.setdefault(key_prefix, []).append(callback)

        listener = MKTLSubscription()
        self._sub_listeners.setdefault(key_prefix, []).append(listener)
        return listener

    def _listen_loop(self):
        """
        Subscription thread main loop.

        Listens for PUB messages and dispatches them to callbacks and iterators.
        Also applies any deferred subscriptions from `subscribe()`.
        """
        self._sub_socket = self._ctx.socket(zmq.SUB)
        self._sub_socket.setsockopt(zmq.LINGER, 0)
        self._sub_socket.connect(self._sub_address)  # assumes this is set via connect_pub()

        while self._running:
            with self._pending_lock:
                for prefix in self._pending_subscriptions:
                    self._sub_socket.setsockopt(zmq.SUBSCRIBE, prefix.encode())
                self._pending_subscriptions.clear()

            try:
                msg_parts = self._sub_socket.recv_multipart()
                if len(msg_parts) >= 2:
                    key = msg_parts[0].decode()
                    payload = json.loads(msg_parts[1].decode())
                    blob = msg_parts[2] if len(msg_parts) > 2 else None
                    msg = MKTLMessage(self, b'', 'publish', b'', key, payload, blob)

                    for prefix, callbacks in self._sub_callbacks.items():
                        if key.startswith(prefix):
                            for cb in callbacks:
                                try:
                                    cb(msg)
                                except Exception as e:
                                    logging.error(f'Callback failed for key {key}: {e}')

                    for prefix, queues in self._sub_listeners.items():
                        if key.startswith(prefix):
                            for listener in queues:
                                listener.put(msg)
            except Exception as e:
                logging.error(f'Error in _listen_loop: {e}')

    def _publish_loop(self):
        """
        Background thread target that sends queued telemetry publications.

        This method creates and binds the PUB socket, then continuously
        dequeues and broadcasts any telemetry messages posted via publish().
        """
        if self._pub_address is None:
            return

        socket = self._ctx.socket(zmq.PUB)
        socket.bind(self._pub_address)
        while True:
            try:
                key, payload, binary = self._publish_queue.get()

                if binary is not None:
                    frames = [f'bulk:{key}'.encode(), json.dumps(payload).encode(), binary]
                else:
                    frames = [key.encode(), json.dumps(payload).encode()]

                socket.send_multipart(frames)
            except Exception:
                raise  ##TODO

    def publish(self, key: str, payload: dict, binary_blob: Optional[bytes] = None):
        """
        Thread-safe publish interface for telemetry or binary data.

        Enqueues the provided key and payload for broadcasting via the PUB socket.
        The actual ZeroMQ socket is owned by the background _publish_loop thread.

        Args:
            key: Full keypath (e.g., 'adc.temperature') or 'bulk:keyname' for binary.
            payload: JSON-serializable dictionary.
            binary_blob: Optional bytes object to send as the final frame.
        """
        self._publish_queue.put((key, payload, binary_blob))

    def bind_pub(self, address: str):
        """
        Declare the PUB socket bind address. The actual socket will be created
        and bound by _publish_loop() within the background thread started by start().

        Args:
            address: ZeroMQ address string (e.g., 'tcp://*:5560')
        """
        self._pub_address = address

    def connect_sub(self, address: str):
        """
        Set the address of the SUB socket for a telemetry stream.

        Args:
            address: Address of a PUB or XPUB socket to connect to.
        """
        self._sub_address = address
