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
                 destination: Optional[bytes] = None, received_by: Optional[bytes] = ''):
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
        logger = getLogger(__name__)
        if msg_type not in self.VALID_TYPES:
            logger.error(f"Invalid message type attempted: {msg_type}")
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
        self.received_by = received_by or b''
        self._respond_lock = threading.Lock()

        logger.debug(f"MKTLMessage created: msg_type={msg_type}, key={key}, req_id={req_id}")

    def __repr__(self):
        return f'<MKTLMessage {self.coms}, {self.sender_id}, {self.msg_type}, {self.req_id}, {self.key}, {self.json_data}, {self.binary_blob}>'

    @classmethod
    def from_frames(cls, coms, msg: List[bytes], received_by: Optional[bytes] = None):
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
        logger = getLogger(__name__)
        logger.debug("Attempting to parse MKTLMessage from frames...")

        if not msg or len(msg) < 6 and not (msg[0] == b'' and len(msg) < 7):
            logger.error("Malformed message: insufficient frames")
            raise ValueError('Malformed message: insufficient frames')

        if msg[0] == b'':
            #  dest ident stripped by sending socket (ROUTER).
            #  if stripped by receiving socker (REP) null frame would also be gone
            # dest_id was us
            # null, message
            count = 7
            null, sender_id, null2, msg_type, req_id, key, json_payload = msg[:count]  # dealer to router
        else:
            # sender_id, null, message  delaer sent to router, routed added sender
            count = 6
            sender_id, null, msg_type, req_id, key, json_payload = msg[:count]
            if not null == b'':# and null2 == b'':
                logger.error("Malformed message: null frames in wrong places")
                raise ValueError("Malformed message: null frames in wrong places")

        # sender_id, dest_id, null, msg_type, req_id, key, json_payload = msg[:7]  #dealer to router
        # sender_id, null, msg_type, req_id, key, json_payload = msg[:7]  # router to router

        extra_frames = msg[count:] if len(msg) > count else []
        binary_blob = extra_frames[0] if extra_frames else None

        msg_type = msg_type.decode()
        key = key.decode()
        json_data = json.loads(json_payload.decode())

        message = cls(coms, sender_id, msg_type, req_id, key, json_data, binary_blob, received_by=received_by)
        logger.debug(f"Parsed frames into {message}")
        return message

    def to_frames(self) -> List[bytes]:
        """
        Build the ZeroMQ frame list for this message.

        Frames:
            [sender_id, "", msg_type, req_id, key, json, [blob]]

        Returns:
            List of byte strings to send with send_multipart().
        """
        #Router to dealer router requires destination id and will strip it
        #Dealer to router, router will prepend sender id

        if self.is_request():  #out over a dealer
            frames = [
                b'',
                self.msg_type.encode(),
                self.req_id,
                self.key.encode(),
                json.dumps(self.json_data).encode()
            ]
        else:  #out over a router
            frames = [
                self.destination,
                b'',
                self.sender_id,
                b'',
                self.msg_type.encode(),
                self.req_id,
                self.key.encode(),
                json.dumps(self.json_data).encode()
            ]
        if self.binary_blob:
            frames.append(self.binary_blob)
        return frames

    @staticmethod
    def try_parse(coms, msg: List[bytes], received_by: Optional[bytes] = None) -> Tuple[Optional['MKTLMessage'], Optional[bytes], Optional[str]]:
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
        logger = getLogger(__name__)
        # logger.debug(f'Trying to parse frames:\n   '+',\n   '.join(map(str, msg)))
        try:
            m = MKTLMessage.from_frames(coms, msg, received_by=received_by)
            return m, None, None
        except Exception as e:
            logger.error(f"Failed to parse MKTLMessage: {e}")
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
        logger = getLogger(__name__)
        logger.debug(f"Attempting ack for {self}")
        with self._respond_lock:
            if not self.responded:
                self._enqueue('ack', {'pending': True}, destination=self.sender_id)
                logger.info(f"ACK sent for {self}")

    def respond(self, value, binary_blob: Optional[bytes] = None):
        """
        Send a full 'response' message to the requester.

        Includes a JSON-serializable payload and an optional binary blob.

        Args:
            value: JSON-compatible return value.
            binary_blob: Optional raw bytes for frame 6.
        """
        logger = getLogger(__name__)
        with self._respond_lock:
            if not self.responded:
                logger.debug(f"Attempting respond {self}")
                self._enqueue('response', value, binary_blob=binary_blob, destination=self.sender_id)
                self.responded = True
                logger.info(f"Response queued for {self}")
            else:
                logger.debug(f"Already responded for {self}")

    def fail(self, error_msg):
        """
        Send an 'error' message in response to a failed request.

        Args:
            error_msg: Text string describing the error condition.
        """
        logger = getLogger(__name__)
        logger.debug(f"Attempting fail {self}")
        with self._respond_lock:
            if not self.responded:
                self._enqueue('error', {'error': error_msg}, destination=self.sender_id)
                self.responded = True
                logger.warning(f"Error sent for message {self}, error={error_msg}")
            else:
                logger.debug(f"Already responded for {self}")

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
        logger = getLogger(__name__)
        self.msg_type = msg_type
        self.json_data = payload
        self.binary_blob = binary_blob
        if destination is not None:
            self.destination = destination
        logger.debug(f"Enqueueing {self}")
        self.coms._send_queue.put(self)

    def has_blob(self):
        """
        Returns True if the message includes a binary frame.
        """
        return self.binary_blob is not None


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
        logger = getLogger(__name__)
        logger.debug("Creating MKTLSubscription instance...")
        self._queue = queue.Queue()
        self._closed = False

    def put(self, msg: MKTLMessage):
        """
        Add a message to the internal queue for consumption.

        Used internally by MKTLComs when a subscribed message arrives.
        """
        logger = getLogger(__name__)
        logger.debug(f"Putting message into MKTLSubscription queue for key={msg.key}")
        if not self._closed:
            self._queue.put(msg)
        else:
            logger.warning("Attempted to put message into a closed subscription.")

    def close(self):
        """
        Mark this subscription as closed. Future calls to `get()` will stop blocking.

        Used to cleanly terminate long-lived subscription loops.
        """
        logger = getLogger(__name__)
        logger.info("Closing MKTLSubscription.")
        self._closed = True

    def __iter__(self):
        """
        Yield all messages placed in the subscription queue.

        Blocks until the subscription is closed or an error occurs.
        """
        logger = getLogger(__name__)
        logger.debug("Starting iteration over MKTLSubscription messages.")
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
        logger = getLogger(__name__)
        logger.debug("Constructing MKTLComs instance...")
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
        if registry_addr:
            self._routing_table['registry.owner'] = ('registry', registry_addr)
            self._routing_table['registry.config'] = ('registry', registry_addr)

        self._connected_addresses: Set[str] = set()

        self._shutdown_callback = shutdown_callback

        if authoritative_keys:
            for k, h in authoritative_keys.items():
                self.register_key_handler(k, h)
        self._register_internal_handlers()

        logger.info(f"MKTLComs created with identity={self.identity}, registry_addr={self.registry_addr}")

    def __repr__(self):
        return f'MKTLComs(identity={self.identity}, registry_addr={self.registry_addr})'

    def _register_internal_handlers(self):
        """
        Register built-in handlers for mKTL internal keys.

        This includes `.mktl_control` and other diagnostic/control endpoints.
        Called automatically at startup.
        """
        logger = getLogger(__name__)
        logger.debug("Registering internal mktl_control handler...")
        self.register_key_handler(f'{self.identity}.mktl_control', self._handle_control_message)

    def _handle_control_message(self, msg):
        """
        Route and process incoming mKTL control messages.

        Handles `get`, `set`, `ack`, `response`, and `error` messages.
        Separates routing logic and handler dispatch.
        """
        logger = getLogger(__name__)
        logger.info(f'Control message received: {msg.json_data}')
        msg.respond('ACK')
        if msg.json_data['value'] == 'shutdown' and self._shutdown_callback is not None:
            logger.warning("Shutdown command received via mktl_control.")
            self._shutdown_callback()

    def _send_registry_config(self):
        """
        Announce this node's authoritative keys to the registry service.

        Encodes the identity, address, and known keys into a StoreConfig payload
        and sends it as a `set` to `registry.config`.
        """
        logger = getLogger(__name__)
        logger.debug("Attempting to send registry config...")

        keys = list(self.authoritative_keys.keys())
        user_keys = list(keys)
        user_keys.remove(f'{self.identity}.mktl_control')
        if not user_keys:
            logger.debug("No authoritative keys to announce (besides mktl_control).")
            return

        payload = {'identity': self.identity,
                   'address': self._bind_address,
                   'keys': keys,
                   }

        self.set('registry.config', payload)

        # ctx = zmq.Context.instance()
        # s = ctx.socket(zmq.DEALER)
        # s.setsockopt(zmq.IDENTITY, self.identity.encode())
        # s.connect(self.registry_addr)
        # req_id = uuid.uuid4().bytes
        #
        # frames = [b'registry', b'', b'set', req_id, b'registry.config', json.dumps(payload).encode()
        #           ]
        # s.send_multipart(frames)
        # logger.info("Registry config set message sent.")
        # s.close()

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
        logger = getLogger(__name__)
        if not self.registry_addr:
            logger.warning('Registry address not configured.')
            return None

        result = self.get('registry.owner', {'key': key})
        identity = result.json_data.get('identity')
        address = result.json_data.get('address')
        if identity and address:
            self._routing_table[key] = (identity, address)
            logger.info(f"Registry resolution for {key}: identity={identity}, address={address}")
            return identity, address
        else:
            logger.warning(f"Could not resolve owner for key={key}")
            return None

        # ctx = zmq.Context.instance()
        # s = ctx.socket(zmq.DEALER)
        # temp_id = f'query-{uuid.uuid4().hex[:8]}'
        # s.setsockopt(zmq.IDENTITY, temp_id.encode())
        # s.connect(self.registry_addr)
        # poller = zmq.Poller()
        # poller.register(s, zmq.POLLIN)
        #
        # req_id = uuid.uuid4().bytes
        # json_data = json.dumps({'key': key}).encode()
        #
        # frames = [
        #     b'registry', b'', b'get', req_id,
        #     b'registry.owner', json_data
        # ]
        # s.send_multipart(frames)
        # logger.debug(f"Sent registry owner query for key={key}")
        #
        # socks = dict(poller.poll(timeout=2000))
        # if s in socks:
        #     reply = s.recv_multipart()
        #     logger.debug(f'Received multipart message on {s}')
        #     _, message_type, req_id, key, json_data = reply[:5]
        #     payload = json.loads(json_data.decode())
        #     identity = payload.get('identity')
        #     address = payload.get('address')
        #     if identity and address:
        #         self._routing_table[key.decode()] = (identity, address)
        #         logger.info(f"Registry resolution for {key.decode()}: identity={identity}, address={address}")
        #         return identity, address
        #
        # logger.warning(f"Could not resolve owner for key={key}")
        # return None

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
        logger = getLogger(__name__)
        if destination is not None:
            logger.debug(f"Destination override provided for key={key}: {destination}")
            return destination

        identity, address = self._routing_table.get(key, (None, None))
        if identity is None:
            logger.debug(f"No local routing info for key={key}; querying registry.")
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
        logger = getLogger(__name__)
        logger.info(f"Setting binding ROUTER socket address to {address}")
        self._bind_address = address

    def _connect_for_key(self, key: str):
        """
        Connect the DEALER socket for sending mKTL requests to the identity.

        Args:
            key: The key for which a connections is required.
        """
        logger = getLogger(__name__)
        identity, address = self._routing_table[key]
        if address not in self._connected_addresses:
            self._dealer.connect(address)
            self._connected_addresses.add(address)
            logger.info(f"Dealer connected to {address} for key={key}")

    def register_key_handler(self, key: str, handler: Callable):
        """
        Register a key handler dynamically after MKTLComs has been created.

        This augments the `authoritative_keys` passed to `__init__`.

        Args:
            key: Key this handler responds to.
            func: Function that accepts MKTLMessage and optionally returns a result.
        """
        self.authoritative_keys[key] = handler
        getLogger(__name__).debug(f"Registered key handler for {key}")

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
        logger = getLogger(__name__)
        logger.info("Starting MKTLComs communication threads.")
        self._running = True

        t = threading.Thread(target=self._serve_loop, daemon=True)
        logger.info('Starting serve loop thread')
        t.start()
        self._threads.append(t)

        if self._pub_address:
            t_pub = threading.Thread(target=self._publish_loop, daemon=True)
            logger.info('Starting publish loop thread')
            t_pub.start()
            self._threads.append(t_pub)

        if self._sub_address:
            t_sub = threading.Thread(target=self._listen_loop, daemon=True)
            logger.info('Starting listen loop thread')
            t_sub.start()
            self._threads.append(t_sub)

        if self.registry_addr:
            logger.info('Sending StoreConfig to registry')
            self._send_registry_config()

    def stop(self):
        """
        Cleanly shut down all communication threads and close sockets.
        """
        logger = getLogger(__name__)
        logger.info("Stopping MKTLComs...")
        self._running = False
        for t in self._threads:
            t.join()
        logger.debug("All communication threads joined.")

    def get(self, key: str, payload: Any, timeout: float = 2000.0, destination: Optional[str] = None) -> MKTLMessage:
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
        getLogger(__name__).debug(f"Initiating 'get' request for key={key}, destination={destination}")
        resolved = self._resolve_destination(key, destination)
        return self._send_request('get', key, payload, timeout, None, resolved)

    def set(self, key: str, value: Any, timeout: float = 2000.0, binary_blob: Optional[bytes] = None,
            destination: Optional[str] = None) -> MKTLMessage:
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
        getLogger(__name__).debug(f"Initiating 'set' request for key={key}, value={value}, destination={destination}")
        resolved = self._resolve_destination(key, destination)
        return self._send_request('set', key, value, timeout, binary_blob, destination=resolved)

    def _send_request(self, msg_type: str, key: str, payload: dict, timeout: float, binary_blob: Optional[bytes] = None,
                      destination: Optional[str] = None) -> MKTLMessage:
        """
        Build and transmit a control request to another node, blocking for response.

        Handles request framing, timeout logic, and error propagation.
        """
        logger = getLogger(__name__)
        if self._dealer is None:
            logger.error("Cannot send request because MKTLComs is not started.")
            raise RuntimeError('MKTLComs must be started before using get/set')

        req_id = uuid.uuid4().bytes
        logger.debug(f"Creating MKTLMessage for request type={msg_type}, key={key}")
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
        self._pending_replies[req_id]=None
        self._send_queue.put(m)

        start_time = time.time()
        while time.time() - start_time < timeout:
            with self._client_lock:
                rpl = self._pending_replies[req_id]
                if rpl is not None:
                    msg_obj = self._pending_replies.pop(req_id)
                    if msg_obj.msg_type == 'ack':
                        logger.info(f"ACK reply for request {req_id}")
                        self._pending_replies[req_id]=None
                        continue
                    elif msg_obj.msg_type == 'error':
                        logger.info(f"ERROR reply for request {req_id}: {msg_obj.json_data}")
                    else:
                        logger.info(f"RESPONSE reply for request {req_id}: {msg_obj.json_data}")
                    return msg_obj
            time.sleep(0.01)

        logger.error(f"Timeout waiting for {msg_type} response to key={key}")
        raise TimeoutError(f'Timeout waiting for {msg_type} response to key: {key}')

    def _handle_message(self, m: MKTLMessage):
        """
        Internal dispatcher for incoming control messages.

        Routes incoming `get` or `set` requests to the correct registered handler
        based on key match, or responds with an error if unmatched.
        """
        logger = getLogger(__name__)
        logger.debug(f'Handling message for key: {m.key}, type: {m.msg_type}')

        if m.key in self.authoritative_keys:
            try:
                result = self.authoritative_keys[m.key](m)
                if result is not None:
                    m.respond(result)
                logger.debug(f'Sent response for key: {m.key}')
            except Exception as e:
                logger.error(f"Exception in handler for key={m.key}: {e}", exc_info=True)
                m.fail(str(e))
                logger.warning(f'Sent error response for key: {m.key}')
        elif m.key.endswith('.mktl_control'):
            m.ack()
            logger.debug(f'Sent ack for key: {m.key}')
        else:
            m.fail('Unknown key')
            logger.warning(f'Sent error response for key: {m.key}')

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
        logger = getLogger(__name__)
        logger.info(f"Requesting subscription to prefix={key_prefix}")
        with self._pending_lock:
            self._pending_subscriptions.add(key_prefix)

        if callback:
            self._sub_callbacks.setdefault(key_prefix, []).append(callback)
            logger.debug(f"Callback registered for prefix={key_prefix}")

        listener = MKTLSubscription()
        self._sub_listeners.setdefault(key_prefix, []).append(listener)
        logger.debug(f"Created MKTLSubscription for prefix={key_prefix}")
        return listener

    def _serve_loop(self):
        """
        Main server thread loop for receiving ROUTER messages.

        Handles control-plane traffic: requests, responses, and registry lookups.
        """
        logger = getLogger(__name__)
        logger.debug(f'Starting server loop for {self.identity} at {self._bind_address}')
        self._router = self._ctx.socket(zmq.ROUTER)
        self._router.setsockopt(zmq.IDENTITY, self.identity.encode())
        if self._bind_address:
            logger.info('ROUTER socket created and bound')
            self._router.bind(self._bind_address)

        self._dealer = self._ctx.socket(zmq.DEALER)
        logger.info('DEALER socket created and connected')
        self._dealer.setsockopt(zmq.IDENTITY, self.identity.encode())

        poller = zmq.Poller()
        poller.register(self._router, zmq.POLLIN)
        poller.register(self._dealer, zmq.POLLIN)

        while self._running:
            events = dict(poller.poll(timeout=10))

            for sock in (self._router, self._dealer):
                if sock in events and events[sock] == zmq.POLLIN:
                    msg = sock.recv_multipart()
                    logger.debug(f'Received multipart message on {sock}')
                    m, sender_id, err = MKTLMessage.try_parse(self, msg, received_by=sock.identity)
                    if err:
                        logger.warning(f'Received malformed message from {sender_id}: {err}')
                        # frames = [
                        #     sender_id, b'', b'error',
                        #     uuid.uuid4().bytes, b'',
                        #     json.dumps({'error': f'Malformed message: {err}'}).encode()
                        # ]
                        # sock.send_multipart(frames)
                    elif m.is_reply():
                        logger.debug(f'Received reply for request {m.req_id}')
                        with self._client_lock:
                            if m.req_id in self._pending_replies:
                                self._pending_replies[m.req_id] = m
                            else:
                                logger.warning(f'Received reply for unknown request: {m.req_id}')
                    else:
                        logger.debug(f'Received message: {m}')
                        try:
                            m.respond(self._handle_message(m))
                        except Exception as e:
                            m.fail(e)
                            logger.error(f'Exception while handling message: {e}', exc_info=True)

            try:
                while True:
                    item = self._send_queue.get_nowait()
                    frames = item.to_frames()
                    if item.is_reply():
                        logger.debug(f'Sending with router: {item}')#\n' + '   ,\n'.join(map(str, frames)))
                        self._router.send_multipart(frames)
                    else:
                        self._connect_for_key(item.key)
                        logger.debug(f'Sending with dealer: {item}')#\n' + '   ,\n'.join(map(str, frames)))
                        self._dealer.send_multipart(frames)
            except queue.Empty:
                pass

        self._router.close()
        self._dealer.close()

    def _listen_loop(self):
        """
        Subscription thread main loop.

        Listens for PUB messages and dispatches them to callbacks and iterators.
        Also applies any deferred subscriptions from `subscribe()`.
        """
        logger = getLogger(__name__)
        self._sub_socket = self._ctx.socket(zmq.SUB)
        logger.info('SUB socket created for telemetry listening')
        self._sub_socket.setsockopt(zmq.LINGER, 0)
        self._sub_socket.connect(self._sub_address)  # assumes this is set via connect_pub()
        logger.debug(f'Connected SUB socket to {self._sub_address}')

        while self._running:
            with self._pending_lock:
                for prefix in self._pending_subscriptions:
                    self._sub_socket.setsockopt(zmq.SUBSCRIBE, prefix.encode())
                    logger.debug(f"Subscribed to prefix={prefix}")
                self._pending_subscriptions.clear()

            try:
                msg_parts = self._sub_socket.recv_multipart()
                logger.debug('Received multipart message on SUB socket')
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
                                    logger.error(f'Callback failed for key {key}: {e}', exc_info=True)

                    for prefix, queues in self._sub_listeners.items():
                        if key.startswith(prefix):
                            for listener in queues:
                                listener.put(msg)
            except Exception as e:
                logger.error(f'Error in _listen_loop: {e}', exc_info=True)

    def _publish_loop(self):
        """
        Background thread target that sends queued telemetry publications.

        This method creates and binds the PUB socket, then continuously
        dequeues and broadcasts any telemetry messages posted via publish().
        """
        logger = getLogger(__name__)
        if self._pub_address is None:
            logger.warning("No PUB address set; publish loop will exit.")
            return

        socket = self._ctx.socket(zmq.PUB)
        logger.info('PUB socket created for telemetry publishing')
        socket.bind(self._pub_address)
        logger.info(f'Bound PUB socket to {self._pub_address}')
        while True:
            try:
                key, payload, binary = self._publish_queue.get()
                if key is None and payload is None and binary is None:
                    logger.debug("Publish loop received termination sentinel.")
                    break

                if binary is not None:
                    frames = [f'bulk:{key}'.encode(), json.dumps(payload).encode(), binary]
                else:
                    frames = [key.encode(), json.dumps(payload).encode()]

                socket.send_multipart(frames)
                logger.debug(f'Sent multipart message: {key}')
            except Exception as e:
                logger.error(f"Error in _publish_loop: {e}", exc_info=True)
                raise   #TODO

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
        getLogger(__name__).debug(f'Queueing message for key: {key}')
        self._publish_queue.put((key, payload, binary_blob))

    def bind_pub(self, address: str):
        """
        Declare the PUB socket bind address. The actual socket will be created
        and bound by _publish_loop() within the background thread started by start().

        Args:
            address: ZeroMQ address string (e.g., 'tcp://*:5560')
        """
        getLogger(__name__).info(f"Binding PUB socket to {address}")
        self._pub_address = address

    def connect_sub(self, address: str):
        """
        Set the address of the SUB socket for a telemetry stream.

        Args:
            address: Address of a PUB or XPUB socket to connect to.
        """
        getLogger(__name__).info(f"Connecting SUB socket to {address}")
        self._sub_address = address
