"""mktlcoms.py

Implements the MKTLComs communication layer for the mKTL protocol.

This module defines a decentralized messaging system built on ZeroMQ, supporting
request/response, publish/subscribe, and registry-based service discovery.

Typical usage:

    coms = MKTLComs(identity="my.service", authoritative_keys={"my.key": my_handler})
    coms.bind_pub("tcp://*:5572")
    coms.start()

    val, blob = coms.get("another.service.key")
"""
from collections import defaultdict
from dataclasses import dataclass
from logging import getLogger
import zmq
import threading
import uuid
from uuid import UUID
import json
import queue
import time
from typing import Callable, Dict, Optional, Any, List, Tuple, Union, Set
from pyre import Pyre


@dataclass
class MKTLPeer:
    name: str
    uuid: UUID

    def __init__(self, name, uuid):
        self.name = name.decode(errors='ignore') if isinstance(name, bytes) else name
        self.uuid = UUID(bytes=uuid) if isinstance(uuid, bytes) else uuid

    def __str__(self):
        return f"{self.name} ({self.uuid})"


class MKTLMessage:
    VALID_TYPES = {'get', 'set', 'ack', 'response', 'config', 'error', 'publish'}
    REPLY_TYPES = {'ack', 'response', 'error'}
    REQUEST_TYPES = {'get', 'set'}

    def __init__(self,  msg_type, req_id:UUID | bytes, key, json_data, binary_blob=None,
                 sender:Optional[MKTLPeer]=None, destination: Optional[UUID] = None,
                 received_by: Optional[MKTLPeer] = None, coms: Optional["MKTLComs"] = None):
        """
        Represents a message in the system, encapsulating various data including sender, type,
        request identifier, and more.

        It validates the message type against allowed types and initializes required
        attributes. This class also facilitates thread-safe response handling by using
        a lock mechanism.

        Args:
            msg_type (str): The type of the message must be one of the valid types defined in `VALID_TYPES`.
            req_id (str): A unique identifier for the request associated with the message.
            key (str): A key relevant to the contents or handling of the message.
            json_data (dict): JSON-encoded data as part of the message body.
            binary_blob (bytes, optional): A binary large object as part of the message body.
            sender_id (MKTLPeer): The sender identifier of the message.
            destination (UUID | None, optional): The destination address or identifier for the message.
            received_by (MKTLPeer, optional): The identifier of the recipient or handler.
            coms (MKTLComs, optional): The communication interface used for message transmission, without ack/error/resp
        """
        logger = getLogger(__name__)
        if msg_type not in self.VALID_TYPES:
            logger.error(f"Invalid message type attempted: {msg_type}")
            raise ValueError(f'Invalid message type: {msg_type}')

        self.coms = coms
        self.sender = sender
        self.msg_type = msg_type
        if msg_type is 'publish':
            self.req_id = None
        else:
            self.req_id = req_id if isinstance(req_id, UUID) else UUID(bytes=req_id)
        self.key = key
        self.json_data = json_data
        self.binary_blob = binary_blob
        self.responded = False
        self.acknowledged = False
        self.destination = destination or b''
        self.received_by = received_by or b''
        self._respond_lock = threading.Lock()

        logger.debug(f"MKTLMessage created: msg_type={msg_type}, key={key}, req_id={req_id}")

    def __repr__(self):
        return (f'<MKTLMessage {self.msg_type}, {self.req_id}, {self.key}, {self.json_data}, '
                f'blob={len(self.binary_blob)/1024 if self.binary_blob is not None else 0} KiB, '
                f'sender={self.sender}, coms={self.coms}>')

    def has_blob(self):
        """
        Returns True if the message includes a binary frame.
        """
        return self.binary_blob is not None

    @classmethod
    def from_frames(cls, msg: List[bytes], sender: MKTLPeer = None, received_by: Optional[MKTLPeer] = None,
                    coms: Optional["MKTLComs"] = None):
        """
        Construct a MKTLMessage from a list of 4 or 5 ZeroMQ frames.

        This includes decoding the message type, request ID, key name, and the JSON body.
        Frame 5, if present, is stored as a binary blob.

        Args:
            coms: The parent MKTLComs instance.
            msg: List of frames received via ROUTER or SUB socket.

        Returns:
            An MKTLMessage instance.
        """
        logger = getLogger(__name__)
        logger.debug("Attempting to parse MKTLMessage from frames:\n   " + ',\n   '.join(map(str, msg)))

        if not msg or len(msg) < 4:
            logger.error("Malformed message: insufficient frames")
            raise ValueError('Malformed message: insufficient frames')

        msg_type, req_id, key, json_payload = msg[:4]
        extra_frames = msg[4:] if len(msg) > 4 else []
        binary_blob = extra_frames[0] if extra_frames else None

        msg_type = msg_type.decode()
        key = key.decode()
        json_data = json.loads(json_payload.decode())

        message = cls(msg_type, req_id, key, json_data, binary_blob, sender=sender, received_by=received_by, coms=coms)
        logger.debug(f"Parsed frames into {message}")
        return message

    @staticmethod
    def try_parse(msg: List[bytes], sender: MKTLPeer = None, received_by: Optional[MKTLPeer] = None,
                  coms: Optional["MKTLComs"] = None) -> Tuple[Optional['MKTLMessage'], Optional[str]]:
        """
        Attempt to parse a multipart message into an MKTLMessage.

        Returns a tuple of (message, error). If parsing fails, the message will be None and
        error will contain a string reason.

        Args:
            coms: The parent MKTLComs instance.
            msg: List of ZeroMQ frames.

        Returns:
            Tuple of (MKTLMessage or None, error string or None)
        """
        logger = getLogger(__name__)
        logger.debug(f'Trying to parse frames')# :\n   '+',\n   '.join(map(str, msg)))
        try:
            m = MKTLMessage.from_frames(msg, received_by=received_by, sender=sender, coms=coms)
            return m, None
        except Exception as e:
            logger.debug(f"Failed to parse MKTLMessage: {e}")
            return None, str(e)

    def to_frames(self) -> List[bytes]:
        """
        Build the ZeroMQ frame list for this message.

        Frames:
            [msg_type, req_id, key, json, [blob]]

        Returns:
            List of byte strings to send with send_multipart().
        """

        frames = [
            self.msg_type.encode(),
            self.req_id.bytes,
            self.key.encode(),
            json.dumps(self.json_data).encode()
        ]

        if self.binary_blob:
            frames.append(self.binary_blob)
        return frames

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
        if self.coms is None:
            raise RuntimeError("Cannot ack a message without a coms instance")
        logger = getLogger(__name__)
        logger.debug(f"Attempting ack for {self}")
        with self._respond_lock:
            if not self.responded:
                msg = MKTLMessage('ack', self.req_id, self.key,{'pending': True},
                                  destination=self.sender)
                self.coms._send_queue.put(msg)
                self.acknowledged = True
                logger.info(f"ACK sent for {self}")

    def respond(self, value, binary_blob: Optional[bytes] = None):
        """
        Send a full 'response' message to the requester.

        Includes a JSON-serializable payload and an optional binary blob.

        Args:
            value: JSON-compatible return value.
            binary_blob: Optional raw bytes for frame 6.
        """
        if self.coms is None:
            raise RuntimeError("Cannot respond on a message without a coms instance")

        logger = getLogger(__name__)
        logger.debug(f"Attempting respond {self}")
        with self._respond_lock:
            if not self.responded:
                msg = MKTLMessage('response', self.req_id, self.key, value, binary_blob=binary_blob,
                                  destination=self.sender)
                self.coms._send_queue.put(msg)
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
        if self.coms is None:
            raise RuntimeError("Cannot fail a message without a coms instance")
        logger = getLogger(__name__)
        logger.debug(f"Attempting fail {self}")
        with self._respond_lock:
            if not self.responded:
                msg = MKTLMessage('error', self.req_id, self.key, {'error': error_msg},
                                  destination=self.sender)
                self.coms._send_queue.put(msg)
                self.responded = True
                logger.warning(f"Error sent for message {self}, error={error_msg}")
            else:
                logger.debug(f"Already responded for {self}")

    @classmethod
    def build_config(cls, coms: "MKTLComs", keys: tuple) -> "MKTLMessage":
        """
        Build a config message to announce keys via a WHISPER.

        Args:
            coms: An instance of MKTLComs (provides identity, context, etc.).
            keys: A tuple of keys to be announced.

        Returns:
            An MKTLMessage instance configured for the config message.
        """
        return cls(
            coms=coms,
            msg_type='config',
            req_id=uuid.uuid4(),
            key='',
            json_data={"keys": list(keys)},
            binary_blob=None
        )


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
                 shutdown_callback: Optional[Callable] = None, pub_address: Optional[str] = None,
                 group: Optional[str] = "MKTLGROUP", start=False):
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

        # Init Zyre
        self._zyre = Pyre(identity)
        logger.debug(f"Initialized Zyre peer with identity={identity}")

        # Identity and Zyre group
        self.identity = MKTLPeer(self._zyre.name(), self._zyre.uuid())
        self.group = group
        self.authoritative_keys = {}

        self._ctx = zmq.Context.instance()
        self._running = False
        self._threads = []

        self._send_queue = queue.Queue()

        self._pub_socket = None
        self._pub_address = pub_address
        self._publish_queue = queue.Queue()

        self._sub_socket = None
        self._sub_address = None

        self._client_lock = threading.Lock()
        self._pending_replies = {}

        self._pending_subscriptions = set()
        self._pending_lock = threading.Lock()

        self._sub_callbacks: Dict[str, set[Callable]] = defaultdict(set)
        self._sub_listeners: Dict[str, List[MKTLSubscription]] = defaultdict(list)
        self._routing_table: Dict[str, MKTLPeer] = {}  # key -> identity

        self._shutdown_callback = shutdown_callback

        if authoritative_keys:
            for k, h in authoritative_keys.items():
                self.register_key_handler(k, h)
        self._register_internal_handlers()

        logger.info(f"MKTLComs created with identity={self.identity}")

        if start:
            self.start()

    def __repr__(self):
        return f'MKTLComs(identity={self.identity})'

    def _register_internal_handlers(self):
        """
        Register built-in handlers for mKTL internal keys.

        This includes `.mktl_control` and other diagnostic/control endpoints.
        Called automatically at startup.
        """
        logger = getLogger(__name__)
        logger.debug("Registering internal mktl_control handler...")
        self.register_key_handler(f'{self.identity.name}.mktl_control', self._handle_control_message)

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

    def _resolve_destination(self, key: str, destination: Optional[MKTLPeer]) -> MKTLPeer:
        """
        Determine the correct identity to route a request to.

        Args:
            key: The key to be resolved.
            destination: Optional explicit override.

        Returns:
            MKTLPeer identity of the target service.
        """
        logger = getLogger(__name__)
        if destination is not None:
            logger.debug(f"Destination override provided for key={key}: {destination}")
            return destination

        destination = self._routing_table.get(key, None)
        if destination is not None:
            logger.debug(f"Resolved destination for key={key}: {destination}")
        else:
            logger.warning(f"No destination found in routing table for key={key}")
            raise RuntimeError(f'No peer found for key: {key}')

        return destination

    def _send_request(self, msg_type: str, key: str, payload: dict, timeout: float, binary_blob: Optional[bytes] = None,
                      destination: Optional[UUID] = None) -> MKTLMessage:
        """
        Build and transmit a control request to another node, blocking for response.

        Handles request framing, timeout logic, and error propagation.
        """
        logger = getLogger(__name__)
        if not self._running:
            logger.error("Cannot send request because MKTLComs is not started.")
            raise RuntimeError('MKTLComs must be started before using get/set')

        req_id = uuid.uuid4()
        logger.debug(f"Creating MKTLMessage for request type={msg_type}, key={key}")
        m = MKTLMessage(
            msg_type=msg_type,
            req_id=req_id,
            key=key,
            json_data=payload,
            binary_blob=binary_blob,
            destination=destination
        )
        self._pending_replies[req_id] = None
        self._send_queue.put(m)

        start_time = time.time()
        while time.time() - start_time < timeout:
            with self._client_lock:
                rpl = self._pending_replies[req_id]
                if rpl is not None:
                    msg_obj = self._pending_replies.pop(req_id)
                    if msg_obj.msg_type == 'ack':
                        logger.info(f"ACK reply for request {req_id}")
                        self._pending_replies[req_id] = None
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

    def _serve_loop(self):
        """
        Main loop for handling Zyre-based messaging: WHISPER and peer tracking.
        """
        logger = getLogger(__name__)
        logger.info(f"Zyre loop started. Listening in group '{self.group}'...")

        # Start Zyre peer and join group
        self._zyre.join(self.group)
        self._zyre.start()
        self._zyre.shout(self.group, MKTLMessage.build_config(coms=self,
                                                              keys=tuple(self.authoritative_keys.keys())).to_frames())

        # Poll Zyre socket
        poller = zmq.Poller()
        zyre_sock = self._zyre.socket()
        poller.register(zyre_sock, zmq.POLLIN)

        while self._running:
            events = dict(poller.poll(timeout=10))

            if events.get(zyre_sock) == zmq.POLLIN:
                try:
                    frames = self._zyre.recv()
                    if not frames or len(frames) < 2:
                        continue

                    event_type = frames[0].decode(errors='ignore')
                    peer = MKTLPeer(frames[2], frames[1])
                    mktl_payload = frames[3:]

                    logger.debug(f"Received Zyre event: {event_type} from {peer}")

                    if event_type == "WHISPER":
                        m, err = MKTLMessage.try_parse(mktl_payload, sender=peer, received_by=self.identity, coms=self)
                        if err:
                            logger.warning(f"Malformed WHISPER from {peer}: {err}")
                            continue

                        if m.msg_type == 'config':
                            for key in m.json_data.get("keys", []):
                                # Add to the routing table
                                self._routing_table[key] = peer
                                logger.info(f"Discovered config key '{key}' from {peer}")

                        if m.is_request():
                            logger.debug(f"Handling request: {m}")
                            try:
                                result = self._handle_message(m)
                                if not m.responded:
                                    m.respond(result)
                            except Exception as e:
                                m.fail(e)
                                logger.error(f"Exception handling message: {e}", exc_info=True)

                        elif m.is_reply():
                            logger.debug(f'Received reply for request {m.req_id}')
                            with self._client_lock:
                                if m.req_id in self._pending_replies:
                                    self._pending_replies[m.req_id] = m
                                else:
                                    logger.warning(f'Ignoring reply for unknown request: {m.req_id}')

                    elif event_type == "ENTER":
                        logger.info(f"Peer {peer} entered, whispering keys")
                        cfg_message = MKTLMessage.build_config(coms=self, keys=tuple(self.authoritative_keys.keys()))
                        self._zyre.whisper(peer.uuid, cfg_message.to_frames())

                    elif event_type == "SHOUT":
                        m, err = MKTLMessage.try_parse(mktl_payload, sender=peer, received_by=self.identity, coms=self)
                        if err:
                            logger.warning(f"Malformed SHOUT from {peer}: {err}")
                            continue

                        if m.msg_type == 'config':
                            for key in m.json_data.get("keys", []):
                                # Add to the routing table
                                self._routing_table[key] = peer
                                logger.info(f"Discovered config key '{key}' from {peer} via SHOUT")

                    elif event_type == "EXIT":
                        logger.info(f"{peer} EXITED, removing keys")
                        self._routing_table = {k: v for k, v in self._routing_table.items() if v.uuid != peer.uuid}

                    else:
                        logger.debug(f"Unhandled Zyre event: {event_type}")

                except Exception as e:
                    logger.error(f"Exception in serve loop: {e}", exc_info=True)

            try:
                while True:
                    item = self._send_queue.get_nowait()
                    dest = item.destination if item.is_reply() else self._routing_table[item.key]
                    self._zyre.whisper(dest.uuid, item.to_frames())
                    logger.debug(f'Sending {item} to {dest}')
            except queue.Empty:
                pass

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
                msg_parts = self._sub_socket.recv_multipart(zmq.NOBLOCK)
                logger.debug('Received multipart message on SUB socket')
                if len(msg_parts) >= 2:
                    key = msg_parts[0].decode()
                    payload = json.loads(msg_parts[1].decode())
                    blob = msg_parts[2] if len(msg_parts) > 2 else None
                    msg = MKTLMessage('publish',None, key,  payload, blob)

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
            except zmq.Again:
                pass
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
                logger.debug(f'Sent multipart message: {frames[0].decode()}')
            except Exception as e:
                logger.error(f"Error in _publish_loop: {e}", exc_info=True)
                raise   #TODO

    def bind_pub(self, address: str):
        """
        Declare the PUB socket bind address. The actual socket will be created
        and bound by _publish_loop() within the background thread started by start().

        Args:
            address: ZeroMQ address string (e.g., 'tcp://*:5560')
        """
        if self._running:
            raise RuntimeError('Must bind_pub before starting.')
        getLogger(__name__).info(f"Setting PUB socket to {address}")
        self._pub_address = address

    def connect_sub(self, address: str):
        """
        Set the address of the SUB socket for a telemetry stream.

        Args:
            address: Address of a PUB or XPUB socket to connect to.
        """
        getLogger(__name__).info(f"Connecting SUB socket to {address}")
        self._sub_address = address

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

    def on_key(self, key: str):
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

        t = threading.Thread(name='MKTL thread', target=self._serve_loop, daemon=True)
        logger.info('Starting serve loop thread')
        t.start()
        self._threads.append(t)

        if self._pub_address:
            t_pub = threading.Thread(name='PUB thread', target=self._publish_loop, daemon=True)
            logger.info('Starting publish loop thread')
            t_pub.start()
            self._threads.append(t_pub)

        if self._sub_address:
            t_sub = threading.Thread(name='SUB thread', target=self._listen_loop, daemon=True)
            logger.info('Starting listen loop thread')
            t_sub.start()
            self._threads.append(t_sub)

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

    def get(self, key: str, payload: Any = None, timeout: float = 2.0, destination: Optional[UUID] = None) -> MKTLMessage:
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
        return self._send_request('get', key, payload or {}, timeout, None, UUID)

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
        if isinstance(key_prefix, str):
            key_prefix = [key_prefix]
        logger.info(f"Requesting subscription to prefixes: {key_prefix}")
        with self._pending_lock:
            for k in key_prefix:
                self._pending_subscriptions.add(k)

        if callback:
            for k in key_prefix:
                self._sub_callbacks[k].add(callback)
            logger.debug(f"Callback registered for prefix={key_prefix}")

        listener = MKTLSubscription()
        for k in key_prefix:
            self._sub_listeners[k].append(listener)
        logger.debug(f"Created MKTLSubscription for prefixes: {key_prefix}")
        return listener

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
