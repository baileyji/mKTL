import zmq
import threading
import uuid
import json
import queue
import time
import logging
from typing import Callable, Dict, Optional, Any, List, Tuple, Union, Set


class MKTLMessage:
    VALID_TYPES = {"get", "set", "ack", "response", "error", "publish"}
    REPLY_TYPES = {"ack", "response", "error"}
    REQUEST_TYPES = {"get", "set"}

    def __init__(self, coms, sender_id, msg_type, req_id, key, json_data, binary_blob=None, destination: Optional[bytes] = None):
        if msg_type not in self.VALID_TYPES:
            raise ValueError(f"Invalid message type: {msg_type}")

        self.coms = coms
        self.sender_id = sender_id
        self.msg_type = msg_type
        self.req_id = req_id
        self.key = key
        self.json_data = json_data
        self.binary_blob = binary_blob
        self.responded = False
        self.destination = destination or b''

    @classmethod
    def from_frames(cls, coms, msg: List[bytes]):
        if not msg or len(msg) < 6:
            raise ValueError("Malformed message: insufficient frames")

        sender_id = msg[0]
        _, msg_type, req_id, key, json_payload = msg[1:6]
        extra_frames = msg[6:] if len(msg) > 6 else []
        binary_blob = extra_frames[0] if extra_frames else None

        msg_type = msg_type.decode()
        key = key.decode()
        json_data = json.loads(json_payload.decode())

        return cls(coms, sender_id, msg_type, req_id, key, json_data, binary_blob)

    @staticmethod
    def try_parse(coms, msg: List[bytes]) -> Tuple[Optional["MKTLMessage"], Optional[bytes], Optional[str]]:
        try:
            m = MKTLMessage.from_frames(coms, msg)
            return m, None, None
        except Exception as e:
            sender_id = msg[0] if len(msg) >= 1 else None
            return None, sender_id, str(e)

    def is_reply(self):
        return self.msg_type in self.REPLY_TYPES

    def is_request(self):
        return self.msg_type in self.REQUEST_TYPES

    def ack(self):
        if not self.responded:
            self._enqueue("ack", {"pending": True}, destination=self.sender_id)

    def respond(self, value, binary_blob: Optional[bytes] = None):
        if not self.responded:
            self._enqueue("response", {"value": value}, binary_blob, destination=self.sender_id)
            self.responded = True

    def fail(self, error_msg):
        if not self.responded:
            self._enqueue("error", {"error": error_msg}, destination=self.sender_id)
            self.responded = True

    def _enqueue(self, msg_type: str, payload: dict, binary_blob: Optional[bytes] = None, destination: Optional[bytes] = None):
        self.msg_type = msg_type
        self.json_data = payload
        self.binary_blob = binary_blob
        if destination is not None:
            self.destination = destination
        self.coms._send_queue.put(self)

    def has_blob(self):
        return self.binary_blob is not None

    def get_frames(self) -> List[bytes]:
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
        self._queue = queue.Queue()
        self._closed = False

    def put(self, msg: MKTLMessage):
        if not self._closed:
            self._queue.put(msg)

    def close(self):
        self._closed = True

    def __iter__(self):
        while not self._closed:
            try:
                msg = self._queue.get(timeout=0.5)
                yield msg
            except queue.Empty:
                continue


class MKTLComs:
    def __init__(
        self,
        identity: Optional[str] = None,
        authoritative_keys: Optional[Dict[str, Callable]] = None,
        registry_addr: Optional[str] = None,
    ):
        self.identity = identity or f"mktl-{uuid.uuid4().hex[:8]}"
        self.authoritative_keys = authoritative_keys or {}
        self.registry_addr = registry_addr

        self._ctx = zmq.Context.instance()
        self._running = False
        self._threads = []

        self._bind_address = None
        self._connect_addresses = []
        self._send_queue = queue.Queue()

        self._pub_socket = None
        self._pub_address = None

        self._router = None
        self._dealer = None
        self._sub_socket = None
        self._sub_address = None

        self._client_lock = threading.Lock()
        self._pending_replies = {}

        self._sub_callbacks: Dict[str, List[Callable]] = {}
        self._sub_listeners: Dict[str, List[MKTLSubscription]] = {}

        self._routing_table: Dict[str, Tuple[str, str]] = {}  # key -> (identity, address)
        self._connected_addresses: Set[str] = set()

        self._register_internal_handlers()

    def _register_internal_handlers(self):
        self.register_key_handler(f"{self.identity}.mktl_control", self._handle_control_message)

    def _handle_control_message(self, msg):
        logging.info(f"Control message received: {msg.json_data}")
        return msg.respond("ACK")

    def _query_registry_owner(self, key: str) -> Optional[Tuple[str, str]]:
        if not self.registry_addr:
            logging.warning("Registry address not configured.")
            return None

        ctx = zmq.Context.instance()
        s = ctx.socket(zmq.DEALER)
        temp_id = f"query-{uuid.uuid4().hex[:8]}"
        s.setsockopt(zmq.IDENTITY, temp_id.encode())
        s.connect(self.registry_addr)

        req_id = uuid.uuid4().bytes
        json_data = json.dumps({"key": key}).encode()

        frames = [
            b"registry", b"", b"get", req_id,
            b"registry.owner", json_data
        ]
        s.send_multipart(frames)

        poller = zmq.Poller()
        poller.register(s, zmq.POLLIN)
        socks = dict(poller.poll(timeout=2000))
        if s in socks:
            reply = s.recv_multipart()
            if len(reply) >= 6:
                payload = json.loads(reply[5].decode())
                identity = payload.get("identity")
                address = payload.get("address")
                if identity and address:
                    self._routing_table[key] = (identity, address)
                    if address not in self._connected_addresses:
                        self._connect_addresses.append(address)
                    return identity, address

        return None

    def _resolve_destination(self, key: str, destination: Optional[str]) -> str:
        if destination is not None:
            return destination

        identity, _ = self._routing_table.get(key, (None, None))
        if identity is None:
            resolved = self._query_registry_owner(key)
            if not resolved:
                raise RuntimeError(f"Could not resolve destination for key: {key}")
            identity, _ = resolved
        return identity

    def _load_keys_for_prefix(self, prefix: str):
        pass

    def bind(self, address: str):
        self._bind_address = address

    def connect(self, address: str):
        if address not in self._connected_addresses:
            self._connect_addresses.append(address)
            self._connected_addresses.add(address)

    def register_key_handler(self, key: str, handler: Callable):
        self.authoritative_keys[key] = handler

    def start(self):
        self._running = True
        t = threading.Thread(target=self._serve_loop, daemon=True)
        t.start()
        self._threads.append(t)

        if self.registry_addr:
            self._send_registry_config()

        if self._sub_socket:
            t_sub = threading.Thread(target=self._listen_loop, daemon=True)
            t_sub.start()
            self._threads.append(t_sub)

    def _serve_loop(self):
        self._router = self._ctx.socket(zmq.ROUTER)
        self._router.setsockopt(zmq.IDENTITY, self.identity.encode())
        self._router.bind(self._bind_address)

        self._dealer = self._ctx.socket(zmq.DEALER)
        self._dealer.setsockopt(zmq.IDENTITY, self.identity.encode())
        for addr in self._connect_addresses:
            self._dealer.connect(addr)

        poller = zmq.Poller()
        poller.register(self._router, zmq.POLLIN)
        poller.register(self._dealer, zmq.POLLIN)

        while self._running:
            events = dict(poller.poll(timeout=10))

            for sock in (self._router, self._dealer):
                if sock in events and events[sock] == zmq.POLLIN:
                    msg = sock.recv_multipart()
                    m, sender_id, err = MKTLMessage.try_parse(self, msg)
                    if m:
                        try:
                            self._handle_message(m)
                        except Exception as e:
                            logging.error(f"Exception while handling message: {e}", exc_info=True)
                    elif sender_id:
                        error_msg = {
                            "error": f"Malformed message: {err}"
                        }
                        frames = [
                            sender_id,
                            b"",
                            b"error",
                            uuid.uuid4().bytes,
                            b"",
                            json.dumps(error_msg).encode()
                        ]
                        self._router.send_multipart(frames)

            try:
                while True:
                    item = self._send_queue.get_nowait()
                    if item.msg_type in ("ack", "response", "error"):
                        self._router.send_multipart(item.get_frames())
                    else:
                        self._dealer.send_multipart(item.get_frames())
            except queue.Empty:
                pass

        self._router.close()
        self._dealer.close()

    def _send_registry_config(self):
        ctx = zmq.Context.instance()
        s = ctx.socket(zmq.DEALER)
        s.setsockopt(zmq.IDENTITY, self.identity.encode())
        s.connect(self.registry_addr)

        keys = list(self.authoritative_keys.keys())
        payload = {
            "identity": self.identity,
            "address": self._bind_address,
            "keys": keys
        }

        req_id = uuid.uuid4().bytes
        frames = [
            b"registry", b"", b"set", req_id,
            b"registry.config", json.dumps(payload).encode()
        ]
        s.send_multipart(frames)
        s.close()

    def stop(self):
        self._running = False
        for t in self._threads:
            t.join()

    def get(self, key: str, timeout: float = 2.0, destination: Optional[str] = None) -> Any:
        resolved = self._resolve_destination(key, destination)
        return self._send_request("get", key, {}, timeout, None, resolved)

    def set(self, key: str, value: Any, timeout: float = 2.0, binary_blob: Optional[bytes] = None, destination: Optional[str] = None):
        resolved = self._resolve_destination(key, destination)
        return self._send_request("set", key, {"value": value}, timeout, binary_blob, resolved)

    def _send_request(self, msg_type: str, key: str, payload: dict, timeout: float, binary_blob: Optional[bytes] = None, destination: Optional[str] = None):
        if self._dealer is None:
            raise RuntimeError("MKTLComs must be started before using get/set")

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
                    if m.msg_type == "error":
                        raise RuntimeError(m.json_data.get("error", "Unknown error"))
                    return m.json_data.get("value"), m.binary_blob if m.has_blob() else None
            time.sleep(0.01)

        raise TimeoutError(f"Timeout waiting for {msg_type} response to key: {key}")

    def _handle_message(self, m: MKTLMessage):
        if m.is_reply():
            with self._client_lock:
                self._pending_replies[m.req_id] = m
        elif m.is_request():
            if m.key.endswith(".mktl_control"):
                m.respond("ack")
            else:
                m.fail("Unknown key")

    def subscribe(self, key_prefix: str, callback: Optional[Callable] = None) -> MKTLSubscription:
        if key_prefix not in self._sub_callbacks:
            self._sub_callbacks[key_prefix] = []
            self._sub_listeners[key_prefix] = []
            if self._sub_socket is None:
                self._sub_socket = self._ctx.socket(zmq.SUB)
                self._sub_socket.setsockopt(zmq.SUBSCRIBE, key_prefix.encode())
                if self._sub_address:
                    self._sub_socket.connect(self._sub_address)

        if callback:
            self._sub_callbacks[key_prefix].append(callback)

        listener = MKTLSubscription()
        self._sub_listeners[key_prefix].append(listener)
        return listener

    def _listen_loop(self):
        if not self._sub_socket:
            return

        while self._running:
            try:
                msg_parts = self._sub_socket.recv_multipart()
                if len(msg_parts) >= 2:
                    key = msg_parts[0].decode()
                    payload = json.loads(msg_parts[1].decode())
                    blob = msg_parts[2] if len(msg_parts) > 2 else None
                    msg = MKTLMessage(self, b"", "publish", b"", key, payload, blob)

                    for prefix, callbacks in self._sub_callbacks.items():
                        if key.startswith(prefix):
                            for cb in callbacks:
                                try:
                                    cb(msg)
                                except Exception as e:
                                    logging.error(f"Callback failed for key {key}: {e}")

                    for prefix, queues in self._sub_listeners.items():
                        if key.startswith(prefix):
                            for listener in queues:
                                listener.put(msg)
            except Exception as e:
                logging.error(f"Error in _listen_loop: {e}")

    def publish(self, key: str, payload: dict, binary_blob: Optional[bytes] = None):
        if self._pub_socket is None:
            raise RuntimeError("PUB socket not initialized. Call bind_pub/connect_pub first.")

        frames = [key.encode(), json.dumps(payload).encode()]
        if binary_blob:
            frames.append(binary_blob)

        self._pub_socket.send_multipart(frames)

    def bind_pub(self, address: str):
        self._pub_address = address
        self._pub_socket = self._ctx.socket(zmq.PUB)
        self._pub_socket.bind(address)

    def connect_pub(self, address: str):
        self._sub_address = address
        if self._sub_socket is None:
            self._sub_socket = self._ctx.socket(zmq.SUB)
        self._sub_socket.connect(address)