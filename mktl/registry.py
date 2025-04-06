
import zmq
import threading
import uuid
import json
from collections import defaultdict

class RegistryServer:
    def __init__(self, bind_addr):
        self._ctx = zmq.Context.instance()
        self._router = self._ctx.socket(zmq.ROUTER)
        self._router.bind(bind_addr)
        self._running = False
        self._known_keys = {}  # key -> {identity, address}
        self._identity_to_keys = defaultdict(set)

    def start(self):
        self._running = True
        threading.Thread(target=self._loop, daemon=True).start()

    def stop(self):
        self._running = False
        self._router.close()

    def _loop(self):
        while self._running:
            try:
                msg = self._router.recv_multipart()
                if len(msg) < 6:
                    continue

                sender_id, _, msg_type, req_id, key, payload = msg[:6]
                msg_type = msg_type.decode()
                key = key.decode()
                data = json.loads(payload.decode())

                if msg_type == "get" and key == "registry.owner":
                    target = data["key"]
                    response = self._known_keys.get(target, {})
                    frames = [sender_id, b"", b"response", req_id, key.encode(), json.dumps(response).encode()]
                    self._router.send_multipart(frames)

                elif msg_type == "set" and key == "registry.config":
                    ident = data["identity"]
                    addr = data["address"]
                    for k in data["keys"]:
                        self._known_keys[k] = {"identity": ident, "address": addr}
                        self._identity_to_keys[ident].add(k)
                    frames = [sender_id, b"", b"ack", req_id, key.encode(), json.dumps({"registered": True}).encode()]
                    self._router.send_multipart(frames)

                else:
                    frames = [sender_id, b"", b"error", req_id, key.encode(), json.dumps({"error": "Unknown request"}).encode()]
                    self._router.send_multipart(frames)

            except Exception as e:
                print("Registry error:", e)
