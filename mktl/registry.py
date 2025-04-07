from mktlcoms import MKTLComs, MKTLMessage
import threading


class RegistryServer:
    def __init__(self, identity="registry", bind_addr="tcp://*:5570"):

        authoritative_keys = {"registry.owner": self._handle_owner,
                              "registry.config": self._handle_config}

        self.coms = MKTLComs(identity=identity, authoritative_keys=authoritative_keys)
        self.coms.bind(bind_addr)
        self._store = {}  # key → {identity, address}
        self._identity_to_keys = {}  # identity → set(keys)

    def start(self):
        self.coms.start()

    def _handle_owner(self, *, key, method, context):
        if method == "get":
            target_key = context.get("key")
            if not target_key:
                return {"error": "Missing 'key'"}
            entry = self._store.get(target_key)
            if entry:
                return entry
            return {"error": "Key not found"}

        elif method == "set":
            key_name = context.get("key")
            ident = context.get("identity")
            addr = context.get("address")
            if not all([key_name, ident, addr]):
                return {"error": "Missing key, identity, or address"}
            self._store[key_name] = {"identity": ident, "address": addr}
            self._identity_to_keys.setdefault(ident, set()).add(key_name)
            return {"ok": True}

        return {"error": f"Unsupported method: {method}"}

    def _handle_config(self, *, key, method, context):
        if method == "get":
            ident = context.get("identity")
            if not ident:
                return {"error": "Missing 'identity'"}
            keys = list(self._identity_to_keys.get(ident, []))
            return {"keys": keys}

        elif method == "set":
            ident = context.get("identity")
            addr = context.get("address")
            keys = context.get("keys")
            if not all([ident, addr, keys]):
                return {"error": "Missing identity, address, or keys"}
            for k in keys:
                self._store[k] = {"identity": ident, "address": addr}
                self._identity_to_keys.setdefault(ident, set()).add(k)
            return {"ok": True}

        return {"error": f"Unsupported method: {method}"}


if __name__ == "__main__":
    reg = RegistryServer()
    reg.start()

    print("RegistryServer running on tcp://*:5570")
    while True:
        threading.Event().wait(60)
