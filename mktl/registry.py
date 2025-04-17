"""registry.py

Minimal standalone registry server for mKTL protocol.

This service allows other components to declare their key ownership and lookup
authoritative addresses for keys. It also supports reverse queries for
what keys a given identity claims to serve.

To run:

    python3 registry.py
"""
from mktl.mktlcoms import MKTLComs, MKTLMessage
import threading
from logging import getLogger

DEFAULT_REGISTRY_PORT = 5571

class RegistryServer:
    """
    Implements the registry server for the mKTL protocol.

    This service maintains a mapping of keys to identities and network addresses.
    It supports two interfaces:
    - `registry.owner`: Look up which identity owns a key.
    - `registry.config`: Register or query all keys owned by an identity.

    This service allows daemons to dynamically advertise their presence and
    enables clients to discover where to send `get`/`set` requests.
    """

    def __init__(self, identity='registry', bind_addr='tcp://*:5570'):
        """
        Initialize the registry server with a local MKTLComs instance.

        Args:
            identity: The ZMQ identity to use for this server.
            authoritative_keys: Optional handlers to override default ones.
        """
        getLogger(__name__).info(f"Initializing RegistryServer with identity={identity}, bind_addr={bind_addr}")
        authoritative_keys = {'registry.owner': self._handle_owner,
                              'registry.config': self._handle_config}

        self.coms = MKTLComs(identity=identity, authoritative_keys=authoritative_keys,
                             shutdown_callback=self.shutdown, bind_addr=bind_addr, start=False)
        getLogger(__name__).debug("RegistryServer: MKTLComs instance created and bound.")

        self._store = {}  # key → {identity, address}
        self._identity_to_keys = {}  # identity → set(keys)

    def start(self):
        """
        Start the registry's internal MKTLComs instance.

        This launches the background communication thread and enables key lookup
        and registration services.
        """
        getLogger(__name__).info("Starting RegistryServer...")
        self.coms.start()
        getLogger(__name__).debug("RegistryServer started and ready to handle requests.")

    def shutdown(self):
        getLogger(__name__).warning("Shutdown called on RegistryServer.")
        self.coms.stop()
        exit(0)

    def _handle_owner(self, m: MKTLMessage):
        """
        Respond to `get` or `set` requests on `registry.owner`.

        - `get` retrieves the identity and address for a key.
        - `set` registers a new key-owner mapping using provided identity/address.

        Expected payload fields:
            key, identity, address
        """
        method = m.msg_type
        context = m.json_data
        getLogger(__name__).debug(f"Handling owner request: method={method}, payload={context}")

        if method == 'get':
            target_key = context.get('key')
            if not target_key:
                getLogger(__name__).warning("Missing 'key' in owner get request.")
                return {'error': "Missing 'key'"}
            entry = self._store.get(target_key)
            getLogger(__name__).debug(f"Lookup result for key={target_key}: {entry}")
            return entry or {'error': 'Key not found'}

        elif method == 'set':
            key_names = context.get('keys')
            ident = context.get('identity')
            addr = context.get('address')
            if not all([key_names, ident, addr]):
                getLogger(__name__).warning("Incomplete set request in registry.owner.")
                return {'error': 'Missing key, identity, or address'}
            for k in key_names:
                self._store[k] = {'identity': ident, 'address': addr}
                self._identity_to_keys.setdefault(ident, set()).add(k)
                getLogger(__name__).info(f"Registered ownership: key={k}, identity={ident}, address={addr}")
            return {'ok': True}

        getLogger(__name__).warning(f"Unsupported method in owner request: {method}")
        return {'error': f'Unsupported method: {method}'}

    def _handle_config(self, m: MKTLMessage):
        """
        Respond to `get` or `set` requests on `registry.config`.

        - `get` retrieves all keys owned by a given identity.
        - `set` registers multiple keys associated with a single identity/address.

        Expected payload fields:
            identity, address, keys[]
        """
        method = m.msg_type
        context = m.json_data
        getLogger(__name__).debug(f"Handling config request: method={method}, payload={context}")

        if method == 'get':
            ident = context.get('identity')
            if not ident:
                getLogger(__name__).warning("Missing 'identity' in config get request.")
                return {'error': "Missing 'identity'"}
            keys = list(self._identity_to_keys.get(ident, []))
            getLogger(__name__).debug(f"Returning keys for identity={ident}: {keys}")
            return {'keys': keys}

        elif method == 'set':
            ident = context.get('identity')
            addr = context.get('address')
            keys = context.get('keys')
            if not all([ident, addr, keys]):
                getLogger(__name__).warning("Incomplete set request in registry.config.")
                return {'error': 'Missing identity, address, or keys'}
            for k in keys:
                self._store[k] = {'identity': ident, 'address': addr}
                self._identity_to_keys.setdefault(ident, set()).add(k)
                getLogger(__name__).info(f"Registered config for identity={ident}, address={addr}, keys={k}")
            return {'ok': True}

        getLogger(__name__).warning(f"Unsupported method in config request: {method}")
        return {'error': f'Unsupported method: {method}'}


if __name__ == '__main__':
    from logging import basicConfig, DEBUG, getLogger
    basicConfig(level=DEBUG)
    getLogger('mktl').setLevel(DEBUG)
    getLogger('__main__').setLevel(DEBUG)
    bind_addr = f'tcp://*:{DEFAULT_REGISTRY_PORT}'
    reg = RegistryServer(bind_addr=bind_addr)
    reg.start()

    getLogger(__name__).info(f"RegistryServer running on {bind_addr}")
    while True:
        threading.Event().wait(60)
