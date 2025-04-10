"""registry.py

Minimal standalone registry server for mKTL protocol.

This service allows other components to declare their key ownership and lookup
authoritative addresses for keys. It also supports reverse queries for
what keys a given identity claims to serve.

To run:

    python3 registry.py
"""
from mktlcoms import MKTLComs, MKTLMessage
import threading


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

        authoritative_keys = {'registry.owner': self._handle_owner,
                              'registry.config': self._handle_config}

        self.coms = MKTLComs(identity=identity, authoritative_keys=authoritative_keys,
                             shutdown_callback=self.shutdown)
        self.coms.bind(bind_addr)
        self._store = {}  # key → {identity, address}
        self._identity_to_keys = {}  # identity → set(keys)

    def start(self):
        """
        Start the registry's internal MKTLComs instance.

        This launches the background communication thread and enables key lookup
        and registration services.
        """
        self.coms.start()

    def shutdown(self):
        self.coms.stop()
        exit(0)

    def _handle_owner(self, m:MKTLMessage):
        """
        Respond to `get` or `set` requests on `registry.owner`.

        - `get` retrieves the identity and address for a key.
        - `set` registers a new key-owner mapping using provided identity/address.

        Expected payload fields:
            key, identity, address
        """
        method = m.msg_type
        context = m.json_data
        if method == 'get':
            target_key = context.get('key')
            if not target_key:
                return {'error': "Missing 'key'"}
            entry = self._store.get(target_key)
            if entry:
                return entry
            return {'error': 'Key not found'}

        elif method == 'set':
            key_name = context.get('key')
            ident = context.get('identity')
            addr = context.get('address')
            if not all([key_name, ident, addr]):
                return {'error': 'Missing key, identity, or address'}
            self._store[key_name] = {'identity': ident, 'address': addr}
            self._identity_to_keys.setdefault(ident, set()).add(key_name)
            return {'ok': True}

        return {'error': f'Unsupported method: {method}'}

    def _handle_config(self, m:MKTLMessage):
        """
        Respond to `get` or `set` requests on `registry.config`.

        - `get` retrieves all keys owned by a given identity.
        - `set` registers multiple keys associated with a single identity/address.

        Expected payload fields:
            identity, address, keys[]
        """
        method = m.msg_type
        context = m.json_data
        if method == 'get':
            ident = context.get('identity')
            if not ident:
                return {'error': "Missing 'identity'"}
            keys = list(self._identity_to_keys.get(ident, []))
            return {'keys': keys}

        elif method == 'set':
            ident = context.get('identity')
            addr = context.get('address')
            keys = context.get('keys')
            if not all([ident, addr, keys]):
                return {'error': 'Missing identity, address, or keys'}
            for k in keys:
                self._store[k] = {'identity': ident, 'address': addr}
                self._identity_to_keys.setdefault(ident, set()).add(k)
            return {'ok': True}

        return {'error': f'Unsupported method: {method}'}



if __name__ == '__main__':
    from logging import basicConfig, DEBUG, getLogger
    basicConfig(level=DEBUG)
    getLogger('mktl').setLevel(DEBUG)
    getLogger('__main__').setLevel(DEBUG)
    reg = RegistryServer()
    reg.start()

    getLogger(__name__).info("RegistryServer running on tcp://*:5570")
    while True:
        threading.Event().wait(60)
