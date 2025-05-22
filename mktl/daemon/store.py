import subprocess
import sys
import zmq
import importlib.resources
import os

from ..config import hash as config_hash  # Don't shadow hash()
from ..config import items
from ..config import cache
from ..config import file
from ..client import store
from .. import config
from ..protocol import discover
from ..protocol import request
from ..protocol import publish

from . import item
from . import persist
from . import port


class Store(store.Store):
    """ The daemon version of a Store is based on the client version; the
        behavior defaults to the client approach, but for items specified in
        the daemon *config* file a daemon-specific variant of the item will
        be loaded that supplements the client behavior with daemon-specific
        functionality.

        The developer is expected to subclass :class:`Store` class and
        implement a :func:`setup` method, and/or a :func:`setup_final`
        method.

        The *store* argument is the name of this store; *config* is the base
        name of the mKTL configuration file that defines the items in this
        store. *arguments* is expected to be an :class:`argparse.ArgumentParser`
        instance, though in practice it can be any Python object with specific
        named attributes of interest to a :class:`Store` subclass; it is not
        required. This is intended to be a vehicle for subclasses to receive
        key information from command-line arguments.
    """

    def __init__(self, name, config, arguments=None):

        self.name = name
        self.config = None
        self._items = dict()
        self.daemon_config = None
        self.daemon_uuid = None
        self._daemon_keys = set()

        daemon_config = file.load(name, cfg)
        self._update_daemon_config(daemon_config)

        # Use cached port numbers when possible. The ZMQError is thrown
        # when the requested port is not available; let a new one be
        # auto-assigned when that happens.

        req, pub = port.load(self.name, self.daemon_uuid)

        try:
            self.pub = publish.Server(port=pub, avoid=port.used())
        except zmq.error.ZMQError:
            self.pub = publish.Server(port=None, avoid=port.used())

        try:
            self.req = RequestServer(self, port=req, avoid=port.used())
        except zmq.error.ZMQError:
            self.req = RequestServer(self, port=None, avoid=port.used())

        port.save(self.name, self.daemon_uuid, self.req.port, self.pub.port)

        provenance = dict()
        provenance['stratum'] = 0
        provenance['hostname'] = self.req.hostname
        provenance['req'] = self.req.port
        provenance['pub'] = self.pub.port

        self.provenance = list()
        self.provenance.append(provenance)

        # A bit of a chicken and egg problem with the provenance. It can't be
        # established until the listener ports are known; we can't establish
        # the listener ports without knowing our UUID; we don't know the UUID
        # until the configuration is loaded. We're doctoring the configuration
        # after-the-fact, and thus need to refresh the local cache to ensure
        # consistency.

        self.daemon_config[self.daemon_uuid]['provenance'] = self.provenance
        cache.add(self.name, self.daemon_config)

        config = items.get(name)
        self._update_config(config)

        # Local machinery is intact. Invoke the setup() method, which is the
        # hook for the developer to establish their own custom Item classes
        # before filling in with empty caching Item classes.

        self.setup()
        self._setup_missing()

        # Restore any persistent values, and enable the retention of future
        # persistent values. If there are no persistent items present in this
        # store the call to _restore() is a no-op, and the persistence
        # subprocess will exit.

        self._restore()
        self._begin_persistence()

        # The promise is that setup_final() gets invoked after everything else
        # is ready, but before we go on the air.

        self.setup_final()

        # Ready to go on the air.

        discovery = discover.DirectServer(self.req.port)

        guides = discover.search(wait=True)
        self._publish_config(guides)

    def _begin_persistence(self):
        """ Start the background process responsible for updating the
            persistent value cache.
        """

        ### This is not a valid way to find the markpersistd executable.

        arguments = list()
        binfile = os.path.join(str(importlib.resources.files('mktl')), '../python/bin/markpersistd')
        arguments.append(binfile)
        arguments.append(self.name)
        arguments.append(self.daemon_uuid)
        print(arguments)
        pipe = subprocess.PIPE
        self.persistence = subprocess.Popen(arguments)

    def _publish_config(self, targets=tuple()):
        """ Put our local configuration out on the wire.
        """

        config = dict(self.daemon_config)

        req = dict()
        req['request'] = 'CONFIG'
        req['name'] = self.name
        req['data'] = config

        for address, port in targets:
            try:
                request.send(req, address, port)
            except zmq.error.ZMQError:
                pass

    def _restore(self):
        """ Bring back any values in the local persistent cache, and push them
            through to affected Items for handling.
        """

        loaded = persist.load(self.name, self.daemon_uuid)

        for key in loaded.keys():
            faux_message = loaded[key]
            item = self[key]
            item.req_set(faux_message)

    def _update_config(self, config):

        self.config = config

        keys = config.keys()
        keys = list(keys)
        keys.sort()

        for key in keys:
            try:
                self._items[key]
            except KeyError:
                self._items[key] = None

    def _update_daemon_config(self, cfg):

        uuid = list(cfg.keys())[0]
        self.daemon_config = cfg
        self.daemon_uuid = uuid
        cache.add(self.name, cfg)

        config = items.get(self.name)
        self._daemon_keys.update(config)
        self._update_config(config)

    def setup(self):
        """ Subclasses should override the :func:`setup` method to instantiate
            any custom :class:`Item` subclasses or otherwise execute custom
            code. When :func:`setup` is called the bulk of the :class:`Store`
            machinery is in place, but cached values have not been loaded, nor
            has the presence of this daemon been announced. The default
            implementation of this method takes no actions.
        """

        pass


    def _setup_missing(self):
        """ Inspect the locally known list of :class:`Item` instances; create
            default, caching instances for any that were not previously
            populated by the call to :func:`setup`.
        """

        local = list(self._daemon_keys)

        for key in local:
            x = self._items[key]

            if x is None:
                x = item.Item(self, key)
                self._items[key] = x


    def setup_final(self):
        """ Subclasses should override the :func:`setup_final` method to
            execute any/all code that should occur after all :class:`Item`
            instances have been created, including any non-custom :class:`Item`
            instances, but before this :class:`Store` announces its availability
            on the local network. The default implementation of this method
            takes no actions.
        """

        pass


# end of class Store


class RequestServer(request.Server):

    def __init__(self, store, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.store = store

    def req_config(self, request):

        store = request['name']

        if store == self.store.name:
            config = dict(self.store.daemon_config)
        else:
            config = items.get(store)

        return config

    def req_handler(self, socket, lock, ident, request):
        """ Inspect the incoming request type and decide how a response
            will be generated.
        """

        self.req_ack(socket, lock, ident, request)

        try:
            type = request['request']
        except KeyError:
            raise KeyError("invalid request JSON, 'request' not set")

        try:
            name = request['name']
        except KeyError:
            if type != 'HASH':
                raise KeyError("invalid request JSON, 'name' not set")

        if type == 'HASH':
            payload = self.req_hash(request)
        elif type == 'SET':
            payload = self.req_set(request)
        elif type == 'GET':
            payload = self.req_get(request)
        elif type == 'CONFIG':
            payload = self.req_config(request)
        else:
            raise ValueError('unhandled request type: ' + type)

        return payload

    def req_get(self, request):

        key = request['name']
        store, key = key.split('.', 1)

        if key in self.store._daemon_keys:
            pass
        else:
            raise KeyError('this daemon does not contain ' + repr(key))

        payload = self.store[key].req_get(request)
        return payload

    def req_set(self, request):

        key = request['name']
        store, key = key.split('.', 1)

        if key in self.store._daemon_keys:
            pass
        else:
            raise KeyError('this daemon does not contain ' + repr(key))

        payload = self.store[key].req_set(request)
        return payload

    def req_hash(self, request):

        try:
            store = request['name']
        except KeyError:
            store = None

        cached = config_hash.get(store)
        return cached

# end of class RequestServer


# vim: set expandtab tabstop=8 softtabstop=4 shiftwidth=4 autoindent:
