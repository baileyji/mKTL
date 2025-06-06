""" Implmentation of the top-level :func:`get` method. This is intended to be
    the principal entry point for users interacting with a key/value store.
"""

import zmq

from . import client
from . import config
from . import protocol


cache = dict()

def get(store, key=None):
    """ Return a cached :class:`Store` or :class:`Item` instance. If both a
        *store* and a *key* are specified, the requested *key* will be returned
        from the requested *store*. The same will occur if the sole argument is
        a store and key name concatenated with a dot (store.KEY). If only a
        *store* name is provided, a matching :class:`Store` instance will be
        returned.

        If the caller always uses :func:`get` to retrieve a :class:`Store` or
        :class:`Item` they will always receive the same instance of that class.

        The :func:`get` method is intended to be the primary entry point for
        all interactions with a key/value store, acting like a factory method
        without enforcing strict singleton behavior.
    """

    store = str(store)

    if key is None and '.' in store:
        store, key = store.split('.', 1)

    # Work from the local cache of Store instances first. This sequence
    # of checks is replicated at the end of the routine, after all the
    # handling of configuration data.

    try:
        store = cache[store]
    except KeyError:
        pass
    else:
        if key is None:
            return store
        else:
            key = store[key]
            return key


    # Assume any configuration loaded into memory is recent and adequate.

    try:
        config = config.get(store)
    except KeyError:
        config = None

    # If there is no local configuration, try loading one from disk. If that
    # succeeds we need to confirm it is still current before proceeding.

    if config is None:
        try:
            config = config.load(store)
        except KeyError:
            config = None
        else:
            config.add(store, config, save=False)
            config = refresh(store, config)

    # If we still don't have a configuration it's time to try a network
    # broadcast and hope someone's out there that can help.

    if config is None:
        guides = protocol.Discover.search()
        if len(guides) == 0:
            raise RuntimeError("no configuration available for '%s' (local or remote)" % (store))

        hostname,port = guides[0]
        client = protocol.Request.client(hostname, port)

        request = dict()
        request['request'] = 'CONFIG'
        request['name'] = store

        pending = client.send(request)
        response = pending.wait()

        try:
            config = response['data']
        except KeyError:
            raise RuntimeError("no configuration available for '%s' (local or remote)" % (store))

        # If we made it this far the network came through with an answer.
        config.add(store, config)


    # The local reference to the configuration isn't necessary, when the Store
    # instance initializes it will request the current configuration from what's
    # in config.Cache.

    store = client.Store(store)
    cache[store.name] = store

    if key is None:
        return store
    else:
        key = store[key]
        return key



def refresh(store, config):
    """ This is a helper method for :func:`get` defined in this file. The
        *config* passed in here was loaded from a file. Inspect the provenance
        for each block and attempt to refresh the local contents. Save any
        changes back to disk for future clients.
    """

    for uuid in config.keys():
        block = config[uuid]
        local_hash = block['hash']
        updated = False

        # Make a copy of the provenance sequence, traversing it in reverse
        # order (highest stratum first) looking for an updated configuration.

        provenance = list(block['provenance'])
        provenance.reverse()

        for stratum in provenance:
            hostname = stratum['hostname']
            req = stratum['req']

            client = protocol.Request.client(hostname, req)

            request = dict()
            request['request'] = 'HASH'
            request['name'] = store

            try:
                pending = client.send(request)
            except zmq.ZMQError:
                # No response from this daemon; move on to the next entry in
                # the provenance. If no daemons respond the client will have
                # to rely on the local disk cache.
                continue

            response = pending.wait()

            try:
                hashes = response['data']
            except KeyError:
                # No response available.
                continue

            try:
                remote_hash = hashes[uuid]
            except KeyError:
                # This block is not present on the remote side.
                continue

            if local_hash != remote_hash:
                # Mismatch; need to request an update before proceeding.
                request['request'] = 'CONFIG'
                pending = client.send(request)
                ### Again, exception handling may be required, though the
                ### previous request went through, so there shouldn't be a
                ### a fresh exception here unless the remote daemon just
                ### went offline.
                response = pending.wait()

                try:
                    new_block = response['data']
                except KeyError:
                    # No response available.
                    continue

                config.add(store, new_block)
                break


    # Whatever is present in the loaded cache is as good as it will get.
    # Return the current contents.

    config = config.get(store)
    return config





# vim: set expandtab tabstop=8 softtabstop=4 shiftwidth=4 autoindent:
