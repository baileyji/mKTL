#! /usr/bin/env python3

# This is a mKTL test daemon. Receiving trivial requests, providing trivial
# answers.

import uuid
import threading
import time
import socket

import mktl.config.hash
import mktl.daemon.store
import mktl.daemon.item
import mktl.config.provenance


def main():
    store = MarkPie('mpie', 'cloned')

    while True:
        try:
            main.shutdown.wait(30)
        except (KeyboardInterrupt, SystemExit):
            break


main.shutdown = threading.Event()


class MarkPie(mktl.daemon.store.Store):

    def setup(self):
        """ Custom item instantiation happens here.
        """

        config = self.daemon_config[self.daemon_uuid]
        items = self.daemon_config[self.daemon_uuid]['keys']
        keys = items.keys()
        keys = list(keys)

        SequenceInteger(self, 'SEQUENCE_INTEGER')
        keys.remove('SEQUENCE_INTEGER')

        for key in keys:
            item_config = items[key]
            item = mktl.daemon.item.Item(self, key)

            try:
                type = item_config['type']
            except KeyError:
                type = None

            if type == 'numeric':
                item.cached = 0

        return


# end of class MarkPie


class SequenceInteger(mktl.daemon.item.Item):
    maximum = 2 ** 63 - 1

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cached = 0
        self.poll(0.5)

    def req_refresh(self):
        self.cached += 1

        if self.cached > self.maximum:
            self.cached = 0

        mktl.daemon.item.Item.req_refresh(self)


# end of class SequenceInteger

def generate_config():
    store = 'potpie'
    local_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, store))

    block = dict()
    block['name'] = store
    block['uuid'] = local_uuid
    block['time'] = time.time()

    hostname = socket.getfqdn()
    req = main.req.port
    pub = main.pub.port
    mktl.config.provenance.add(block, hostname, req, pub)

    keys = dict()

    keys['INTEGER'] = dict()
    keys['INTEGER']['description'] = 'A dummy keyword, ostensibly numeric.'
    keys['INTEGER']['units'] = 'meaningless units'
    keys['INTEGER']['type'] = 'numeric'

    keys['STRING'] = dict()
    keys['STRING']['description'] = 'A dummy keyword, ostensibly a string.'
    keys['STRING']['type'] = 'string'

    keys['BULK'] = dict()
    keys['BULK']['description'] = 'A bulk data keyword.'
    keys['BULK']['type'] = 'bulk'

    block['hash'] = mktl.config.hash.hash(keys)
    block['keys'] = keys

    return block


if __name__ == '__main__':
    main()
