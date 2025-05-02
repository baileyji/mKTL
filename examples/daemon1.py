from logging import getLogger
import logging
import logging.config
from typing import Callable, Any
import time
import random
import threading
import numpy as np
from mktl.mktlcoms import MKTLComs, MKTLMessage


class HardwareThing:
    def __init__(self, name):
        self.name = name
        getLogger(__name__).info(f"{self} created")

    def __repr__(self):
        return f"HardwareThing(name={self.name})"

    def cmd1(self):
        getLogger(__name__).info(f"{self}.cmd1")

    def cmd2(self, config:Any, kwarg=8000):
        getLogger(__name__).info(f"{self}.cmd2(config={config}, kwarg={kwarg})")

    def status(self, verbose:bool=False)->dict:
        getLogger(__name__).info(f"{self}.status(verbose={verbose})")
        d = {'node': '',
                 'position': '',
                 'target_position':  '',
                 'error_reg': '',
                 'error_code': '',}
        d.update({
             'network_state': '',
             'mode_of_operation': '',
             'velocity_demand' : '',
             'velocity_actual': '',
             'velocity_profile': '',
             'velocity_target': '',
             'torque_actual' : '',
             'controlword': '',
             'statusword': '',
             'temperatue': '',
             })
        status = {id: [d,d] for id in range(11)}

        return status

    def shutdown(self):
        getLogger(__name__).info(f"{self}.shutdown()")


class Daemon:
    def __init__(self, mKTLname:str, publish_addr:tuple[str, int]=None, start=True):

        self._shutdown = False
        self.name = mKTLname
        self.comms: MKTLComs | None = None
        self.CSU_COMMANDS: dict[str, Callable] = {}

        # Set up hardware
        self.hardware = HardwareThing(f"{mKTLname}'s hardware")

        # Set up command handlers
        commands = {
            f'{self.name}.cmd1': self.handler,
            f'{self.name}.cmd2': self.handler,
            f'{self.name}.status': self.handler,
        }

        # Set up communication
        self.comms = MKTLComs(identity=self.name,
                              authoritative_keys=commands,
                              shutdown_callback=self.shutdown,
                              pub_address=f'tcp://{publish_addr[0]}:{publish_addr[1]}',
                              start=False)

        if start:
            self.start()


    def start(self):
            self.comms.start()
            threading.Thread(target=self.publish_junk, name='Publish Junk', args=('status.temp',),
                             daemon=True).start()

    def publish_junk(self, topic):
        blob = np.ones(1024 ** 2, dtype=np.uint32)  # 4 MB
        while not self._shutdown:
            self.comms.publish(f'{self.name}.{topic}', np.random.uniform(),
                              binary_blob=blob if np.random.uniform() <.3 else None)
            time.sleep(2)

    def subscribe_junk(self, topics):
        self.comms.subscribe(topics)
        for message in self.comms.subscribe(topics):
            getLogger(__name__).info(f'Heard a: {message}')

    def shutdown(self):
        """Shutdown the server."""
        getLogger(__name__).info('Shutting down')
        self._shutdown = True
        self.hardware.shutdown()
        self.comms.stop()
        exit(0)

    def handler(self, msg:MKTLMessage):
        """
        Handle a command message.

        TODO: This is gross and my fault -JIB, will clean up with better integrations with mktlcoms.
        """

        SETTABLE = ('cmd1', 'cmd2')
        GETTABLE = ('status', )

        key_prefix, _, key_tail = msg.key.rpartition('.')
        payload = msg.json_data
        msg.ack()

        if msg.msg_type =='get' and key_tail not in GETTABLE:
            msg.fail('get unsupported')

        if msg.msg_type =='set' and key_tail not in SETTABLE:
            msg.fail('set unsupported')

        args = payload.get('args', [])
        kwargs = payload.get('kwargs', {})

        try:
            result = getattr(self.hardware, key_tail)(*args, **kwargs) or 'OK'
        except Exception as e:
            getLogger(__name__).exception(f"Exception for {msg.key}: {e}", exc_info=True)
            msg.fail(str(e))
            return

        msg.respond(result)

def setup_logging():


    config = {'loggers': {
        'mktl':
             {'level': 'DEBUG'},
         "":
             {'handlers': [ 'default' ],
             'level': 'WARNING', 'propagate': False},
        '__main__':
             {'level': 'DEBUG'}},
        'version': 1,
        'disable_existing_loggers': False,
        'handlers':
             {'default':  {'class': 'logging.StreamHandler',
                           'formatter': 'default',
                           'level': 'DEBUG',
                           'stream': 'ext://sys.stdout'}},
        'formatters': {
            'brieffmt':
                {'format': '%(message)s'},
            'default':
                {'format': '%(asctime)s %(name)s:%(levelname)-8s (%(threadName)s) %(message)s',
                 'datefmt': '%H:%M:%S'}
        }
     }
    logging.config.dictConfig(config)

if __name__ == '__main__':
    setup_logging()

    app = Daemon('DemoDaemon1', publish_addr = ('localhost', 6000), start=False)
    app.comms.connect_sub('tcp://localhost:6001')


    threading.Thread(name='ALL Listener', target=app.subscribe_junk,
                     args=(('DemoDaemon1.status.temp', 'bulk:DemoDaemon2.status.temp'), ), daemon=True).start()

    threading.Thread(name='Small Listener', target=app.subscribe_junk,
                     args=(('DemoDaemon2.status.temp',), ), daemon=True).start()

    app.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        app.shutdown()