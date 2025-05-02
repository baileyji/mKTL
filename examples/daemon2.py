from daemon1 import Daemon, setup_logging
import threading
import time

if __name__ == '__main__':
    setup_logging()

    app = Daemon('DemoDaemon2', publish_addr = ('localhost', 6001), start=False)
    app.comms.connect_sub('tcp://localhost:6000')

    threading.Thread(name='ALL Listener', target=app.subscribe_junk,
                     args=(('DemoDaemon1.status.temp', 'bulk:DemoDaemon1.status.temp'),), daemon=True).start()

    threading.Thread(name='Small Listener', target=app.subscribe_junk,
                     args=(('DemoDaemon1.status.temp',), ), daemon=True).start()

    app.start()


    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        app.shutdown()

