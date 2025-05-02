from daemon1 import setup_logging

setup_logging()
from mktl.mktlcoms import MKTLComs, MKTLMessage
coms = MKTLComs(identity='interactive', start=True)