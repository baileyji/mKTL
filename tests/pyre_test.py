import logging
import time
import threading
from mktl.mktlcoms import MKTLComs

# Set up logging
logging.basicConfig(level=logging.DEBUG)


def handle_adc_enabled(msg):
    logging.info(f"Handler received message: {msg}")
    logging.info(f"Hello World!")


def start_receiver(node_name: str, group_name: str):
    """
    Starts a MKTLComs node that will receive messages.
    """
    node = MKTLComs(
        identity=node_name,
        authoritative_keys={"adc.enabled": handle_adc_enabled},  # Handler is callable
        group=group_name,
        bind_addr=f"tcp://*:{5700 + hash(node_name) % 1000}",
        pub_address=f"tcp://*:{5800 + hash(node_name) % 1000}",
        start=True
    )

    logging.info(f"{node_name} listening for messages...")
    time.sleep(10)  # Keep the receiver running for a bit


def start_sender(node_name: str, group_name: str, message_data: dict, key: str):
    """
    Starts a MKTLComs node and sends a `get` request using MKTLComs API.
    """
    node = MKTLComs(
        identity=node_name,
        group=group_name,
        bind_addr=f"tcp://*:{5700 + hash(node_name) % 1000}",
        pub_address=f"tcp://*:{5800 + hash(node_name) % 1000}",
        start=True
    )

    # Wait for peer discovery and key announcements
    time.sleep(3)

    try:
        response = node.get(key, message_data, timeout=20)
        logging.info(f"{node_name} received response: {response}")
    except Exception as e:
        logging.error(f"{node_name} failed to get response: {e}")


def run_test():
    group_name = "TestGroup"
    key = "adc.enabled"
    message = {"message": "Hello from Peer1"}

    # Start receiver first
    receiver = threading.Thread(target=start_receiver, args=("Peer2", group_name))
    receiver.start()

    time.sleep(10)

    # Then start a sender
    sender = threading.Thread(target=start_sender, args=("Peer1", group_name, message, key))
    sender.start()

    sender.join()
    receiver.join()

    print("Test completed.")


if __name__ == "__main__":
    run_test()
