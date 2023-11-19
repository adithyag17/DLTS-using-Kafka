# Driver.py
import json
from confluent_kafka import Producer, Consumer, KafkaException
import sys
import time

def create_producer():
    config = {'bootstrap.servers': 'localhost:9092'}
    return Producer(config)

def create_consumer(driver_id):
    config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': f'driver_group_{driver_id}',
        'auto.offset.reset': 'earliest'
    }
    return Consumer(config)

def send_registration_message(producer, driver_id, port):
    registration_message = {
        'node_id': driver_id,
        'node_IP': f'127.0.0.1:{port}',
        'message_type': 'DRIVER_NODE_REGISTER'
    }
    message = json.dumps(registration_message)
    producer.produce('driver_registration_topic', key=str(driver_id), value=message)
    producer.flush()

def main():
    if len(sys.argv) != 3:
        print("Usage: python Driver.py <driver_id> <port>")
        sys.exit(1)

    driver_id = int(sys.argv[1])
    port = int(sys.argv[2])

    producer = create_producer()
    consumer = create_consumer(driver_id)

    # Send registration message to Orchestrator
    send_registration_message(producer, driver_id, port)

    # try:
    #     while True:
    #         # Perform driver logic

    #         # Listen for messages from Orchestrator
    #         msg = consumer.poll(1.0)
    #         if msg is not None:
    #             print(f"Received message from Orchestrator: {msg.value()}")
    #             # Process Orchestrator's message if needed

    #         time.sleep(2)

    # except KeyboardInterrupt:
    #     pass
    # finally:
    #     consumer.close()

if __name__ == "__main__":
    main()
