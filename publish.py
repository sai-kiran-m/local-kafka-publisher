import os
import json
import time
import argparse

from kafka import KafkaProducer

DATA_FOLDER = os.path.join(os.path.dirname(__file__), "data")

def load_json_messages(file_name):
    """Load JSON data from a file"""

    file_path = os.path.join(DATA_FOLDER, file_name)
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        raise FileNotFoundError(f"File at {file_path} does not exist")
    with open(file_path, "r") as f:
        return json.load(f)

def publish_messages(file_name, topic, delay):
    """Publish messages to a Kafka topic from a JSON file"""

    producer =  KafkaProducer(
        bootstrap_servers ="localhost:9092",
        value_serializer=lambda dict_obj: json.dumps(dict_obj).encode("utf-8")
    )

    messages = load_json_messages(file_name)
    for i, msg in enumerate(messages):
        producer.send(topic, value=msg)
        print(f" Published to msg at index {i} to '{topic}'")
        if delay > 0:
            time.sleep(delay)

    producer.flush()
    print(" All messages published")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Publish messages to a Kafka topic from a JSON file")
    parser.add_argument("--file_name", default="sample-orders.json", help="Path to the JSON file containing msgs")
    parser.add_argument("--topic_name", default="orders", help="Kafka topic where messages will be published")
    parser.add_argument("--delay", default=0, type=int, help="Delay in seconds between messages")

    args = parser.parse_args()

    file_name = args.file_name
    topic_name = args.topic_name
    delay = args.delay
    print(f" Publishing messages from '{file_name}' to topic '{topic_name}' with a delay of {delay} seconds")

    publish_messages(file_name, topic_name, delay)
    print(" Publishing completed")