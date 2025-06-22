from confluent_kafka import Consumer, KafkaException, KafkaError
import json
from pymongo import MongoClient
from datetime import datetime

timestamp = datetime.now().strftime("%Y-%m-%d %H-%M-%S")    # Current timestamp
log_file = f"D:\projects\kafka\kafka_using_python\kafka-consumer-svc-py\Logs\consumer_{timestamp}.log"                 # Log file name

def print_log(log_message):
    with open(log_file, 'a') as log:
        log.write(f"{log_message}\n")

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker
    'group.id': 'python-consumer-group',     # Consumer group ID
    'auto.offset.reset': 'earliest'          # Start reading from the earliest message
}

#mongo db connection
client = MongoClient('localhost', 27017)
db = client['KafkaDB']
collection = db['kafkaTopicData']

# Create Consumer instance
consumer = Consumer(conf)

# Subscribe to the topic
consumer.subscribe(['my_topic1'])

# Poll for messages
try:
    while True:
        msg = consumer.poll(timeout=1.0)  # Timeout after 1 second
        if msg is None:
            print_log("no more messages")
            break
        elif msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print_log(f"End of partition reached {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
            else:
                raise KafkaException(msg.error())
        else:
            print_log(f"Received message: {json.loads(msg.value().decode('utf-8'))}")
            print_log(f"Message key: {msg.key().decode('utf-8')}")
            msg=json.loads(msg.value().decode('utf-8'))
            collection.insert_one(msg)
            print_log("Message saved to MongoDB")

except KeyboardInterrupt:
    print_log("Terminating the consumer.")

finally:
    # Close the consumer connection
    consumer.close()
