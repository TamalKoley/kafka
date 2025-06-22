from confluent_kafka import Producer
import json
from datetime import datetime

timestamp = datetime.now().strftime('%Y-%m-%d %H-%M-%S')   # current date and time
log_file=f"D:\projects\kafka\kafka_using_python\kafka-producer-svc-py\logs\producer_{timestamp}.log"


# Define a delivery report callback
def delivery_report(err, msg):
    if err is not None:
        log_message=f"Message delivery failed: {err}"
    else:
        log_message=f"Message delivered to {msg.topic()} [{msg.partition()}]"
    with open(log_file, 'a') as f:
        f.write(log_message + '\n')

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker
    'client.id': 'python-producer'
}

msg={'name':'John','age':25}
json_msg=json.dumps(msg);

# Create Producer instance
producer = Producer(conf)

# Produce a message
producer.produce('my_topic1', key='key', value=json_msg, callback=delivery_report)

# Wait for any outstanding messages to be delivered
producer.flush()
