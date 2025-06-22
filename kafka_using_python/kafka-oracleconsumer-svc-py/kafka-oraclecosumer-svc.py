from confluent_kafka import Consumer, KafkaException, KafkaError
import datetime
import cx_Oracle
import os
import glob
import time

def print_log(log_message):
    with open(Log_file, 'a') as log:
        log.write(f"{log_message}\n")

def clear_logs():
    # keep only 5 latest log files
    log_files = glob.glob('D:\projects\kafka\kafka_using_python\kafka-oracleconsumer-svc-py\Logs\consumer*.log')
    log_files.sort(key=os.path.getctime, reverse=True)

    for log_file in log_files[5:]:
        os.remove(log_file)

timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S")    # Current timestamp
Log_file = f"D:\projects\kafka\kafka_using_python\kafka-oracleconsumer-svc-py\Logs\consumer_{timestamp}.log"                 # Log file name

# Kafka configuration
kafka_conf = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker
    'group.id': 'python-consumer-group',     # Consumer group ID
    'auto.offset.reset': 'earliest'          # Start reading from the earliest message
}

# Create Consumer instance
consumer = Consumer(kafka_conf)
consumer.subscribe(['custom_kafka_py'])

#create oracle connection
oracle_dsc = cx_Oracle.makedsn('localhost', 1521, service_name='ORCL')
oracle_user = 'system'
oracle_password = '12345'
oracle_connection = cx_Oracle.connect(user=oracle_user, password=oracle_password, dsn=oracle_dsc)
oracle_cursor = oracle_connection.cursor()



try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            print_log("no more messages")
            break
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                 print_log(f"End of partition reached {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                 break
            else:
                raise KafkaException(msg.error())
        # Process message
        message_value = msg.value().decode('utf-8')
        print(f'Received message: {message_value}')
        print_log(f'Received message: {message_value}')
        # Insert into Oracle DB
        insert_query = "INSERT INTO my_table (column_name) VALUES (:1)"
        # oracle_cursor.execute(insert_query, [message_value])
        # oracle_connection.commit()
        print('Message inserted into Oracle DB')

except Exception as e:
    print(f'Error: {e}')
finally:
    # Close connections
    oracle_cursor.close()
    oracle_connection.close()
    consumer.close()
    clear_logs()