import psycopg2; # PostgreSQL adapter for Python

from confluent_kafka import Producer
import json
from datetime import datetime
from decimal import Decimal

timestamp= datetime.now().strftime("%Y%m%d-%H%M%S")
log_file = f'D:\projects\kafka\kafka_using_python\kafka-prostgraseproducer-svc-py\logs\kafka_producer_logs_{timestamp}.log'
conf = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker
    'client.id': 'python-producer'
}

def delivery_report(err, msg):
    if err is not None:
        log_message=f"Message delivery failed: {err}"
    else:
        log_message=f"Message delivered to {msg.topic()} [{msg.partition()}]"
    with open(log_file, 'a') as f:
        f.write(log_message + '\n')
        # Open a cursor to perform database operations

def deimalDefault(obj):
    if isinstance(obj, Decimal):
        return float(obj)    

def log_msg(msg):
    with open(log_file, 'a') as f:
        f.write(msg + '\n')

def read_data_from_postgresql():  # Function to read data from PostgreSQL
    try:
        # Connect to your postgres DB
        connection = psycopg2.connect(
            dbname="test_db",
            user="postgres",
            password="Bubai@1996",
            host="localhost",
            port="5432"
        )

        # Open a cursor to perform database operations
        cursor = connection.cursor()

        # Execute a query
        cursor.execute("SELECT * FROM public.employees")

        # Retrieve query results
        records = cursor.fetchall()
        producer=Producer(conf) # Create a producer instance
        for record in records:
            log_msg(f"Record: {record}")
            msg={'EMPLOYEE_ID':record[0],'FIRST_NAME':record[1],'LAST_NAME':record[2],'HIRE_DATE':'2025-02-19','SALARY':record[4]}
            producer.produce('custom_kafka_py', key='key', value=json.dumps(msg,default=deimalDefault),callback=delivery_report)
            producer.flush()
            log_msg(f"Message published successfully")

    except Exception as error:
        log_msg(f"Error reading data from PostgreSQL table: {error}")

    finally:
        # Close communication with the database
        if connection:
            cursor.close()
            connection.close()

# Call the function to read data
read_data_from_postgresql()