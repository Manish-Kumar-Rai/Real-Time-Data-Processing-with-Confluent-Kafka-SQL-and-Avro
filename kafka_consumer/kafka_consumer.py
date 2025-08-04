import os
import threading
from time import sleep
import json
from config.logger import get_logger
from kafka_consumer.data_transform import transform_record
from kafka_consumer.json_writer import write_json_to_file
from kafka_producer.mssql_connector import MSSQLConnector

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

connector = MSSQLConnector()
config = connector.get_config()
kafka_config = config.get('kafka',{})
kafka_schema_config = config.get('kafka_schema',{})
logger = get_logger(__name__)


# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
    'url': os.getenv(kafka_schema_config['schema_registry_url']),
    'basic.auth.user.info': '{}:{}'.format(
        os.getenv(kafka_schema_config['schema_username']),
        os.getenv(kafka_schema_config['schema_password'])
    )
})

# Fetch the latest Avro schema for the value
subject_name = 'product_updates-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro DeSerializer for the value
key_deserializer = StringDeserializer('utf-8')
avro_deserializer = AvroDeserializer(schema_registry_client,schema_str)

def consumer_worker(consumer_id):

    # Define the DeserializingConsumer
    consumer = DeserializingConsumer({
        'bootstrap.servers': os.getenv(kafka_config['bootstrap_servers_env']),
        'security.protocol': os.getenv(kafka_config['security_protocol_env']),
        'sasl.mechanisms': os.getenv(kafka_config['sasl_mechanisms_env']),
        'sasl.username': os.getenv(kafka_config['username_env']),
        'sasl.password':os.getenv(kafka_config['password_env']),
        'key.deserializer': key_deserializer,
        'value.deserializer':avro_deserializer,
        'group.id':'group1',
        'auto.offset.reset':'earliest'
    })

    # Subscribe to the 'product_updates' topic
    consumer.subscribe(['product_updates'])
    logger.info(f'[{consumer_id}] started consuming...')

    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            if msg.error():
                logger.error(f'[{consumer_id}] Error: {msg.error()}')
                continue

            record = msg.value()
            if record:
                transform = transform_record(record)
                write_json_to_file(transform,f'consumer_output_{consumer_id}.json')
    
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        logger.info(f'[{consumer_id}] stopped consuming.')

if __name__ == '__main__':
    threads = []
    for i in range(1,6):
        t = threading.Thread(target=consumer_worker,args=(i,),daemon=True)
        threads.append(t)
        t.start()
    
    try:
        while True:
            sleep(2)
    except KeyboardInterrupt:
        logger.info('Shutting down consumers...')

