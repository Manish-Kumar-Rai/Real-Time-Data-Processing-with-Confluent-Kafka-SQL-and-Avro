import os
import time
from config import logger
from kafka_producer.mssql_connector import MSSQLConnector

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

connector = MSSQLConnector()
config = connector.get_config()
kafka_config = config.get('kafka',{})
kafka_schema_config = config.get('kafka_schema',{})
logger = logger.get_logger(__name__)

def transform_row_to_avro(row):
    '''
    Convert MSSQL row (tuple) into a dictionary matching Avro schema.
    Handles timestamp conversion to milliseconds.
    '''
    try:
        id_val = row[0]
        name_val = row[1]
        category_val = row[2]
        price_val = float(row[3])
        last_updated_dt = row[4]

        # Convert datetime to UNIX timestamp in milliseconds
        last_updated_millis = int(last_updated_dt.timestamp() * 1000)

        return {
            'id': id_val,
            'name': name_val,
            'category': category_val,
            'price': price_val,
            'last_updated':last_updated_millis
        }
    
    except Exception as err:
        logger.error(f'[Transform Error] Failed to convert row: {row} | Error: {err}]')
        return None
    
def delivery_report(err,msg):
    '''
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.

    '''
    if err is not None:
        logger.error("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    logger.info('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))
    

# Define Kafka configuration

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

# Create Avro Serializer for the value
key_serializer = StringSerializer()

# with open('confluentkakfka_avro_schema.avsc','r') as f:
#     schema_str = f.read()

avro_serializer = AvroSerializer(schema_registry_client,schema_str)  # I can put my .avsc file here if i haven't manually uploded on confluent kafka schema registry

# Define the SerializingProducer
producer = SerializingProducer({
    'bootstrap.servers': os.getenv(kafka_config['bootstrap_servers_env']),
    'security.protocol': os.getenv(kafka_config['security_protocol_env']),
    'sasl.mechanisms': os.getenv(kafka_config['sasl_mechanisms_env']),
    'sasl.username': os.getenv(kafka_config['username_env']),
    'sasl.password': os.getenv(kafka_config['password_env']),
    'key.serializer': key_serializer,
    'value.serializer': avro_serializer
})

rows = connector.fetch_incremental_data()

for row in rows:
    avro_record = transform_row_to_avro(row)
    if avro_record:
        producer.produce(
            topic='product_updates',
            key=str(avro_record['id']),
            value=avro_record,
            on_delivery=delivery_report
        )
    time.sleep(2)

producer.flush()
logger.info('All Data successfully published to Kafka')