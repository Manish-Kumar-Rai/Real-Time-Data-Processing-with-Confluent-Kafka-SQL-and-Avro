# ---------- Simple Python script to auto-generate sample   ----------
import threading
import random
import datetime
import time
from faker import Faker
from kafka_producer.mssql_connector import MSSQLConnector
from config.logger import get_logger

logger = get_logger(__name__)


def insert_fake_products(n=5):
    fake = Faker()
    categories = ['Electronics', 'Furniture', 'Stationery', 'Home Decor', 'Grocery']

    try:
        connector = MSSQLConnector()
        connector.get_connection()
        connector.conn.autocommit = True
        cursor = connector.conn.cursor()

        last_record = connector.fetch_last_record()
        last_id = last_record[0][0] if last_record else 0
        base_time = last_record[0][4] if last_record else datetime.datetime.now() - datetime.timedelta(days=90)
        total_seconds = 30*24*3600

        # Generate random seconds and sort them for incremental timestamps
        random_seconds = sorted(random.randint(0,total_seconds) for _ in range(n+1))
        
        products = []
        for i in range(1,n+1):
            name = fake.name()
            category = random.choice(categories)
            price = round(random.uniform(250.0,6578.0),2)
            clean_last_updated = (base_time + datetime.timedelta(seconds=random_seconds[i])).replace(microsecond=0)
            str_last_updated = clean_last_updated.strftime('%Y-%m-%d %H:%M:%S')
            products.append((i + last_id,name,category,price,str_last_updated))
        
        insert_query = '''
                     INSERT INTO products(id,name,category,price,last_updated)
                     VALUES(?,?,?,?,?)   
                    '''
        
        cursor.executemany(insert_query,products)
        logger.info(f"Inserted {n} fake products into the 'products' table.")
    except Exception as err:
        logger.error(f'[Feeder Error] {err}')

    finally:
        if connector.conn:
            cursor.close()
            connector.close_connection()


def start_feeder(interval_sec=10,batch_size=5):
    def feeder_loop():
        while True:
            insert_fake_products(n=batch_size)
            time.sleep(interval_sec)

    feeder_thread = threading.Thread(target=feeder_loop,daemon=True)
    feeder_thread.start()
    logger.info(f'Auto Feeder Started - inserts {batch_size} records every {interval_sec} seconds.')

if __name__ == '__main__':
    start_feeder(interval_sec=5,batch_size=5)

    try:
        while True:
            time.sleep(60) # Keeps main thread alive

    except KeyboardInterrupt:
        logger.info('Feeder stopped.')

