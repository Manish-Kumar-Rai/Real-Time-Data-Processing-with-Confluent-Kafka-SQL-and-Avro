# ---------- Simple Python script to auto-generate sample   ----------

import random
import datetime
from faker import Faker
from kafka_producer.mssql_connector import MSSQLConnector


def insert_sample_data(n):
    fake = Faker()
    categories = ['Electronics', 'Furniture', 'Stationery', 'Home Decor', 'Grocery']

    try:
        connector = MSSQLConnector()
        connector.get_connection()
        connector.conn.autocommit = True
        cursor = connector.conn.cursor()
        last_record = connector.fetch_last_record()

        products = []
        last_id = last_record[0][0] if last_record else 0
        base_time = last_record[0][3] if last_record else datetime.datetime.now() - datetime.timedelta(days=90)
        total_seconds = 30*24*3600
        # Generate 500 random seconds and sort them for incremental timestamps
        random_seconds = sorted(random.randint(0,total_seconds) for _ in range(n+1))
        
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
        print(f"Inserted {n} fake products into the 'products' table.")
    except Exception as err:
        print(f'Error: {err}')

    finally:
        if connector.conn:
            cursor.close()
            connector.close_connection()

# Inserting Samples
insert_sample_data(5)