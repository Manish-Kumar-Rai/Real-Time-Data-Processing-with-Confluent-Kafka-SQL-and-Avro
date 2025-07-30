# ---------- Simple Python script to auto-generate sample   ----------

import pyodbc
import yaml
import os
import random
from faker import Faker
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Load config.yaml
with open(r'..\config\config.yaml','r') as f:
    config = yaml.safe_load(f)

# Read actual values from .env using keys from YAML
driver = config['database']['driver']
server = os.getenv(config['database']['server_env'])
database = os.getenv(config['database']['name_env'])
username = os.getenv(config['database']['user_env'])
password = os.getenv(config['database']['password_env'])


# Define connection string for MSSQL Server
conn_str = (
    f'DRIVER={driver};'
    f'SERVER={server};'
    f'DATABASE={database};'
    f'UID={username};'
    f'PWD={password}'
)


def insert_sample_data(n):
    fake = Faker()
    categories = ['Electronics', 'Furniture', 'Stationery', 'Home Decor', 'Grocery']

    try:
        conn = pyodbc.connect(conn_str,autocommit=True)
        print(f'Connected successfully to MS SQL Server on {server}, database: {database}')
        cursor = conn.cursor()

        products = []

        for i in range(1,n+1):
            name = fake.name()
            category = random.choice(categories)
            price = round(random.uniform(250.0,6578.0),2)
            last_updated = fake.date_time_between(start_date='-30d',end_date='now')
            products.append((i,name,category,price,last_updated))
        
        insert_query = '''
                     INSERT INTO products(id,name,category,price,last_updated)
                     VALUES(?,?,?,?,?)   
                    '''
        
        cursor.executemany(insert_query,products)
        print(f"Inserted {n} fake products into the 'products' table.")
    except pyodbc.Error as err:
        print(f'Error: {err}')

    finally:
        if conn:
            cursor.close()
            conn.close()

# Inserting Samples
insert_sample_data(500)