import pyodbc
import os
import yaml
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

try:
    conn = pyodbc.connect(conn_str,autocommit=True,timeout=2)
    print(f'Connected successfully to MS SQL Server on {server}, database: {database}')

    cursor = conn.cursor()
    # Create the database if it doesn't exist
    cursor.execute('''
            IF DB_ID('GrowDataSkill') IS NULL
	            CREATE DATABASE GrowDataSkill;
            ''')
    
    # Switch to the database
    cursor.execute('USE GrowDataSkill')

    # Drop the table if it exists
    cursor.execute('''
            IF OBJECT_ID('products', 'U') IS NOT NULL
                DROP TABLE products;
            ''')

    # Create the product table
    cursor.execute('''
            CREATE TABLE products (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                category VARCHAR(50) NOT NULL,
                price MONEY CHECK(price >=0),
                last_updated DATETIME DEFAULT GETDATE()
            );
            ''')
    
    # Optional: Add an index on last_updated for faster incremental fetches
    cursor.execute('CREATE INDEX idx_last_updated ON products(last_updated);')

    # Optional: view the table
    cursor.execute('SELECT * FROM products;')

    # Print Data
    rows = cursor.fetchall()
    for row in rows:
        print(row,'1')

except pyodbc.Error as err:
    print(f'Error: {err}')
    conn = None

finally:
    if conn:
        cursor.close()
        conn.close()
