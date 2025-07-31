from kafka_producer.mssql_connector import MSSQLConnector

try:
    connector = MSSQLConnector()
    conn = connector.get_connection()
    conn.autocommit = True

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

except Exception as e:
    print(f'Error: {e}')

finally:
    if conn:
        cursor.close()
        conn.close()
