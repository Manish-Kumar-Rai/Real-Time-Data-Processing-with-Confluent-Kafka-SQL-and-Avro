import os
import yaml
import pyodbc
import logging
from dotenv import load_dotenv

# Get the absolute path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Build absolute path to config.yaml from script location
config_path = os.path.join(BASE_DIR,'..','config','config.yaml')
config_path = os.path.abspath(config_path)

CHECKPOINT_PATH = os.path.join(BASE_DIR,'checkpoints')
CHECKPOINT_PATH = os.path.abspath(CHECKPOINT_PATH)
os.makedirs(CHECKPOINT_PATH,exist_ok=True)

class MSSQLConnector:
    def __init__(self,config_path = config_path):

        self.conn = None
        # Load environment variables from .env
        load_dotenv()

        # Setup logging
        logging.basicConfig(level=logging.INFO,format='[%(levelname)s] %(message)s')

        # Load config.yaml
        try:
            with open(config_path,'r') as f:
                self.config = yaml.safe_load(f)
        except Exception as e:
            logging.error(f'Failed to load config file: {e}')
            raise

        # Read actual values from .env using keys from YAML
        db_config = self.config.get('database',{})
        self.driver = db_config.get('driver')
        self.server = os.getenv(db_config.get('server_env'))
        self.database = os.getenv(db_config.get('name_env'))
        self.username = os.getenv(db_config.get('user_env'))
        self.password = os.getenv(db_config.get('password_env'))

    def get_connection(self):
        '''Establish and return a database connection.'''
        if self.conn:
            return self.conn

        # Define connection string for MSSQL Server
        conn_str = (
            f'DRIVER={self.driver};'
            f'SERVER={self.server};'
            f'DATABASE={self.database};'
            f'UID={self.username};'
            f'PWD={self.password}'
        )

        try:
            self.conn = pyodbc.connect(conn_str)
            logging.info(f'Connected to server: {self.server}, database: {self.database}')
            return self.conn
        except pyodbc.Error as err:
            logging.error(f'Connecion Error: {err}')
            return None
        
    def read_last_fetched(self):
        '''Read the last fetched timestamp from file, or return default.'''
        path=os.path.join(CHECKPOINT_PATH, 'last_fetched.txt')
        try:
            if os.path.exists(path):
                with open(path,'r') as f:
                    timestamp = f.read().strip()
                    logging.info(f'Last fetched timestamp read: {timestamp}')
                    return timestamp
            else:
                default = '1900-01-01 00:00:00'
                logging.info(f'Checkpoint not found. Using default: {default}')
                return default
        except Exception as e:
            logging.error(f'Failed to read checkpoint: {e}')
            return default
        
    def write_last_fetched(self,timestamp):
        '''Write the latest timestamp to checkpoint file.'''
        path=os.path.join(CHECKPOINT_PATH, 'last_fetched.txt')
        try:
            with open(path,'w') as f:
                f.write(timestamp)
                logging.info(f'Updated last fetched timestamp: {timestamp}')
        except Exception as e:
            logging.error(f'Failed to write checkpoint: {e}')

    def fetch_incremental_data(self):
        path=os.path.join(CHECKPOINT_PATH, 'last_fetched.txt')
        conn = self.get_connection()
        if conn is None:
            return []
        
        cursor = conn.cursor()
        last_fetched = self.read_last_fetched()
        try:
            query = '''
                SELECT id,name,category,last_updated
                FROM products
                WHERE last_updated > ?
                ORDER BY last_updated ASC
            '''
            cursor.execute(query,last_fetched)
            rows = cursor.fetchall()

            if rows:
                latest_timestamp = max([row[-1] for row in rows]).strftime('%Y-%m-%d %H:%M:%S')
                self.write_last_fetched(latest_timestamp)
                return rows
            else:
                logging.info('No new records found.')
                return []
        
        except pyodbc.Error as e:
            logging.error(f'Query Failed: {e}')
            return []
        
    def fetch_last_record(self):
        '''Read the last record from the product table'''
        
        self.conn = self.conn or self.get_connection()
        if not self.conn:
            return []
        
        cursor = self.conn.cursor()

        try:
            query = '''
                    SELECT 
                        TOP 1 id,name,category,last_updated
                    FROM products
                    ORDER BY id DESC
                '''
            cursor.execute(query)
            row = list(cursor.fetchall())
            return row
        except pyodbc.Error as e:
            logging.error(f'Query failed: {e}')
            return []
        
    def close_connection(self):
        if self.conn:
            self.conn.close()
            self.conn = None
            logging.info('Database connection closed.')
        
if __name__ == '__main__':
    connector = MSSQLConnector()
    rows = connector.fetch_incremental_data()
    for row in rows:
        print(row)