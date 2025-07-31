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

class MSSQLConnector:
    def __init__(self,config_path = config_path):

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
        # Define connection string for MSSQL Server
        conn_str = (
            f'DRIVER={self.driver};'
            f'SERVER={self.server};'
            f'DATABASE={self.database};'
            f'UID={self.username};'
            f'PWD={self.password}'
        )

        try:
            conn = pyodbc.connect(conn_str)
            logging.info(f'Connected to server: {self.server}, database: {self.database}')
            return conn
        except pyodbc.Error as err:
            logging.error(f'Connecion Error: {err}')
            return None
        
