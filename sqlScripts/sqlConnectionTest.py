import os
import yaml
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Load config.yaml
with open('..\config\config.yaml','r') as f:
    config = yaml.safe_load(f)

# Read actual values from .env using keys from YAML
driver = config['database']['driver']
server = os.getenv(config['database']['server_env'])
database = os.getenv(config['database']['name_env'])
username = os.getenv(config['database']['user_env'])
password = os.getenv(config['database']['password_env'])

print(f"Connecting to SQL SERVER at: {server}, DB: {database}")