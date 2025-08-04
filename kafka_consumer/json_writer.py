import json
from config.logger import get_logger

logger = get_logger(__name__)

def write_json_to_file(data,filename):
    '''
    Append a single JSON object to a file in newline-delimited JSON format.
    '''
    try:
        with open(filename,'a') as f:
            json.dump(data,f)
            f.write('\n')
    
    except Exception as e:
        logger.error(f"Couldn't write to {filename} | Error: {e}")