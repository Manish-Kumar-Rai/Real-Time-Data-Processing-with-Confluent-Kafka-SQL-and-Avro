from config.logger import get_logger

logger = get_logger(__name__)

def transform_record(record):
    '''
    Apply business logic transformations:
    - Convert category to uppercase.
    - Apply 10% discount if category is ELECTRONICS.
    '''

    try:
        transformed = record.copy()
        transformed['category'] = transformed['category'].upper()

        if transformed['category'] == 'ELECTRONICS':
            original_price = transformed['price']
            transformed['price'] = round(original_price * 0.9,2) # 10% discount

        return transformed
    
    except Exception as e:
        logger.error(f'Failed to tranform record: {record} | Error: {e}')
        return record
