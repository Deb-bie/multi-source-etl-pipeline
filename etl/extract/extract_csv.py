import os
import pandas as pd # type: ignore
from datetime import datetime
import logging


logger = logging.getLogger(__name__)


def extract_csv_data(filepath: str) -> dict:
    """
    Extract fraud watchlist data from a CSV file.
    
    Returns a dict containing:
        - data: pandas DataFrame with raw records
        - metadata: extraction stats for audit logging
    """
    start_time = datetime.now()
    logger.info(f"Starting CSV extraction from: {filepath}")
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"CSV file not found: {filepath}")
    
    try:
        df = pd.read_csv(
            filepath,
            dtype={
                'entity_id':   str,
                'entity_type': str,
                'reason':      str,
                'listed_date': str,
            },
           
            parse_dates=['listed_date'],
        )
        
        row_count = len(df)
        logger.info(f"Successfully extracted {row_count} rows from CSV")
        
        return {
            'data': df,
            'metadata': {
                'source': 'csv',
                'filepath': filepath,
                'records_extracted': row_count,
                'columns': list(df.columns),
                'extracted_at': start_time.isoformat(),
                'duration_seconds': (datetime.now() - start_time).total_seconds(),
            }
        }
        
    except pd.errors.ParserError as e:
        logger.error(f"Failed to parse CSV: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during CSV extraction: {e}")
        raise

