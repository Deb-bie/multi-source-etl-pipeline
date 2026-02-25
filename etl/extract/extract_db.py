import pandas as pd # type: ignore
from datetime import datetime
import logging
from typing import Optional
from sqlalchemy import create_engine, text # type: ignore


logger = logging.getLogger(__name__)


def get_engine(host: str, port: int, dbname: str, user: str, password: str):
    """
    Create a SQLAlchemy engine with connection pooling.
    """
    connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    
    engine = create_engine(
        connection_string,
        pool_size=5,        
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=3600,
    )
    
    logger.info(f"Database engine created: {host}:{port}/{dbname}")
    return engine


def extract_customers_from_db(
    engine,
    schema: str = "crm",
    watermark_date: Optional[str] = None,
    batch_size: int = 10000
) -> dict:
    """
    Extract customers from PostgreSQL database.

    Args:
        engine:         SQLAlchemy engine connected to source DB
        schema:         Name of the source schema
        watermark_date: 'YYYY-MM-DD' — only extract customers created on/after this date
        batch_size:     Number of rows per chunk to fetch

    Returns:
        dict: {
            'data': pandas DataFrame of extracted customers,
            'metadata': dict of extraction info
        }
    """
    start_time = datetime.now()
    chunks = []
    total_rows = 0


    base_query = f"""
        SELECT 
            customer_id,
            first_name,
            last_name,
            email,
            city,
            state,
            risk_segment,
            created_at
        FROM {schema}.customers
    """

    if watermark_date:
        query = base_query + " WHERE created_at >= :watermark ORDER BY created_at ASC"
        params = {'watermark': watermark_date}
        logger.info(f"Incremental extraction: customers created from {watermark_date} onwards")
    else:
        query = base_query + " ORDER BY created_at ASC"
        params = {}
        logger.info("Full extraction: all customers from source DB")
    
    # ── CHUNKED READING ──────────────────────────────────────────
    
    with engine.connect() as conn:
        for chunk in pd.read_sql(text(query), conn, params=params, chunksize=batch_size):
            chunks.append(chunk)
            total_rows += len(chunk)
            logger.debug(f"Read chunk: {total_rows} rows so far")
    
    if not chunks:
        df = pd.DataFrame()
        logger.warning("No rows extracted from database")
    else:
        df = pd.concat(chunks, ignore_index=True)
    

    metadata = {
        'source':            'postgresql',
        'schema':            schema,
        'table':             'customers',
        'watermark':         watermark_date,
        'records_extracted': total_rows,
        'extracted_at':      start_time.isoformat(),
        'duration_seconds':  (datetime.now() - start_time).total_seconds(),
    }

    logger.info(f"Customer extraction complete: {total_rows} rows in {metadata['duration_seconds']:.2f}s")
    
    return {
        'data': df,
        'metadata': metadata
    }