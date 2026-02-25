import os

import requests # type: ignore
from datetime import datetime
from typing import List, Dict
import pandas as pd # type: ignore
from dotenv import load_dotenv # type: ignore
import logging
import time

logger = logging.getLogger(__name__)
load_dotenv()

CREDIT_API_BASE_URL = os.getenv("CREDIT_API_BASE_URL")

if not CREDIT_API_BASE_URL:
    raise EnvironmentError("CREDIT_API_BASE_URL is not set")


def fetch_credit_score(
    customer_id: str, 
    timeout: int = 5,
    max_retries: int = 3,
    retry_delay: float = 2.0
) -> dict:
    
    """
    Fetch a single customer's credit score from the mock credit bureau API.

    Retries up to 3 times with exponential backoff on transient failures.
    """

    url = f"{CREDIT_API_BASE_URL}/{customer_id}"

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(
                url, 
                timeout=timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code in [400, 401, 403, 404]:
                logger.warning(f"Customer {customer_id} not found or invalid request: {e}")
                return None
            raise
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            logger.warning(f"Network error for customer {customer_id}: {e}")

        if attempt < max_retries:
            wait_time = retry_delay * (2 ** (attempt - 1))  # exponential backoff
            logger.info(f"Retrying in {wait_time:.1f}s...")
            time.sleep(wait_time)



def extract_credit_scores(customer_ids: List[str]) -> dict:
    """
    Extract credit scores for a list of customers.

    Args:
        customer_ids: list of customer_id strings to fetch credit data for

    Returns:
        dict with:
            - 'data': pandas DataFrame of credit scores
            - 'metadata': dict of extraction info
    """
    start_time = datetime.now()
    records = []

    logger.info(f"Fetching credit scores for {len(customer_ids)} customers...")

    for customer_id in customer_ids:
        credit_data = fetch_credit_score(customer_id)
        if credit_data:
            records.append(credit_data)
        else:
            logger.debug(f"No credit data returned for customer {customer_id}")

    

    df = pd.DataFrame(records)

    logger.info(f"Extracted credit scores for {len(df)} customers")

    metadata = {
        'source': 'mock_credit_api',
        'records_requested': len(customer_ids),
        'records_extracted': len(df),
        'extracted_at': start_time.isoformat(),
        'duration_seconds': (datetime.now() - start_time).total_seconds(),
    }

    return {'data': df, 'metadata': metadata}


