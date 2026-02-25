from datetime import datetime
from kafka import KafkaConsumer # type: ignore
import json
import pandas as pd # type: ignore
import logging

logger = logging.getLogger(__name__)

def extract_transaction_stream(
    topic: str,
    bootstrap_servers: str,
    max_messages: int = 1000,
    consumer_group: str = "etl_transaction_consumer"
) -> dict:
    """
    Extract transactions from a Kafka stream in micro-batches.

    Args:
        topic: Kafka topic to consume from
        bootstrap_servers: Kafka broker(s)
        max_messages: Max number of messages to consume in one run
        consumer_group: Kafka consumer group id

    Returns:
        dict:
            - data: pandas DataFrame of transactions
            - metadata: extraction metadata
    """
    start_time = datetime.now()

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=consumer_group,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

    records = []

    for i, message in enumerate(consumer):
        records.append(message.value)
        if i + 1 >= max_messages:
            break

    consumer.close()

    df = pd.DataFrame(records)

    metadata = {
        "source": "kafka_transaction_stream",
        "topic": topic,
        "consumer_group": consumer_group,
        "records_extracted": len(df),
        "max_messages": max_messages,
        "extracted_at": start_time.isoformat(),
        "duration_seconds": (datetime.now() - start_time).total_seconds(),
    }

    logger.info(
        f"Kafka extraction complete | topic={topic} | "
        f"records={len(df)} | duration={metadata['duration_seconds']:.2f}s"
    )

    return {
        "data": df,
        "metadata": metadata
    }

