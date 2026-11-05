# isort: skip_file
"""
Consumes clickstream events from Kafka topic with fault tolerance
"""

import json
import os
import sys
from datetime import datetime
from typing import Dict, List

import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaConsumer

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.ingestion_helpers import generate_batch_id  # noqa: E402
from utils.ingestion_helpers import get_mysql_engine  # noqa: E402
from utils.ingestion_helpers import (  # noqa: E402
    complete_pipeline_run,
    setup_logging,
    start_pipeline_run,
)

load_dotenv()

# ============================================
# CONFIGURATION
# ============================================

BATCH_SIZE = 1000  # Number of events before writing to MySQL
CONSUMER_GROUP = "ecommerce_consumer_group"

# ============================================
# KAFKA CONSUMER
# ============================================


def create_consumer(topic: str, group_id: str = CONSUMER_GROUP):
    """Create Kafka consumer with fault tolerance"""
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="earliest",  # Start from earliest if no offset
        enable_auto_commit=False,  # Manual commit after processing
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        key_deserializer=lambda m: m.decode("utf-8") if m else None,
    )


# ============================================
# MAIN KAFKA INGESTION
# ============================================


def consume_clickstream(
    topic: str = "clickstream_events",
    batch_size: int = BATCH_SIZE,
    max_messages: int = None,
    logger=None,
):
    """
    Consume clickstream events from Kafka and write to MySQL

    Args:
        topic: Kafka topic name
        batch_size: Number of events to batch before writing
        max_messages: Maximum messages to consume (None = infinite)
        logger: Logger instance
    """
    if logger is None:
        logger = setup_logging("ingest_kafka")

    batch_id = generate_batch_id()
    run_id = start_pipeline_run("ingest_kafka", "kafka_stream", batch_id)

    logger.info(
        "Starting Kafka consumer",
        extra={"extra": {"topic": topic, "batch_id": batch_id, "run_id": run_id}},
    )

    consumer = create_consumer(topic)
    engine = get_mysql_engine()

    events_buffer = []
    total_events = 0

    try:
        for message in consumer:
            event = message.value

            # Add metadata
            event["ingested_at"] = datetime.now().isoformat()
            event["batch_id"] = batch_id
            event["is_processed"] = False

            events_buffer.append(event)
            total_events += 1

            # Write when buffer reaches batch_size
            if len(events_buffer) >= batch_size:
                write_batch_to_mysql(events_buffer, engine, logger)
                events_buffer = []

                # Commit offsets after successful write
                consumer.commit()

                logger.info(
                    "Batch processed",
                    extra={
                        "extra": {
                            "batch_size": batch_size,
                            "total_events": total_events,
                        }
                    },
                )

            # Stop if max_messages reached
            if max_messages and total_events >= max_messages:
                break

    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    except Exception as e:
        logger.error("Kafka consumer error", extra={"extra": {"error": str(e)}})
        complete_pipeline_run(run_id, "failed", error_message=str(e))
        raise
    finally:
        # Write any remaining events
        if events_buffer:
            write_batch_to_mysql(events_buffer, engine, logger)
            consumer.commit()
            logger.info(
                "Final batch written",
                extra={"extra": {"remaining_events": len(events_buffer)}},
            )

        consumer.close()
        complete_pipeline_run(
            run_id, "success", rows_extracted=total_events, rows_loaded=total_events
        )

    return {
        "success": True,
        "run_id": run_id,
        "batch_id": batch_id,
        "total_events": total_events,
    }


def write_batch_to_mysql(events: List[Dict], engine, logger):
    """Write a batch of events to MySQL staging"""
    if not events:
        return

    df = pd.DataFrame(events)

    # Ensure all required columns exist
    required_cols = [
        "event_id",
        "session_id",
        "event_type",
        "page_url",
        "event_timestamp",
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    # Map to staging table
    df.rename(
        columns={
            "user_id": "user_id",
            "product_id": "product_id",
            "device_type": "device_type",
            "browser_name": "browser_name",
        },
        inplace=True,
    )

    try:
        df.to_sql("stg_raw_clickstream", engine, if_exists="append", index=False)
        logger.info("Batch written to MySQL", extra={"extra": {"batch_size": len(df)}})
    except Exception as e:
        logger.error(
            "Failed to write batch",
            extra={"extra": {"error": str(e), "batch_size": len(df)}},
        )
        raise


# ============================================
# COMMAND LINE EXECUTION
# ============================================

if __name__ == "__main__":
    logger = setup_logging("ingest_kafka")

    # Consume 10,000 events for testing
    result = consume_clickstream(
        topic="clickstream_events", max_messages=10000, logger=logger
    )
    print(json.dumps(result, indent=2))
