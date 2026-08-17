import json
import os
import time
from datetime import UTC, datetime

from dotenv import load_dotenv
from kafka import KafkaProducer


load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC = os.getenv("KAFKA_TOPIC")


def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda value: json.dumps(value).encode("utf-8")
            )
            print("Connected to Kafka")
            return producer

        except Exception as exc:
            print(f"Kafka not ready yet: {exc}")
            time.sleep(5)


invalid_transactions = [
    {
        "test_case": "missing_transaction_id",
        "transaction_id": None,
        "customer_id": 1001,
        "merchant": "TestMerchant",
        "amount": 120.50,
        "currency": "PLN",
        "country": "PL",
        "payment_method": "CARD",
        "fraud_flag": False,
        "event_timestamp": datetime.now(UTC).isoformat()
    },
    {
        "test_case": "non_positive_amount",
        "transaction_id": "INVALID-NEGATIVE-AMOUNT",
        "customer_id": 1002,
        "merchant": "TestMerchant",
        "amount": -50.00,
        "currency": "PLN",
        "country": "PL",
        "payment_method": "BLIK",
        "fraud_flag": False,
        "event_timestamp": datetime.now(UTC).isoformat()
    },
    {
        "test_case": "missing_merchant",
        "transaction_id": "INVALID-MISSING-MERCHANT",
        "customer_id": 1003,
        "merchant": None,
        "amount": 75.00,
        "currency": "PLN",
        "country": "PL",
        "payment_method": "TRANSFER",
        "fraud_flag": False,
        "event_timestamp": datetime.now(UTC).isoformat()
    },
    {
        "test_case": "invalid_event_timestamp",
        "transaction_id": "INVALID-BAD-TIMESTAMP",
        "customer_id": 1004,
        "merchant": "TestMerchant",
        "amount": 200.00,
        "currency": "PLN",
        "country": "PL",
        "payment_method": "APPLE_PAY",
        "fraud_flag": False,
        "event_timestamp": "not-a-valid-timestamp"
    },
    {
        "test_case": "multiple_missing_fields",
        "transaction_id": "INVALID-MULTIPLE-FIELDS",
        "customer_id": None,
        "merchant": "TestMerchant",
        "amount": None,
        "currency": None,
        "country": "PL",
        "payment_method": None,
        "fraud_flag": None,
        "event_timestamp": datetime.now(UTC).isoformat()
    },
]


if __name__ == "__main__":
    producer = create_producer()

    for transaction in invalid_transactions:
        producer.send(TOPIC, value=transaction)
        print(f"Sent invalid transaction: {transaction['test_case']}")

    producer.flush()
    producer.close()

    print(f"Sent {len(invalid_transactions)} invalid transactions to topic: {TOPIC}")
