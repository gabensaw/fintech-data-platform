import json
import os
import random
import time
import uuid
from datetime import datetime, UTC
from dotenv import load_dotenv
from faker import Faker
from kafka import KafkaProducer

load_dotenv()

fake = Faker() # Faker XD LMAO

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC = os.getenv("KAFKA_TOPIC")

while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print("Connected to Kafka")
        break
    except Exception as e:
        print(f"Kafka not ready yet: {e}")
        time.sleep(5)

MERCHANTS = [
    "Amazon",
    "Allegro",
    "Netflix",
    "Spotify",
    "Steam",
    "Apple",
    "Uber"
]

PAYMENT_METHODS = [
    "CARD",
    "BLIK",
    "TRANSFER",
    "APPLE_PAY",
    "GOOGLE_PAY"
]


def generate_transaction():
    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": random.randint(1000, 9999),
        "merchant": random.choice(MERCHANTS),
        "amount": round(random.uniform(5, 5000), 2),
        "currency": "PLN",
        "country": "PL",
        "payment_method": random.choice(PAYMENT_METHODS),
        "fraud_flag": random.random() < 0.02,
        "timestamp": datetime.now(UTC).isoformat()
    }


while True:
    transaction = generate_transaction()

    producer.send(
        TOPIC,
        value=transaction
    )

    producer.flush()

    print(transaction)

    time.sleep(1)