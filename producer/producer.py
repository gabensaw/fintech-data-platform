import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, UTC
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC = os.getenv("KAFKA_TOPIC")

# --------------------------------------------------
# Kafka connection
# --------------------------------------------------

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

# --------------------------------------------------
# Merchant popularity
# --------------------------------------------------

MERCHANT_WEIGHTS = {
    "Allegro": 18,
    "Amazon": 15,
    "Biedronka": 12,
    "Lidl": 10,
    "Uber": 9,
    "Bolt": 8,
    "Pyszne": 8,
    "Apple": 7,
    "Booking": 6,
    "MediaMarkt": 5,
    "Ikea": 5,
    "Zalando": 4,
    "Steam": 4,
    "Netflix": 3,
    "Spotify": 3,
}

# --------------------------------------------------
# Transaction amount ranges
# --------------------------------------------------

AMOUNT_RANGES = {
    "Netflix": (20, 80),
    "Spotify": (15, 50),
    "Pyszne": (20, 200),
    "Uber": (15, 150),
    "Bolt": (15, 150),
    "Steam": (20, 600),
    "Biedronka": (10, 500),
    "Lidl": (10, 500),
    "Allegro": (20, 3000),
    "Amazon": (20, 4000),
    "Zalando": (50, 2500),
    "Booking": (100, 5000),
    "MediaMarkt": (100, 12000),
    "Apple": (100, 8000),
    "Ikea": (100, 15000),
}

# --------------------------------------------------
# Fraud rates
# --------------------------------------------------

FRAUD_RATES = {
    "Booking": 0.08,
    "Steam": 0.07,
    "Amazon": 0.06,
    "Allegro": 0.06,
    "Apple": 0.05,
    "MediaMarkt": 0.05,
    "Uber": 0.03,
    "Bolt": 0.03,
    "Zalando": 0.03,
    "Ikea": 0.02,
    "Netflix": 0.01,
    "Spotify": 0.01,
    "Biedronka": 0.01,
    "Lidl": 0.01,
    "Pyszne": 0.01,
}

# --------------------------------------------------
# Payment methods
# --------------------------------------------------

PAYMENT_METHODS = [
    "CARD",
    "BLIK",
    "TRANSFER",
    "APPLE_PAY",
    "GOOGLE_PAY"
]

# --------------------------------------------------
# Generator
# --------------------------------------------------

def generate_transaction():

    merchant = random.choices(
        population=list(MERCHANT_WEIGHTS.keys()),
        weights=list(MERCHANT_WEIGHTS.values()),
        k=1
    )[0]

    min_amount, max_amount = AMOUNT_RANGES[merchant]

    amount = round(
        random.uniform(min_amount, max_amount),
        2
    )

    fraud_rate = FRAUD_RATES[merchant]

    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": random.randint(1000, 9999),
        "merchant": merchant,
        "amount": amount,
        "currency": "PLN",
        "country": "PL",
        "payment_method": random.choice(PAYMENT_METHODS),
        "fraud_flag": random.random() < fraud_rate,
        "event_timestamp": (
            datetime.now(UTC)
            - timedelta(
                days=random.randint(0, 90),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )
        ).isoformat()
    }

# --------------------------------------------------
# Streaming loop
# --------------------------------------------------

while True:

    transaction = generate_transaction()

    producer.send(
        TOPIC,
        value=transaction
    )

    producer.flush()

    print(transaction)

    time.sleep(0.0001)