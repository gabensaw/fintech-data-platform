import json
from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


message = {
    "transaction_id": "TXN_000001",
    "customer_id": 1001,
    "amount": 199.99
}


producer.send(
    "transactions",
    value=message
)

producer.flush()

print("Message sent successfully")