from kafka import KafkaProducer
import json
import time

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

# Sample customer orders
orders = [
    {"order_id": 1, "product": "Laptop", "quantity": 1},
    {"order_id": 2, "product": "Phone", "quantity": 3},
    {"order_id": 3, "product": "Tablet", "quantity": 2}
    ]
  
# Send each order to Kafka topic
for order in orders:
    producer.send('orders', order)
    print(f"Sent: {order}")
    time.sleep(1)
    
producer.flush()
