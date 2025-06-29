from kafka import KafkaConsumer
import json

# Create Kafka consumer
consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earlies',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)


# Consumer messages from Kafka topic
print("Listening for messages on 'orders' topic...")
for message in consumer:
    print(f"Received: {message.value}")
