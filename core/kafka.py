import json
from confluent_kafka import Producer, Consumer, KafkaError

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "item_crud_events"

# Initialize Producer
producer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'django-producer'
}
producer = Producer(producer_config)

def delivery_report(err, msg):
    """ Optional callback for checking delivery success. """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def publish_crud_event(action, item_id, item_name):
    """ Publishes a simple CRUD event to the Kafka topic. """
    payload = {
        "action": action,      # 'create', 'update', or 'delete'
        "item_id": item_id,
        "name": item_name
    }
    
    producer.produce(
        topic=TOPIC_NAME,
        key=str(item_id),
        value=json.dumps(payload),
        callback=delivery_report
    )
    # Flush ensures the message is sent immediately
    producer.flush()


def run_simple_consumer():
    """ A simple blocking loop to consume CRUD messages. """
    consumer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'django-crud-group',
        'auto.offset.reset': 'earliest'
    }
    
    consumer = Consumer(consumer_config)
    consumer.subscribe([TOPIC_NAME])
    
    print(f"Starting consumer listening on topic: {TOPIC_NAME}...")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    break
            
            # Process payload
            data = json.loads(msg.value().decode('utf-8'))
            print(f" [🔊 Kafka Event Received] Action: {data['action'].upper()} | ID: {data['item_id']} | Name: {data['name']}")
            
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
