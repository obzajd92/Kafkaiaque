import json
from confluent_kafka import Producer, Consumer, KafkaError

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "item_crud_events"

# Producer Configuration
producer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'django-producer',
    'acks': 'all',  # Await complete confirmation from broker broker string
    'retries': 5
}
producer = Producer(producer_config)

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"✅ Message acknowledged: {msg.topic()} [{msg.partition()}]")

def publish_crud_event(action, item_id, item_name):
    payload = {"action": action, "item_id": item_id, "name": item_name}
    producer.produce(
        topic=TOPIC_NAME,
        key=str(item_id),
        value=json.dumps(payload),
        callback=delivery_report
    )
    producer.flush()

# Consumer Configuration (At-Least-Once)
def run_simple_consumer():
    consumer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'django-at-least-once-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False  # CRITICAL: Disable automatic offset tracking
    }
    
    consumer = Consumer(consumer_config)
    consumer.subscribe([TOPIC_NAME])
    
    print(f"🚀 At-Least-Once Consumer listening on: {TOPIC_NAME}...")
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
            
            try:
                # 1. Process payload business logic
                data = json.loads(msg.value().decode('utf-8'))
                print(f" [🔊 Processing Event] {data['action'].upper()} | ID: {data['item_id']}")
                
                # 2. CRITICAL: Commit offset synchronously *only* after processing succeeds
                consumer.commit(message=msg, asynchronous=False)
                print(f"     ↳ 💾 Offset safely committed.")
                
            except Exception as e:
                print(f"❌ Error processing event, skipping offset commit: {e}")
                # Message will remain uncommitted and re-read on restart/rebalance
                
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
