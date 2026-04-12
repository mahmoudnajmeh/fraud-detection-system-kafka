from confluent_kafka import Consumer
import sys
import json
import re

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'read-alerts',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
}

consumer = Consumer(conf)
consumer.subscribe(['fraud-alerts'])

print("=" * 80)
print("FRAUD ALERTS - READABLE OUTPUT")
print("=" * 80)

try:
    msg_count = 0
    while msg_count < 10:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            continue
        
        raw_str = str(msg.value())
        
        if 'Transaction amount' in raw_str:
            amount_match = re.search(r'Transaction amount \$([0-9,]+\.?[0-9]*)', raw_str)
            merchant_match = re.search(r'Merchant: ([^,]+)', raw_str)
            location_match = re.search(r'Location: ([^\']+)', raw_str)
            
            print(f"\n🚨 FRAUD ALERT:")
            if amount_match:
                print(f"   Amount: ${amount_match.group(1)}")
            if merchant_match:
                print(f"   Merchant: {merchant_match.group(1)}")
            if location_match:
                print(f"   Location: {location_match.group(1)}")
            print(f"   Key: {msg.key().decode() if msg.key() else 'None'}")
            print("-" * 40)
        
        msg_count += 1
        
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
