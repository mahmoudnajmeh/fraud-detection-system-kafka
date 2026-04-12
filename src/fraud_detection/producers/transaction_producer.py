"""Transaction producer for Kafka - Dual format: Avro (storage) + JSON (readable)."""

import time
import json
import random
from typing import Optional
from pathlib import Path
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker
from loguru import logger
import sys
import os

sys.path.append(str(Path(__file__).parent.parent.parent.parent))

from fraud_detection.config.settings import settings
from fraud_detection.config.logger_config import logger
from fraud_detection.models.data_models import Transaction
from fraud_detection.utils.avro_serializer import AvroSerializer

class TransactionProducer:
    """Producer for transaction events - Avro for production, JSON for UI."""
    
    def __init__(self):
        """Initialize the transaction producer."""
        self.producer_config = {
            'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
            'client.id': 'transaction-producer',
            'acks': 'all',
            'retries': 10,
            'retry.backoff.ms': 1000,
            'enable.idempotence': True,
            'compression.type': settings.COMPRESSION_TYPE,
            'batch.size': settings.BATCH_SIZE,
            'linger.ms': settings.LINGER_MS,
            'delivery.timeout.ms': 120000,
        }
        
        self.producer = Producer(self.producer_config)
        self.faker = Faker()
        self.payment_methods = ["CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "BANK_TRANSFER"]
        self.merchants = [
            "Amazon", "Walmart", "Target", "Best Buy", "Apple Store",
            "Nike", "Adidas", "Uber", "Lyft", "Starbucks", "McDonald's",
            "Wendy's", "Home Depot", "Lowe's", "Costco"
        ]
        self.locations = [
            "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
            "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"
        ]
        
        schema_path = Path(__file__).parent.parent / "schemas" / "transaction.avsc"
        self.avro_serializer = AvroSerializer(str(schema_path))
        
        self._ensure_topic()
        
        logger.info("Transaction producer initialized (Avro + JSON dual format)")
    
    def _ensure_topic(self):
        """Ensure the transactions topic exists."""
        admin_client = AdminClient({
            'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS
        })
        
        topic = NewTopic(
            settings.TRANSACTION_TOPIC,
            num_partitions=5,
            replication_factor=1
        )
        
        # Also ensure readable-json topic exists
        json_topic = NewTopic(
            "transactions-readable",
            num_partitions=5,
            replication_factor=1
        )
        
        fs = admin_client.create_topics([topic, json_topic])
        for topic_name, future in fs.items():
            try:
                future.result()
                logger.info(f"Topic {topic_name} created")
            except Exception as e:
                if "already exists" not in str(e):
                    logger.debug(f"Topic {topic_name} already exists")
    
    def delivery_report(self, err, msg):
        """Callback for message delivery reports."""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
            self._store_failed_message(msg)
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")
    
    def _store_failed_message(self, msg):
        """Store failed messages for later recovery."""
        try:
            filename = settings.FAILED_MSGS_DIR / f"failed_{int(time.time())}.json"
            with open(filename, 'w') as f:
                json.dump({
                    'topic': msg.topic(),
                    'key': msg.key().decode() if msg.key() else None,
                    'value': msg.value().decode() if msg.value() else None,
                    'timestamp': time.time()
                }, f)
            logger.warning(f"Stored failed message at {filename}")
        except Exception as e:
            logger.error(f"Failed to store message: {e}")
    
    def generate_transaction(self, user_id: Optional[str] = None) -> Transaction:
        """Generate a realistic fake transaction."""
        if not user_id:
            user_id = f"user_{self.faker.random_number(digits=5)}"
        
        transaction = Transaction(
            user_id=user_id,
            amount=round(random.uniform(10, 5000), 2),
            merchant=random.choice(self.merchants),
            location=random.choice(self.locations),
            payment_method=random.choice(self.payment_methods),
            ip_address=self.faker.ipv4(),
            device_id=f"device_{self.faker.uuid4()[:8]}"
        )
        
        if random.random() < 0.1:
            if random.random() < 0.5:
                transaction.amount = round(random.uniform(10000, 50000), 2)
        
        return transaction
    
    def send_transaction(self, transaction: Transaction):
        """Send a single transaction to Kafka - Avro (production) + JSON (readable)."""
        try:
            # 1. Send Avro (production format - smaller, faster)
            avro_bytes = self.avro_serializer.serialize_one(transaction.to_avro_dict())
            
            self.producer.produce(
                topic=settings.TRANSACTION_TOPIC,
                key=transaction.user_id,
                value=avro_bytes,
                callback=self.delivery_report,
                headers={
                    'content-type': 'avro/binary',
                    'version': '1.0',
                }
            )
            
            # 2. ALSO send JSON (readable for Kafka UI)
            json_bytes = json.dumps(transaction.to_avro_dict(), default=str).encode('utf-8')
            
            self.producer.produce(
                topic="transactions-readable",
                key=transaction.user_id,
                value=json_bytes,
                callback=self.delivery_report,
                headers={
                    'content-type': 'application/json',
                    'version': '1.0',
                }
            )
            
            self.producer.poll(0)
            logger.info(f"Produced transaction {transaction.transaction_id} for user {transaction.user_id}")
            
        except Exception as e:
            logger.error(f"Failed to send transaction: {e}")
            raise
    
    def flush(self):
        """Wait for all messages to be delivered."""
        self.producer.flush()
        logger.info("All messages flushed")
    
    def run_simulation(self, num_transactions: int = 100, interval: float = 0.5):
        """Run a simulation generating multiple transactions."""
        logger.info(f"Starting simulation: {num_transactions} transactions with {interval}s interval")
        
        try:
            for i in range(num_transactions):
                if i % 10 == 0:
                    user_id = "user_12345"
                else:
                    user_id = f"user_{self.faker.random_number(digits=5)}"
                
                transaction = self.generate_transaction(user_id)
                self.send_transaction(transaction)
                time.sleep(interval)
                
                if i % 10 == 0:
                    self.producer.poll(0)
            
            self.flush()
            logger.success(f"Simulation complete: {num_transactions} transactions sent")
            
        except KeyboardInterrupt:
            logger.warning("Simulation interrupted")
            self.flush()
        except Exception as e:
            logger.error(f"Simulation failed: {e}")
            self.flush()
            raise