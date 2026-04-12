"""Audit consumer for compliance logging."""

from confluent_kafka import Consumer, KafkaError
from pathlib import Path
import json
from datetime import datetime
import sys
from loguru import logger

sys.path.append(str(Path(__file__).parent.parent.parent.parent))

from fraud_detection.config.settings import settings

class AuditConsumer:
    """Consumer that logs all events for audit compliance."""
    
    def __init__(self):
        """Initialize the audit consumer."""
        self.consumer_config = {
            'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
            'group.id': settings.AUDIT_CONSUMER_GROUP,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
        }
        
        self.consumer = Consumer(self.consumer_config)
        self.consumer.subscribe([
            settings.TRANSACTION_TOPIC,
            settings.FRAUD_ALERT_TOPIC,
            settings.AUDIT_LOG_TOPIC
        ])
        
        self.audit_dir = settings.LOGS_DIR / "audit"
        self.audit_dir.mkdir(exist_ok=True)
        
        logger.info("Audit consumer initialized")
    
    def run(self):
        """Main consumer loop."""
        logger.info("Starting audit consumer...")
        
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error(f"Consumer error: {msg.error()}")
                    continue
                
                audit_entry = {
                    'timestamp': datetime.utcnow().isoformat(),
                    'topic': msg.topic(),
                    'partition': msg.partition(),
                    'offset': msg.offset(),
                    'key': msg.key().decode() if msg.key() else None,
                }
                
                audit_file = self.audit_dir / f"{datetime.utcnow().date()}.jsonl"
                with open(audit_file, 'a') as f:
                    f.write(json.dumps(audit_entry) + '\n')
                
                logger.bind(audit=True).info(f"Audit: {msg.topic()}[{msg.partition()}] @ {msg.offset()}")
                
        except KeyboardInterrupt:
            logger.info("Shutting down audit consumer...")
        finally:
            self.consumer.close()