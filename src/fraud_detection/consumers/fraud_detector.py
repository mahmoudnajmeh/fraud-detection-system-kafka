"""Fraud detector consumer."""

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
import sys
import os
import json
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta
from threading import Lock
from pathlib import Path
from loguru import logger

sys.path.append(str(Path(__file__).parent.parent.parent.parent))

from fraud_detection.config.settings import settings
from fraud_detection.config.logger_config import logger
from fraud_detection.models.data_models import FraudAlert, Transaction
from fraud_detection.utils.avro_serializer import AvroSerializer
from fraud_detection.processors.enrichment_processor import EnrichmentProcessor
from fraud_detection.processors.fraud_rules_engine import FraudRulesEngine

class FraudDetector:
    """Fraud detector consumer that processes transactions."""
    
    def __init__(self):
        """Initialize the fraud detector."""
        self.consumer_config = {
            'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
            'group.id': settings.FRAUD_DETECTOR_GROUP,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'max.poll.interval.ms': 300000,
            'session.timeout.ms': 45000,
            'heartbeat.interval.ms': 15000,
            'fetch.min.bytes': 1024,
        }
        
        self.consumer = Consumer(self.consumer_config)
        self.consumer.subscribe([settings.TRANSACTION_TOPIC, settings.USER_PROFILE_TOPIC])
        
        schema_path = Path(__file__).parent.parent / "schemas" / "transaction.avsc"
        self.transaction_serializer = AvroSerializer(str(schema_path))
        
        profile_schema_path = Path(__file__).parent.parent / "schemas" / "user_profile.avsc"
        self.profile_serializer = AvroSerializer(str(profile_schema_path))
        
        alert_schema_path = Path(__file__).parent.parent / "schemas" / "fraud_alert.avsc"
        self.alert_serializer = AvroSerializer(str(alert_schema_path))
        
        self.enrichment_processor = EnrichmentProcessor()
        self.rules_engine = FraudRulesEngine()
        
        self.user_transactions = defaultdict(lambda: deque(maxlen=100))
        self.user_profiles = {}
        self.state_lock = Lock()
        
        self.metrics = {
            'transactions_processed': 0,
            'alerts_generated': 0,
            'errors': 0,
            'last_commit_time': time.time()
        }
        
        logger.info("Fraud Detector initialized")
    
    def process_transaction(self, transaction_data: bytes):
        """Process a single transaction."""
        try:
            records = self.transaction_serializer.deserialize(transaction_data)
            if not records:
                return None
            
            transaction_dict = records[0]
            transaction = Transaction(**transaction_dict)
            
            user_id = transaction.user_id
            user_profile = self.user_profiles.get(user_id)
            
            if not user_profile:
                logger.warning(f"No profile found for user {user_id}, using default")
                user_profile = None
            
            with self.state_lock:
                self.user_transactions[user_id].append({
                    'timestamp': transaction.timestamp,
                    'amount': transaction.amount,
                    'location': transaction.location
                })
            
            enriched = self.enrichment_processor.enrich(transaction, user_profile)
            
            alerts = self.rules_engine.check_all_rules(enriched)
            
            self.metrics['transactions_processed'] += 1
            
            if alerts:
                for alert in alerts:
                    logger.bind(fraud_audit=True).info(
                        f"AUDIT | action=FRAUD_ALERT | transaction_id={transaction.transaction_id} | "
                        f"user_id={transaction.user_id} | alert_type={alert.alert_type} | "
                        f"severity={alert.severity} | amount={transaction.amount}"
                    )
            
            logger.bind(verify=True).info(
                f"VERIFY | transactions_processed={self.metrics['transactions_processed']} | "
                f"alerts_generated={self.metrics['alerts_generated']} | errors={self.metrics['errors']}"
            )
            
            return alerts
            
        except Exception as e:
            logger.error(f"Error processing transaction: {e}")
            self.metrics['errors'] += 1
            return []
    
    def process_user_profile(self, profile_data: bytes):
        """Process a user profile update."""
        try:
            records = self.profile_serializer.deserialize(profile_data)
            if not records:
                return
            
            profile_dict = records[0]
            user_id = profile_dict['user_id']
            
            with self.state_lock:
                self.user_profiles[user_id] = profile_dict
            
            logger.debug(f"Updated profile for user {user_id}")
            
        except Exception as e:
            logger.error(f"Error processing profile: {e}")
            self.metrics['errors'] += 1
    
    def send_alerts(self, alerts, producer):
        """Send fraud alerts to Kafka - Avro (production) + JSON (readable)."""
        for alert in alerts:
            try:
                alert_dict = alert.to_avro_dict()
                
                avro_bytes = self.alert_serializer.serialize_one(alert_dict)
                
                producer.produce(
                    topic=settings.FRAUD_ALERT_TOPIC,
                    key=alert.user_id,
                    value=avro_bytes,
                    callback=self.delivery_report
                )
                
                json_dict = alert_dict.copy()
                json_dict['timestamp'] = alert.timestamp.isoformat()
                json_bytes = json.dumps(json_dict, default=str).encode('utf-8')
                
                producer.produce(
                    topic="fraud-alerts-readable",
                    key=alert.user_id,
                    value=json_bytes,
                    headers={'content-type': 'application/json'}
                )
                
                self.metrics['alerts_generated'] += 1
                logger.warning(f"FRAUD ALERT: {alert.alert_type} - {alert.description}")
                
            except Exception as e:
                logger.error(f"Failed to send alert: {e}")
    
    def gdpr_delete_user(self, user_id: str):
        """GDPR erasure - delete all data for a user."""
        with self.state_lock:
            before_transactions = len(self.user_transactions.get(user_id, []))
            before_profile = 1 if user_id in self.user_profiles else 0
            
            logger.bind(deletion=True).info(
                f"DELETION | action=GDPR_ERASURE_REQUEST | user_id={user_id} | "
                f"before_transactions={before_transactions} | before_profile={before_profile} | "
                f"timestamp={datetime.utcnow().isoformat()}"
            )
            
            if user_id in self.user_transactions:
                del self.user_transactions[user_id]
            
            if user_id in self.user_profiles:
                del self.user_profiles[user_id]
            
            logger.bind(deletion=True).info(
                f"DELETION | action=GDPR_ERASURE_COMPLETE | user_id={user_id} | "
                f"requires_vacuum=True | note='Old data still in logs until retention period expires'"
            )
    
    def delivery_report(self, err, msg):
        """Callback for alert delivery."""
        if err is not None:
            logger.error(f"Alert delivery failed: {err}")
    
    def commit_offsets(self):
        """Commit offsets and log metrics."""
        try:
            self.consumer.commit(asynchronous=False)
            self.metrics['last_commit_time'] = time.time()
            
            logger.info(f"Metrics: {self.metrics}")
            
        except Exception as e:
            logger.error(f"Failed to commit offsets: {e}")
    
    def run(self):
        """Main consumer loop."""
        from confluent_kafka import Producer
        
        alert_producer_config = {
            'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
            'client.id': 'alert-producer',
            'acks': 'all',
        }
        alert_producer = Producer(alert_producer_config)
        
        from confluent_kafka.admin import AdminClient, NewTopic
        admin_client = AdminClient({'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS})
        topic = NewTopic("fraud-alerts-readable", num_partitions=3, replication_factor=1)
        try:
            admin_client.create_topics([topic])
        except:
            pass
        
        logger.info("Starting fraud detector...")
        
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        self.metrics['errors'] += 1
                        continue
                
                if msg.topic() == settings.TRANSACTION_TOPIC:
                    alerts = self.process_transaction(msg.value())
                    if alerts:
                        self.send_alerts(alerts, alert_producer)
                        alert_producer.poll(0)
                
                elif msg.topic() == settings.USER_PROFILE_TOPIC:
                    self.process_user_profile(msg.value())
                
                if (self.metrics['transactions_processed'] % 100 == 0 or
                    time.time() - self.metrics['last_commit_time'] > 10):
                    self.commit_offsets()
                    alert_producer.flush()
        
        except KeyboardInterrupt:
            logger.info("Shutting down fraud detector...")
        finally:
            self.commit_offsets()
            alert_producer.flush()
            self.consumer.close()
            logger.success("Fraud detector stopped")