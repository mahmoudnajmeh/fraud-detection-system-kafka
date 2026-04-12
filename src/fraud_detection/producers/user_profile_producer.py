"""User profile producer for Kafka."""

import random
from datetime import datetime, timedelta
from pathlib import Path
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker
from loguru import logger
import sys
import os
from datetime import datetime, timedelta, UTC

sys.path.append(str(Path(__file__).parent.parent.parent.parent))

from fraud_detection.config.settings import settings
from fraud_detection.config.logger_config import logger
from fraud_detection.models.data_models import UserProfile
from fraud_detection.utils.avro_serializer import AvroSerializer

class UserProfileProducer:
    """Producer for user profile events."""
    
    def __init__(self):
        """Initialize the user profile producer."""
        self.producer_config = {
            'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
            'client.id': 'user-profile-producer',
            'acks': 'all',
            'retries': 5,
            'enable.idempotence': True,
        }
        
        self.producer = Producer(self.producer_config)
        self.faker = Faker()
        self.countries = ["US", "UK", "CA", "AU", "DE", "FR", "ES", "IT", "JP", "BR"]
        
        schema_path = Path(__file__).parent.parent / "schemas" / "user_profile.avsc"
        self.avro_serializer = AvroSerializer(str(schema_path))
        
        self._ensure_topic()
        
        logger.info("User profile producer initialized")
    
    def _ensure_topic(self):
        """Ensure the user profiles topic exists with compaction."""
        admin_client = AdminClient({
            'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS
        })
        
        topic = NewTopic(
            settings.USER_PROFILE_TOPIC,
            num_partitions=3,
            replication_factor=1,
            config={
                'cleanup.policy': 'compact',
                'delete.retention.ms': '86400000'
            }
        )
        
        fs = admin_client.create_topics([topic])
        for topic_name, future in fs.items():
            try:
                future.result()
                logger.info(f"Topic {topic_name} created with compaction")
            except Exception as e:
                if "already exists" not in str(e):
                    logger.debug(f"Topic {topic_name} already exists")
    
    def generate_user_profile(self) -> UserProfile:
        """Generate a realistic user profile."""
        account_age = random.randint(1, 365 * 3)
        verified = random.random() < 0.8
        
        return UserProfile(
            user_id=f"user_{self.faker.random_number(digits=5)}",
            email=self.faker.email(),
            full_name=self.faker.name(),
            age=random.randint(18, 80),
            country=random.choice(self.countries),
            risk_score=random.randint(0, 100),
            account_created=datetime.now(UTC) - timedelta(days=account_age),
            verified=verified,
            avg_transaction_amount=round(random.uniform(50, 500), 2) if verified else None,
            preferred_payment_methods=random.sample(
                ["CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "BANK_TRANSFER"],
                k=random.randint(1, 3)
            )
        )
    
    def delivery_report(self, err, msg):
        """Callback for message delivery."""
        if err is not None:
            logger.error(f"Profile delivery failed: {err}")
        else:
            logger.debug(f"Profile delivered to {msg.topic()} [{msg.partition()}]")
    
    def send_profile(self, profile: UserProfile):
        """Send user profile to Kafka."""
        try:
            avro_bytes = self.avro_serializer.serialize_one(profile.to_avro_dict())
            
            self.producer.produce(
                topic=settings.USER_PROFILE_TOPIC,
                key=profile.user_id,
                value=avro_bytes,
                callback=self.delivery_report
            )
            
            self.producer.poll(0)
            logger.info(f"Produced profile for user {profile.user_id}")
            
        except Exception as e:
            logger.error(f"Failed to send profile: {e}")
            raise
    
    def flush(self):
        """Wait for all messages to be delivered."""
        self.producer.flush()
    
    def run_simulation(self, num_profiles: int = 50):
        """Generate and send multiple user profiles."""
        logger.info(f"Generating {num_profiles} user profiles")
        
        for i in range(num_profiles):
            profile = self.generate_user_profile()
            self.send_profile(profile)
            
            if i % 10 == 0:
                self.producer.poll(0)
        
        self.flush()
        logger.success(f"Generated {num_profiles} user profiles")