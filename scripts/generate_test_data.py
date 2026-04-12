import sys
import time
import random
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from fraud_detection.producers.transaction_producer import TransactionProducer
from fraud_detection.producers.user_profile_producer import UserProfileProducer
from fraud_detection.config.logger_config import logger

def generate_user_base():
    logger.info("Generating user profiles...")
    producer = UserProfileProducer()
    producer.run_simulation(100)
    logger.success("User profiles generated")

def generate_transaction_stream():
    logger.info("Starting transaction stream...")
    producer = TransactionProducer()
    
    try:
        while True:
            num_transactions = random.randint(5, 20)
            for i in range(num_transactions):
                if random.random() < 0.3:
                    user_id = f"user_{random.randint(10000, 10100)}"
                else:
                    user_id = None
                
                transaction = producer.generate_transaction(user_id)
                producer.send_transaction(transaction)
                time.sleep(random.uniform(0.1, 0.5))
            
            time.sleep(random.uniform(2, 5))
            
    except KeyboardInterrupt:
        logger.info("Stopping transaction stream")
        producer.flush()

if __name__ == "__main__":
    generate_user_base()
    generate_transaction_stream()