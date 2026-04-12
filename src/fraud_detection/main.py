"""Main entry point for fraud detection system."""

import sys
import time
import signal
from pathlib import Path
from multiprocessing import Process
import click

sys.path.insert(0, str(Path(__file__).parent.parent))

from fraud_detection.config.logger_config import logger
from fraud_detection.producers.transaction_producer import TransactionProducer
from fraud_detection.producers.user_profile_producer import UserProfileProducer
from fraud_detection.consumers.fraud_detector import FraudDetector
from fraud_detection.consumers.alert_consumer import AlertConsumer


@click.group()
def cli():
    """Fraud Detection System CLI."""
    pass


@cli.command()
def setup():
    """Setup Kafka topics and environment."""
    logger.info("Setting up environment...")
    
    from confluent_kafka.admin import AdminClient
    from fraud_detection.config.settings import settings
    
    admin = AdminClient({'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS})
    
    try:
        admin.list_topics(timeout=10)
        logger.success("Kafka is reachable")
    except Exception as e:
        logger.error(f"Cannot connect to Kafka: {e}")
        logger.info("Make sure Docker is running: docker-compose up -d")
        sys.exit(1)
    
    import subprocess
    script_path = Path(__file__).parent.parent.parent / "scripts" / "setup_topics.sh"
    if script_path.exists():
        script_path.chmod(0o755)
        subprocess.run([str(script_path)], shell=True)
    
    logger.success("Environment setup complete")


@cli.command()
@click.option('--mode', type=click.Choice(['all', 'producers', 'consumer', 'monitor']), default='all')
def run(mode):
    """Run the fraud detection system."""
    
    def run_producer():
        logger.info("STARTING: Transaction Producer")
        producer = TransactionProducer()
        producer.run_simulation(num_transactions=500, interval=0.3)
    
    def run_profile_producer():
        logger.info("STARTING: User Profile Producer")
        producer = UserProfileProducer()
        producer.run_simulation(num_profiles=50)
    
    def run_fraud_detector():
        logger.info("STARTING: Fraud Detector")
        detector = FraudDetector()
        detector.run()
    
    def run_alert_consumer():
        logger.info("STARTING: Alert Consumer")
        consumer = AlertConsumer()
        consumer.run()
    
    if mode == "all":
        logger.info("Starting all components...")
        
        processes = []
        
        # Start profile producer first
        p1 = Process(target=run_profile_producer)
        p1.start()
        processes.append(p1)
        time.sleep(2)
        
        # Start fraud detector
        p2 = Process(target=run_fraud_detector)
        p2.start()
        processes.append(p2)
        time.sleep(2)
        
        # Start alert consumer
        p3 = Process(target=run_alert_consumer)
        p3.start()
        processes.append(p3)
        time.sleep(2)
        
        # Start transaction producer LAST
        p4 = Process(target=run_producer)
        p4.start()
        processes.append(p4)
        
        def signal_handler(sig, frame):
            logger.info("Shutting down...")
            for p in processes:
                if p.is_alive():
                    p.terminate()
                    p.join(timeout=5)
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        for p in processes:
            p.join()
    
    elif mode == "producers":
        logger.info("Starting producers...")
        profile_process = Process(target=run_profile_producer)
        tx_process = Process(target=run_producer)
        
        profile_process.start()
        time.sleep(2)
        tx_process.start()
        
        try:
            profile_process.join()
            tx_process.join()
        except KeyboardInterrupt:
            profile_process.terminate()
            tx_process.terminate()
    
    elif mode == "consumer":
        run_fraud_detector()
    
    elif mode == "monitor":
        run_alert_consumer()


if __name__ == "__main__":
    cli()