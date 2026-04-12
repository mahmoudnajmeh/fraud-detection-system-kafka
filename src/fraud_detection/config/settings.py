"""Application settings and configuration."""

import os
from pathlib import Path
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict

from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False
    )
    
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    SCHEMA_REGISTRY_URL: str = "http://localhost:8081"
    
    TRANSACTION_TOPIC: str = "transactions"
    USER_PROFILE_TOPIC: str = "user-profiles"
    ENRICHED_TRANSACTION_TOPIC: str = "enriched-transactions"
    FRAUD_ALERT_TOPIC: str = "fraud-alerts"
    AUDIT_LOG_TOPIC: str = "audit-log"
    
    FRAUD_DETECTOR_GROUP: str = "fraud-detector-group"
    ALERT_CONSUMER_GROUP: str = "alert-consumer-group"
    AUDIT_CONSUMER_GROUP: str = "audit-consumer-group"
    
    FRAUD_THRESHOLD_AMOUNT: float = 10000.0
    MAX_TRANSACTIONS_PER_MINUTE: int = 5
    
    LOG_LEVEL: str = "INFO"
    ENABLE_AUDIT_LOGGING: bool = True
    
    BATCH_SIZE: int = 16384
    LINGER_MS: int = 5
    COMPRESSION_TYPE: str = "snappy"
    
    BASE_DIR: Path = Path(__file__).parent.parent.parent.parent
    LOGS_DIR: Path = BASE_DIR / "logs"
    FAILED_MSGS_DIR: Path = BASE_DIR / "failed_messages"

settings = Settings()

os.makedirs(settings.LOGS_DIR, exist_ok=True)
os.makedirs(settings.FAILED_MSGS_DIR, exist_ok=True)