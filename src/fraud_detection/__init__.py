"""Fraud Detection System - Real-time fraud detection with Apache Kafka."""

__version__ = "1.0.0"
__author__ = "Your Name"
__email__ = "your.email@example.com"

from fraud_detection.config.settings import settings
from fraud_detection.config.logger_config import logger

__all__ = ["settings", "logger"]