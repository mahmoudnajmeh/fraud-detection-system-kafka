"""Kafka producers module."""

from fraud_detection.producers.transaction_producer import TransactionProducer
from fraud_detection.producers.user_profile_producer import UserProfileProducer

__all__ = ["TransactionProducer", "UserProfileProducer"]