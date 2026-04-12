import pytest
from unittest.mock import patch, MagicMock
from fraud_detection.consumers.audit_consumer import AuditConsumer

@patch('fraud_detection.consumers.audit_consumer.Consumer')
def test_audit_consumer_init(mock_consumer):
    consumer = AuditConsumer()
    assert consumer.audit_dir.exists()