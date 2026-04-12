import pytest
from unittest.mock import patch, MagicMock

@pytest.fixture(autouse=True)
def cleanup():
    yield
    # Force cleanup after test

@patch('fraud_detection.consumers.alert_consumer.Consumer')
@patch('fraud_detection.consumers.alert_consumer.AvroSerializer')
def test_alert_consumer_init(mock_avro, mock_consumer):
    from fraud_detection.consumers.alert_consumer import AlertConsumer
    consumer = AlertConsumer()
    consumer.running = False  # Stop any loops
    assert consumer.alert_count == 0

@patch('fraud_detection.consumers.alert_consumer.Consumer')
@patch('fraud_detection.consumers.alert_consumer.AvroSerializer')
def test_process_alert(mock_avro, mock_consumer):
    from fraud_detection.consumers.alert_consumer import AlertConsumer
    consumer = AlertConsumer()
    consumer.running = False
    mock_avro.return_value.deserialize.return_value = [{
        'alert_type': 'HIGH_AMOUNT',
        'severity': 'CRITICAL',
        'description': 'Test alert'
    }]
    consumer.process_alert(b'test')
    assert consumer.alert_count == 1