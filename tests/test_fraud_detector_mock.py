import pytest
from unittest.mock import patch, MagicMock

@pytest.fixture(autouse=True)
def cleanup():
    yield

@patch('fraud_detection.consumers.fraud_detector.Consumer')
@patch('fraud_detection.consumers.fraud_detector.AvroSerializer')
def test_fraud_detector_init(mock_avro, mock_consumer):
    from fraud_detection.consumers.fraud_detector import FraudDetector
    detector = FraudDetector()
    assert detector.metrics['transactions_processed'] == 0

@patch('fraud_detection.consumers.fraud_detector.Consumer')
@patch('fraud_detection.consumers.fraud_detector.AvroSerializer')
def test_process_user_profile(mock_avro, mock_consumer):
    from fraud_detection.consumers.fraud_detector import FraudDetector
    detector = FraudDetector()
    mock_avro.return_value.deserialize.return_value = [{'user_id': 'test123'}]
    detector.process_user_profile(b'test')
    assert 'test123' in detector.user_profiles