import pytest
from unittest.mock import patch, MagicMock

@patch('fraud_detection.producers.user_profile_producer.AdminClient')
@patch('fraud_detection.producers.user_profile_producer.Producer')
@patch('fraud_detection.producers.user_profile_producer.AvroSerializer')
def test_generate_user_profile(mock_avro, mock_producer, mock_admin):
    from fraud_detection.producers.user_profile_producer import UserProfileProducer
    mock_admin.return_value.create_topics.return_value = {}
    producer = UserProfileProducer()
    profile = producer.generate_user_profile()
    assert profile.user_id.startswith('user_')
    assert 18 <= profile.age <= 80
    assert profile.country in producer.countries