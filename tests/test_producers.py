import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from fraud_detection.models.data_models import Transaction

class TestTransactionProducer:
    
    @patch('fraud_detection.producers.transaction_producer.AdminClient')
    @patch('fraud_detection.producers.transaction_producer.Producer')
    @patch('fraud_detection.producers.transaction_producer.AvroSerializer')
    def test_generate_transaction(self, mock_avro, mock_producer, mock_admin):
        from fraud_detection.producers.transaction_producer import TransactionProducer
        mock_admin.return_value.create_topics.return_value = {}
        producer = TransactionProducer()
        transaction = producer.generate_transaction()
        
        assert transaction.user_id is not None
        assert transaction.amount > 0
        assert transaction.merchant is not None
        assert transaction.payment_method in ["CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "BANK_TRANSFER"]
    
    def test_transaction_validation(self):
        with pytest.raises(ValueError):
            Transaction(
                amount=-100,
                user_id="test",
                merchant="test",
                location="test",
                payment_method="CREDIT_CARD"
            )
    
    def test_transaction_serialization(self):
        transaction = Transaction(
            user_id="test_user",
            amount=100.50,
            merchant="Test Merchant",
            location="Test Location",
            payment_method="CREDIT_CARD"
        )
        
        avro_dict = transaction.to_avro_dict()
        assert avro_dict['user_id'] == "test_user"
        assert avro_dict['amount'] == 100.50
        assert 'timestamp' in avro_dict