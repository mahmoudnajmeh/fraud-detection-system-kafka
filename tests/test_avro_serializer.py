import pytest
from pathlib import Path
from fraud_detection.utils.avro_serializer import AvroSerializer

def test_avro_serializer_init():
    schema_path = Path(__file__).parent.parent / "src/fraud_detection/schemas/transaction.avsc"
    serializer = AvroSerializer(str(schema_path))
    assert serializer.schema_name == "Transaction"

def test_avro_serialize_deserialize():
    schema_path = Path(__file__).parent.parent / "src/fraud_detection/schemas/transaction.avsc"
    serializer = AvroSerializer(str(schema_path))
    
    data = {"transaction_id": "test123", "user_id": "user1", "amount": 100.0, 
            "currency": "USD", "timestamp": 1775630000000, "merchant": "Test", 
            "location": "NY", "payment_method": "CREDIT_CARD"}
    
    serialized = serializer.serialize_one(data)
    deserialized = serializer.deserialize_one(serialized)
    
    assert deserialized["transaction_id"] == "test123"