import pytest
from datetime import datetime
from fraud_detection.processors.enrichment_processor import EnrichmentProcessor
from fraud_detection.models.data_models import Transaction

def test_enrichment_processor_init():
    processor = EnrichmentProcessor()
    assert processor is not None

def test_get_risk_level():
    processor = EnrichmentProcessor()
    assert processor._get_risk_level(90) == "HIGH"
    assert processor._get_risk_level(60) == "MEDIUM"
    assert processor._get_risk_level(30) == "LOW"
    assert processor._get_risk_level(10) == "MINIMAL"

def test_enrich_with_profile():
    processor = EnrichmentProcessor()
    transaction = Transaction(
        user_id="test_user",
        amount=500.0,
        merchant="Test",
        location="NY",
        payment_method="CREDIT_CARD"
    )
    profile = {"risk_score": 90, "account_created": datetime.now().timestamp() * 1000}
    enriched = processor.enrich(transaction, profile)
    assert enriched['user_risk_level'] == "HIGH"
    assert enriched['transaction'] == transaction