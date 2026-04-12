import pytest
from datetime import datetime, timedelta
from fraud_detection.processors.fraud_rules_engine import FraudRulesEngine
from fraud_detection.models.data_models import Transaction

def test_risk_score_rule_high():
    engine = FraudRulesEngine()
    transaction = Transaction(
        user_id="test", amount=100, merchant="Test", location="NY", payment_method="CREDIT_CARD"
    )
    enriched = {'transaction': transaction, 'user_profile': {'risk_score': 95}}
    alerts = engine.check_risk_score(enriched)
    assert len(alerts) == 1
    assert alerts[0].alert_type == "RISKY_USER"

def test_risk_score_rule_low():
    engine = FraudRulesEngine()
    transaction = Transaction(
        user_id="test", amount=100, merchant="Test", location="NY", payment_method="CREDIT_CARD"
    )
    enriched = {'transaction': transaction, 'user_profile': {'risk_score': 30}}
    alerts = engine.check_risk_score(enriched)
    assert len(alerts) == 0

def test_unusual_location_rule():
    engine = FraudRulesEngine()
    transaction = Transaction(
        user_id="test", amount=100, merchant="Test", location="Paris, FR", payment_method="CREDIT_CARD"
    )
    enriched = {'transaction': transaction, 'user_profile': {'country': 'US'}}
    alerts = engine.check_unusual_location(enriched)
    assert len(alerts) == 1
    assert alerts[0].alert_type == "UNUSUAL_LOCATION"