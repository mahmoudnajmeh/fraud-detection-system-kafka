"""Tests for fraud detection rules."""

import pytest
from datetime import datetime, timedelta
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from fraud_detection.processors.fraud_rules_engine import FraudRulesEngine
from fraud_detection.models.data_models import Transaction

class TestFraudRulesEngine:
    
    def test_high_amount_rule(self):
        engine = FraudRulesEngine()
        transaction = Transaction(
            user_id="test_user",
            amount=15000,
            merchant="Test",
            location="NY",
            payment_method="CREDIT_CARD"
        )
        
        enriched = {'transaction': transaction, 'user_history': []}
        alerts = engine.check_high_amount(enriched)
        
        assert len(alerts) > 0
        assert alerts[0].alert_type == "HIGH_AMOUNT"
    
    def test_normal_amount_passes(self):
        engine = FraudRulesEngine()
        transaction = Transaction(
            user_id="test_user",
            amount=100,
            merchant="Test",
            location="NY",
            payment_method="CREDIT_CARD"
        )
        
        enriched = {'transaction': transaction, 'user_history': []}
        alerts = engine.check_high_amount(enriched)
        
        assert len(alerts) == 0