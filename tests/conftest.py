"""Pytest configuration and fixtures."""

import pytest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

@pytest.fixture
def sample_transaction():
    from fraud_detection.models.data_models import Transaction
    
    return Transaction(
        user_id="test_user_123",
        amount=100.50,
        merchant="Test Merchant",
        location="New York",
        payment_method="CREDIT_CARD"
    )

@pytest.fixture
def sample_user_profile():
    from fraud_detection.models.data_models import UserProfile
    from datetime import datetime
    
    return UserProfile(
        user_id="test_user_123",
        email="test@example.com",
        full_name="Test User",
        age=30,
        country="US",
        risk_score=50,
        account_created=datetime.utcnow(),
        verified=True
    )