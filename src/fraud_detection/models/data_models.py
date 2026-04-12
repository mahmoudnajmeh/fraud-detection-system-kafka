"""Pydantic data models for validation."""

from pydantic import BaseModel, Field, field_validator, ConfigDict
from datetime import datetime, UTC
from typing import Optional, List
import uuid

class Transaction(BaseModel):
    """Transaction data model."""
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "user_id": "user_12345",
                "amount": 99.99,
                "merchant": "Amazon",
                "location": "New York",
                "payment_method": "CREDIT_CARD"
            }
        }
    )
    
    transaction_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    amount: float
    currency: str = "USD"
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    merchant: str
    location: str
    payment_method: str
    ip_address: Optional[str] = None
    device_id: Optional[str] = None
    
    @field_validator('amount')
    @classmethod
    def amount_must_be_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError('Amount must be positive')
        return v
    
    @field_validator('payment_method')
    @classmethod
    def validate_payment_method(cls, v: str) -> str:
        allowed = ["CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "BANK_TRANSFER"]
        if v not in allowed:
            raise ValueError(f'Payment method must be one of {allowed}')
        return v
    
    def to_avro_dict(self) -> dict:
        return {
            "transaction_id": self.transaction_id,
            "user_id": self.user_id,
            "amount": self.amount,
            "currency": self.currency,
            "timestamp": int(self.timestamp.timestamp() * 1000),
            "merchant": self.merchant,
            "location": self.location,
            "payment_method": self.payment_method,
            "ip_address": self.ip_address,
            "device_id": self.device_id
        }

class UserProfile(BaseModel):
    """User profile data model."""
    
    model_config = ConfigDict()
    
    user_id: str
    email: str
    full_name: str
    age: int
    country: str
    risk_score: int = 0
    account_created: datetime
    verified: bool = False
    avg_transaction_amount: Optional[float] = None
    preferred_payment_methods: List[str] = Field(default_factory=list)
    
    @field_validator('age')
    @classmethod
    def age_must_be_valid(cls, v: int) -> int:
        if not 0 <= v <= 120:
            raise ValueError('Age must be between 0 and 120')
        return v
    
    @field_validator('risk_score')
    @classmethod
    def risk_score_must_be_valid(cls, v: int) -> int:
        if not 0 <= v <= 100:
            raise ValueError('Risk score must be between 0 and 100')
        return v
    
    def to_avro_dict(self) -> dict:
        return {
            "user_id": self.user_id,
            "email": self.email,
            "full_name": self.full_name,
            "age": self.age,
            "country": self.country,
            "risk_score": self.risk_score,
            "account_created": int(self.account_created.timestamp() * 1000),
            "verified": self.verified,
            "avg_transaction_amount": self.avg_transaction_amount,
            "preferred_payment_methods": self.preferred_payment_methods
        }

class FraudAlert(BaseModel):
    """Fraud alert data model."""
    
    model_config = ConfigDict()
    
    alert_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    transaction_id: str
    user_id: str
    alert_type: str
    severity: str
    description: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    transaction_details: Optional[str] = None
    
    def to_avro_dict(self) -> dict:
        return {
            "alert_id": self.alert_id,
            "transaction_id": self.transaction_id,
            "user_id": self.user_id,
            "alert_type": self.alert_type,
            "severity": self.severity,
            "description": self.description,
            "timestamp": int(self.timestamp.timestamp() * 1000),
            "transaction_details": self.transaction_details
        }