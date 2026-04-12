"""Fraud detection rules engine."""

from datetime import datetime, timedelta
from typing import List, Dict, Any, Callable
from fraud_detection.models.data_models import FraudAlert, Transaction
from fraud_detection.config.settings import settings
from loguru import logger

class FraudRulesEngine:
    """Engine that applies fraud detection rules to transactions."""
    
    def __init__(self):
        """Initialize the rules engine with all detection rules."""
        self.rules: List[Callable] = [
            self.check_high_amount,
            self.check_rapid_successive,
            self.check_unusual_location,
            self.check_risk_score,
            self.check_velocity
        ]
        logger.info("Fraud rules engine initialized with {} rules", len(self.rules))
    
    def check_high_amount(self, enriched_transaction: Dict[str, Any]) -> List[FraudAlert]:
        """Rule: Check for unusually high transaction amounts."""
        alerts = []
        transaction = enriched_transaction['transaction']
        threshold = settings.FRAUD_THRESHOLD_AMOUNT
        
        if transaction.amount > threshold:
            severity = "CRITICAL" if transaction.amount > threshold * 2 else "HIGH"
            
            alert = FraudAlert(
                transaction_id=transaction.transaction_id,
                user_id=transaction.user_id,
                alert_type="HIGH_AMOUNT",
                severity=severity,
                description=(
                    f"Transaction amount ${transaction.amount:,.2f} exceeds "
                    f"threshold ${threshold:,.2f}"
                ),
                transaction_details=f"Merchant: {transaction.merchant}, Location: {transaction.location}"
            )
            alerts.append(alert)
            logger.warning(f"HIGH AMOUNT: ${transaction.amount:,.2f} > ${threshold:,.2f}")
        
        return alerts
    
    def check_rapid_successive(self, enriched_transaction: Dict[str, Any]) -> List[FraudAlert]:
        """Rule: Check for multiple transactions in short time window."""
        alerts = []
        transaction = enriched_transaction['transaction']
        user_history = enriched_transaction.get('user_history', [])
        
        one_minute_ago = datetime.utcnow() - timedelta(minutes=1)
        recent_count = sum(
            1 for t in user_history
            if t.get('timestamp', datetime.min) > one_minute_ago
        )
        
        if recent_count > settings.MAX_TRANSACTIONS_PER_MINUTE:
            alert = FraudAlert(
                transaction_id=transaction.transaction_id,
                user_id=transaction.user_id,
                alert_type="RAPID_SUCCESSIVE",
                severity="MEDIUM",
                description=(
                    f"{recent_count} transactions in last minute "
                    f"(limit: {settings.MAX_TRANSACTIONS_PER_MINUTE})"
                ),
                transaction_details=f"Recent count: {recent_count}"
            )
            alerts.append(alert)
            logger.warning(f"RAPID SUCCESSIVE: {recent_count} transactions in 1 minute")
        
        return alerts
    
    def check_unusual_location(self, enriched_transaction: Dict[str, Any]) -> List[FraudAlert]:
        """Rule: Check for transactions from unusual locations."""
        alerts = []
        transaction = enriched_transaction['transaction']
        user_profile = enriched_transaction.get('user_profile', {})
        
        if user_profile and 'country' in user_profile:
            transaction_country = transaction.location.split(',')[-1].strip()[:2]
            user_country = user_profile.get('country', '')
            
            if transaction_country != user_country and transaction_country:
                alert = FraudAlert(
                    transaction_id=transaction.transaction_id,
                    user_id=transaction.user_id,
                    alert_type="UNUSUAL_LOCATION",
                    severity="MEDIUM",
                    description=(
                        f"Transaction from {transaction.location} "
                        f"(user country: {user_country})"
                    ),
                    transaction_details=f"User country: {user_country}, Transaction location: {transaction.location}"
                )
                alerts.append(alert)
                logger.warning(f"UNUSUAL_LOCATION: {transaction.location} vs {user_country}")
        
        return alerts
    
    def check_risk_score(self, enriched_transaction: Dict[str, Any]) -> List[FraudAlert]:
        """Rule: Check user risk score."""
        alerts = []
        transaction = enriched_transaction['transaction']
        user_profile = enriched_transaction.get('user_profile', {})
        
        risk_score = user_profile.get('risk_score', 0) if user_profile else 0
        
        if risk_score > 80:
            severity = "HIGH" if risk_score > 90 else "MEDIUM"
            alert = FraudAlert(
                transaction_id=transaction.transaction_id,
                user_id=transaction.user_id,
                alert_type="RISKY_USER",
                severity=severity,
                description=f"User risk score: {risk_score}",
                transaction_details=f"Risk score: {risk_score}"
            )
            alerts.append(alert)
            logger.warning(f"RISKY USER: score {risk_score}")
        
        return alerts
    
    def check_velocity(self, enriched_transaction: Dict[str, Any]) -> List[FraudAlert]:
        """Rule: Check transaction velocity (total amount in time window)."""
        alerts = []
        transaction = enriched_transaction['transaction']
        user_history = enriched_transaction.get('user_history', [])
        
        ten_minutes_ago = datetime.utcnow() - timedelta(minutes=10)
        recent_total = sum(
            t.get('amount', 0) for t in user_history
            if t.get('timestamp', datetime.min) > ten_minutes_ago
        )
        
        if recent_total > 20000:
            alert = FraudAlert(
                transaction_id=transaction.transaction_id,
                user_id=transaction.user_id,
                alert_type="PATTERN_MATCH",
                severity="HIGH",
                description=f"High velocity: ${recent_total:,.2f} in last 10 minutes",
                transaction_details=f"Total: ${recent_total:,.2f}"
            )
            alerts.append(alert)
            logger.warning(f"HIGH VELOCITY: ${recent_total:,.2f} in 10 minutes")
        
        return alerts
    
    def check_all_rules(self, enriched_transaction: Dict[str, Any]) -> List[FraudAlert]:
        """Apply all fraud detection rules."""
        all_alerts = []
        
        for rule in self.rules:
            try:
                alerts = rule(enriched_transaction)
                all_alerts.extend(alerts)
            except Exception as e:
                logger.error(f"Error applying rule {rule.__name__}: {e}")
        
        if all_alerts:
            logger.info(f"Generated {len(all_alerts)} alerts for transaction {enriched_transaction['transaction'].transaction_id}")
        
        return all_alerts