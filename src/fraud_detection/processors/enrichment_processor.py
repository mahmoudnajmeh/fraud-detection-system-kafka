"""Transaction enrichment processor."""

from typing import Dict, Any, Optional
from datetime import datetime, UTC
from fraud_detection.models.data_models import Transaction
from loguru import logger

class EnrichmentProcessor:
    """Enriches transactions with user profile and historical data."""
    
    def enrich(self, 
               transaction: Transaction, 
               user_profile: Optional[Dict[str, Any]] = None,
               user_history: Optional[list] = None) -> Dict[str, Any]:
        """Enrich transaction with additional data."""
        enriched = {
            'transaction': transaction,
            'user_profile': user_profile,
            'user_history': user_history or [],
            'enrichment_timestamp': datetime.now(UTC).isoformat()
        }
        
        if user_profile:
            enriched['user_risk_level'] = self._get_risk_level(user_profile.get('risk_score', 0))
            enriched['user_age_days'] = self._calculate_user_age_days(user_profile.get('account_created'))
        
        logger.debug(f"Enriched transaction {transaction.transaction_id}")
        return enriched
    
    def _get_risk_level(self, risk_score: int) -> str:
        """Convert risk score to risk level."""
        if risk_score >= 80:
            return "HIGH"
        elif risk_score >= 50:
            return "MEDIUM"
        elif risk_score >= 20:
            return "LOW"
        return "MINIMAL"
    
    def _calculate_user_age_days(self, account_created) -> Optional[int]:
        """Calculate user account age in days."""
        if not account_created:
            return None
        
        if isinstance(account_created, (int, float)):
            account_created = datetime.fromtimestamp(account_created / 1000, tz=UTC)
        
        age = datetime.now(UTC) - account_created
        return age.days