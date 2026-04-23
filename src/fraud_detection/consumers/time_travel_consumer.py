"""Time travel consumer for auditing historical data."""

from pathlib import Path
import sys
from datetime import datetime

sys.path.append(str(Path(__file__).parent.parent.parent.parent))

from fraud_detection.config.logger_config import logger
from fraud_detection.config.settings import settings
from fraud_detection.utils.delta_table_manager import DeltaTableManager

class TimeTravelConsumer:
    """Consumer that uses Delta Lake time travel for auditing."""
    
    def __init__(self):
        self.manager = DeltaTableManager()
        logger.info("Time Travel Consumer initialized")
    
    def audit_version(self, version: int):
        """Audit what the data looked like at a specific version."""
        df = self.manager.read_version(version)
        count = df.count()
        logger.bind(audit=True).info(
            f"AUDIT | version={version} | row_count={count} | "
            f"timestamp={datetime.utcnow().isoformat()}"
        )
        return df
    
    def show_history(self):
        """Show complete version history."""
        history = self.manager.get_history()
        history.select("version", "timestamp", "operation", "operationMetrics").show()
        return history
    
    def recover_deleted_data(self, version_before_delete: int):
        """Recover data from before an accidental deletion."""
        logger.warning(f"RECOVERY | restoring from version {version_before_delete}")
        
        df = self.manager.read_version(version_before_delete)
        
        # Overwrite current table with old snapshot
        df.write.format("delta").mode("overwrite").save(
            self.manager.transactions_table_path
        )
        
        logger.bind(audit=True).info(
            f"RECOVERY_COMPLETE | restored_version={version_before_delete} | "
            f"rows_restored={df.count()}"
        )