"""Delta Lake table manager for time travel and versioning."""

from pathlib import Path
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from fraud_detection.config.logger_config import logger

class DeltaTableManager:
    """Manages Delta Lake operations including time travel."""
    
    def __init__(self, spark: SparkSession = None, table_path: str = None):
        from fraud_detection.config.settings import settings
        
        self.spark = spark or self._get_spark()
        self.transactions_table_path = table_path or str(
            settings.LOGS_DIR.parent / "lake" / "delta" / "transactions"
        )
        self._ensure_table_exists()
    
    def _get_spark(self):
        from delta import configure_spark_with_delta_pip
        builder = (SparkSession.builder
                   .master("local[*]")
                   .appName("fraud-detection-delta")
                   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                   .config("spark.sql.catalog.spark_catalog", 
                          "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
        return configure_spark_with_delta_pip(builder).getOrCreate()
    
    def _ensure_table_exists(self):
        """Create transactions table if it doesn't exist."""
        path = Path(self.transactions_table_path.replace("file:", ""))
        if not path.exists():
            # Create empty table
            empty_df = self.spark.createDataFrame([], self._get_schema())
            empty_df.write.format("delta").mode("overwrite").save(self.transactions_table_path)
            logger.info(f"Created Delta table at {self.transactions_table_path}")
    
    def _get_schema(self):
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
        return StructType([
            StructField("transaction_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("alert_type", StringType(), True),
            StructField("severity", StringType(), True),
            StructField("timestamp", TimestampType(), True),
        ])
    
    def read_current(self):
        """Read current version of the table."""
        return self.spark.read.format("delta").load(self.transactions_table_path)
    
    def read_version(self, version: int):
        """Read a specific version using versionAsOf."""
        return (self.spark.read.format("delta")
                .option("versionAsOf", version)
                .load(self.transactions_table_path))
    
    def read_as_of_timestamp(self, timestamp: str):
        """Read version as of specific timestamp."""
        return (self.spark.read.format("delta")
                .option("timestampAsOf", timestamp)
                .load(self.transactions_table_path))
    
    def get_history(self):
        """Get full version history with operation metadata."""
        delta_table = DeltaTable.forPath(self.spark, self.transactions_table_path)
        return delta_table.history()
    
    def vacuum(self, retention_hours: int = 168):
        """Physically remove old files."""
        # Disable safety check (needed for <7 days retention)
        self.spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        delta_table = DeltaTable.forPath(self.spark, self.transactions_table_path)
        delta_table.vacuum(retentionHours=retention_hours)
        logger.info(f"VACUUM completed with {retention_hours} hours retention")