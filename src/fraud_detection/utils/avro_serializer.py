"""Avro serialization and deserialization utilities."""

import io
import json
from typing import Dict, Any, List, Optional
from pathlib import Path
import fastavro
from loguru import logger

class AvroSerializer:
    """Handles Avro serialization/deserialization with schema validation."""
    
    def __init__(self, schema_path: str):
        """Initialize with schema file path."""
        self.schema_path = Path(schema_path)
        if not self.schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_path}")
        
        with open(self.schema_path, 'r') as f:
            schema_dict = json.load(f)
        
        self.schema = fastavro.parse_schema(schema_dict)
        self.schema_name = schema_dict.get('name', 'Unknown')
        logger.debug(f"Loaded schema '{self.schema_name}' from {schema_path}")
    
    def serialize(self, records: List[Dict[str, Any]]) -> bytes:
        """Serialize multiple records to Avro binary format."""
        try:
            buf = io.BytesIO()
            fastavro.writer(buf, self.schema, records)
            return buf.getvalue()
        except Exception as e:
            logger.error(f"Serialization error for schema '{self.schema_name}': {e}")
            raise
    
    def deserialize(self, data: bytes) -> List[Dict[str, Any]]:
        """Deserialize Avro binary data to Python dicts."""
        try:
            buf = io.BytesIO(data)
            return list(fastavro.reader(buf))
        except Exception as e:
            logger.error(f"Deserialization error for schema '{self.schema_name}': {e}")
            raise
    
    def serialize_one(self, record: Dict[str, Any]) -> bytes:
        """Serialize a single record."""
        return self.serialize([record])
    
    def deserialize_one(self, data: bytes) -> Optional[Dict[str, Any]]:
        """Deserialize to a single record."""
        records = self.deserialize(data)
        return records[0] if records else None