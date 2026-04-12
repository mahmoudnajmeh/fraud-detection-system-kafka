"""Helper utility functions."""

import uuid
import json
from datetime import datetime, date
from typing import Any

def generate_id() -> str:
    """Generate a unique ID."""
    return str(uuid.uuid4())

def json_serializer(obj: Any) -> str:
    """Custom JSON serializer for datetime objects."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

def safe_json_dumps(data: Any, **kwargs) -> str:
    """Safely dump JSON with datetime support."""
    return json.dumps(data, default=json_serializer, **kwargs)

def safe_json_loads(data: str) -> Any:
    """Safely load JSON."""
    try:
        return json.loads(data)
    except json.JSONDecodeError:
        return None

def format_currency(amount: float, currency: str = "USD") -> str:
    """Format amount as currency string."""
    symbols = {"USD": "$", "EUR": "€", "GBP": "£", "JPY": "¥"}
    symbol = symbols.get(currency, currency)
    return f"{symbol}{amount:,.2f}"