from fraud_detection.utils.helpers import generate_id, format_currency

def test_generate_id():
    id1 = generate_id()
    id2 = generate_id()
    assert len(id1) == 36
    assert id1 != id2

def test_format_currency():
    assert format_currency(1000.50, "USD") == "$1,000.50"
    assert format_currency(500, "EUR") == "€500.00"