def test_coder(coder, assert_equal=True):
    import datetime
    import decimal

    obj = {
        "int": 1,
        "datetime": datetime.datetime.now(),
        "decimal": decimal.Decimal("0.12431234123000"),
        "date": datetime.date(2018, 1, 1),
        "time": datetime.datetime.now().time(),
    }

    encoded = coder.encode(obj)
    decoded = coder.decode(encoded)
    print(encoded)
    print(decoded)
    if assert_equal:
        assert obj == decoded
