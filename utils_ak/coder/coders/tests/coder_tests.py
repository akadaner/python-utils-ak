import decimal
import datetime as datetime_module


def _test_coder(coder, assert_equal=True):
    obj = {
        "int": 1,
        "datetime": datetime_module.datetime.now(),
        "decimal": decimal.Decimal("0.12431234123000"),
        "date": datetime_module.date(2018, 1, 1),
        "time": datetime_module.datetime.now().time(),
    }

    encoded = coder.encode(obj)
    decoded = coder.decode(encoded)
    print(encoded)
    print(decoded)
    if assert_equal:
        assert obj == decoded
