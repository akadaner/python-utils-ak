""" Math functionality. """
from decimal import ROUND_HALF_DOWN, ROUND_DOWN, ROUND_CEILING, ROUND_05UP, ROUND_FLOOR, ROUND_HALF_EVEN, ROUND_HALF_UP, ROUND_UP, Decimal as D

ROUND_DIC = {'round': ROUND_HALF_EVEN,
             'nearest': ROUND_HALF_EVEN,
             'floor': ROUND_FLOOR,
             'ceil': ROUND_CEILING,
             'down': ROUND_DOWN,
             'zero': ROUND_DOWN,
             'up': ROUND_UP}


def decimal_round(a, b, rounding='round', precision=0, strip=False):
    a, b = D(a), D(b)
    n = a / b / D('.1') ** precision
    rounding = ROUND_DIC.get(rounding, rounding)
    n = n.quantize(D('1.'), rounding=rounding)
    res = n * b * D('0.1') ** precision
    if strip:
        res = strip_decimal(res)
    return res


# be careful to use this for precise values: rounding 5. to 1. with floor method may result in 4 due to python floating system (5. == 4.99999999999) in python and stuff
def custom_round(a, b, rounding='round', precision=0):
    n = a / b / 0.1 ** precision
    n = int(decimal_round(n, 1, rounding))
    return n * b * 0.1 ** precision


def cast_decimal(f, precision=15, strip=True):
    d = decimal_round(f, D('1.0'), precision=precision)
    if strip:
        s = str(d)
        s = strip_zeros(s)
        d = D(s)
    return d


def strip_decimal(d):
    return D(strip_zeros(str(d)))


def strip_zeros(s):
    return s.rstrip('0').rstrip('.') if '.' in s else s


if __name__ == '__main__':
    roundings = [ROUND_HALF_DOWN, ROUND_DOWN, ROUND_CEILING, ROUND_05UP, ROUND_FLOOR, ROUND_HALF_EVEN, ROUND_HALF_UP, ROUND_UP]
    roundings += list(ROUND_DIC.keys())

    for rounding in roundings:
        for v in [1.99, 1.05, -1.99, -1.05]:
            print(rounding, v, 0.1, custom_round(v, 0.1, rounding))

    # 1.99
    print(decimal_round('1.99', '0.01', rounding='ROUND_FLOOR'))
    # may be 1.98 since 1.99 may be 1.9899999999
    print(decimal_round(1.99, '0.01', rounding='ROUND_FLOOR', precision=0))
    print(decimal_round(1.99, '0.01', rounding='ROUND_FLOOR', precision=2))
    # certainly 1.99, since we round 1.9899999 to 1.99 first
    print(decimal_round(1.99, '0.0100', rounding='ROUND_FLOOR', precision=10))
    print(decimal_round(1.99, '0.0100', rounding='ROUND_FLOOR', precision=10))
    print(decimal_round(1.99, '0.0100', rounding='ROUND_FLOOR', precision=10, strip=True))
