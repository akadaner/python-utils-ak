from utils_ak.coder.coders import *
from utils_ak.coder.coders.tests.coder_tests import _test_coder


def test_coders():
    _test_coder(JsonCoder(), assert_equal=False)
    _test_coder(MsgPackCoder(), assert_equal=False)


if __name__ == "__main__":
    test_coders()
