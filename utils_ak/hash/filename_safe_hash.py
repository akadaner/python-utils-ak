import base64
import hashlib


def safe_filename_hash(s, n=8):
    s = s.encode("utf-8")
    return (
        base64.urlsafe_b64encode(hashlib.md5(s).digest())[:n]
        .decode("utf-8")
        .replace("_", "*")
    )


def test_safe_filename_hash():
    print(safe_filename_hash("foo-bar"))


if __name__ == "__main__":
    test_safe_filename_hash()
