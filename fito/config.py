import os


def read_bool(key, default=False):
    default_val = '1' if default else '0'
    res = os.environ.get(key, default_val).lower()
    assert res in 'true 1 false 0'.split()
    return res == 'true' or res == '1'


interactive_rehash = read_bool('FITO_IR')
