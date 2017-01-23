from file import FileDataStore
try:
    from mongo import MongoHashMap
except ImportError:
    pass
from dict_ds import DictDataStore