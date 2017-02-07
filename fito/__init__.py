__version__ = '0.3.3'

from specs.fields import PrimitiveField, SpecField
from specs.base import Spec
from operation_runner import OperationRunner
from operations.decorate import as_operation
from operations.operation import Operation, OperationField
from data_store.dict_ds import DictDataStore
