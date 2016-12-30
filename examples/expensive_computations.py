from random import Random
from time import sleep

from fito import DictDataStore
from fito.operations.decorate import as_operation


@as_operation()
def expensive_computation(some_input):
    print "Computing..."
    sleep(1)


ds = DictDataStore(execute_cache_size=1)
ds.execute_cache.verbose = True

expensive_operations = map(expensive_computation, range(3) * 3)
Random(42).shuffle(expensive_operations)

for op in expensive_operations:
    print "About to excecute: {}".format(op)
    ds.execute(op)
    print
