from time import sleep

from fito import DictDataStore
from fito.operations.decorate import as_operation


@as_operation()
def expensive_computation(some_input):
    print "Computing..."
    sleep(1)
    return 42


def execute(some_input):
    print
    op = expensive_computation(some_input)
    print "About to execute: {}".format(op)
    ds.execute(op)


# now expensive_computation is a subclass of Operation
print expensive_computation(1)

# create a data store with an execution FIFO cache of size 1
ds = DictDataStore(execute_cache_size=1)
# Hack to make it verbose, will improve that
ds.execute_cache.verbose = True

# Will execute the operation
execute(1)
# Now it is in the cache
execute(1)

# Will execute the operation
# Will remove expensive_computation(1) from the cache
execute(2)

# Cache hit
execute(2)
