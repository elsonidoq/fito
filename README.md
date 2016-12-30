# Installation

`pip install fito`


# Functionality

Fito is a package that works around the concept of `Operations` and `DataStores`.

The simplest way of thinking it is that a subclass of `Operation` defines
a function, and an instance defines that function being also binded 
to it's arguments. 

If the types of the function inputs are **json-serializable**, 
then the `Operation` is as well. 
Not only that, but operations are also **hashable**.

That leads us to the `DataStore`, whose capability is to index an `Operation`.
There are two implementations, one that uses the file system and 
anotherone that uses [MongoDB](https://www.mongodb.com/).

Extra features:
* `as_operation` Decorator that turns any function into a subclass of `Operation`
* `DataStore.cache`: Decorator to turn automatic caching on any function. 
Creates an operation out of the function and the data store is used for caching the results. 
* Both decorators can be used for functions, instance and class methods.

# How does it look like?
It looks like this
```
from fito.data_store.file import FileDataStore
ds = FileDataStore('test') # can be reeplaced by MongoHashMap

@ds.cache()
def f(x, y=1):
    print "executed"
    return x + y
```

That code is enough to cache the executions of `f` into the file system
You can see more examples here:
* A [simple execution flow](https://github.com/elsonidoq/fito/blob/master/examples/simple_flow.py)
Shows how operations can be used to express entities linked together by their execution

* The [auto caching decorator](https://github.com/elsonidoq/fito/blob/master/examples/auto_caching.py)
Shows how operations joint with data stores can be used for automatic function caching

* The [execution FIFO](https://github.com/elsonidoq/fito/blob/master/examples/expensive_computations.py)
Shows how we can leverage for execution cache

# Contributing
This is my first open source piece of software where I'm commiting myself to mantain the next year. 
Let [me](https://twitter.com/ideasrapidas) know if you happen to use it! 
And please, do not hesitate on sending pull requests :D
