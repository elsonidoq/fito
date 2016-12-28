# Installation

`pip install fito`


# Functionality

Fito is a package that works around the concept of `Operations` and `DataStores`.

The simplest way of thinking it is that subclasses of `Operation` defines 
a function, and an instance if this subclass, defines the function 
binded with it's arguments. 

If the types are **json-serializable**, then an `Operation` is as well. 
Not only that, but a operations are also **hashable**.

That lead us to a `DataStore`, whose capability is to index an `Operation`.
There are two implementations, one that uses the `FileSystem` and anotherone that uses `MongoDB`.

Extra features:
* `as_operation` decorator: It turns any function into a subclass of `Operation`
* `DataStore.autosaved`: Decorator to turn automatic cache for any function. Creates an operation out of the function and the data store is used for caching the results. So you can cheaply cache the results either in mongodb or in the file system. 

A small example is worth
```
from fito.data_store.file import FileDataStore
ds = FileDataStore('test') # can be reeplaced by MongoHashMap
@ds.cache()
def f(x, y=1):
    print "executed"
    return x + y
```

An example output would be
```
In [26]: f(1)
executed
Out[26]: 2

In [27]: f(1)
Out[27]: 2
In [30]: ds.clean()

In [31]: f(1)
executed
Out[31]: 3
```