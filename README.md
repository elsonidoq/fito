# About

Fito is the data science ORM. 
Fito is a Python module that helps you organize you code, and integrate
different technologies while handling a consistent and clear 
object model.

Things like:
 * Mapping between config files and behaviour, 
 * Caching results of execution (in memory, or in any key value technology)
 * Or attaching metadata to executions (like metrics, scores, plots, etc)
  
become trivial

# Functionality

Fito is a package that works around the concept of four concepts:
First there are `Specs`. A `Spec` specifies an object.
It provides the capability of specifiyng things, like models or data sources.
Also an `Spec` can be combined with another `Spec` which allows them to specify
things like experiments that combine both models and data sources.
Specs are both **json-serializable** and **hasheable**.
  
An `Operation` is an spec that computes something out of it. Can be though
as a [currified function](https://en.wikipedia.org/wiki/Currying#Illustration)

That leads us to the `DataStore`, whose capability is to index an `Spec`.
There are two implementations, one that uses the file system and 
anotherone that uses [MongoDB](https://www.mongodb.com/).

One nice combination of having this abstraction, is that we can do automatic caching.
That can be performed just by linking operations and data stores together 

Besides that, there's a very helpful decorator, `as_operation` that 
turns any function into a subclass of `Operation`.

# How does it look like?
It looks like this
```
from fito.data_store import DictDataStore
from fito import as_operation

ds = DictDataStore() # Can be any implementation of data store

@as_operation(cache_on=ds)
def f(x, y=1):
    return x + y

f(1).execute() # executed
f(1).execute() # retrieved from cache
```

That code is enough to cache the executions of `f` into memory

You can see more examples here:
* A [simple execution flow](https://github.com/elsonidoq/fito/blob/master/examples/Simple%20Flow.ipynb): 
Shows how operations can be used to express entities linked together by their execution

* The [auto caching decorator](https://github.com/elsonidoq/fito/blob/master/examples/Auto%20Caching.ipynb): 
Shows how operations joint with data stores can be used for automatic function caching

* The [execution FIFO](https://github.com/elsonidoq/fito/blob/master/examples/Expensive%20computations.ipynb): 
Shows how we can leverage on execution cache to avoid recomputing recently executed operations 

* [Managing config files](https://github.com/elsonidoq/fito/blob/master/examples/Handle%20config%20files.ipynb)

# Contributing
This is my first open source piece of software where I'm commiting myself to mantain for the next year. 

Let [me](https://twitter.com/ideasrapidas) know if you happen to use it! 

And please, do not hesitate on sending pull requests :D

# Installation

`pip install fito`


