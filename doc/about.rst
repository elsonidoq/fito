About
-----

Fito aims to be the SQLAlchemy of the data science workflow.

Let me clarify that a little be. In relational databases we have some entities we want to map to rows in tables.
That abstractions helps on the one hand, to detach our code from how serialization is implemented. On the other,
work with better abstractions regarding our domain specific entities.

So, what are the Data Science entities usually like? We usually have input data, probably some ETL, then some
experiments we want to perform and benchmark.

Fito does not aim to provide a pipelining functionality


That means, the goal of fito is to help you specify
experiments, and store metadata attached to them. It also helps you cache expensive computations seamlesly.

It then provides tools to help you on your day to day data science flow issues, so you can just focus on what really
adds value.

What fito is not
----------------
Even though fito allows you to specify depencies between Operations, it does not pretend to be a pipelining
technology like `Luigi <https://github.com/spotify/luigi>`_ ,
`Airflow <https://github.com/apache/incubator-airflow>`_ or `Pinball <https://github.com/pinterest/pinball>`_.

Also, fito does not pretend to provide a way to scale up you code, like what `Spark <http://spark.apache.org/>`_ does.
You can just use fito over spark.

Neither fito pretends to implement efficient key value stores.

Fito just focuses on the ORM part of it.
So if you like some key value tech, or you want to scale up with another tech,
you can still use fito. You've just have to *fitoize* it by building a fito layer on top of it.

For example, :class:`fito.data_store.mongo.MongoHashMap` provides the interface fito expects from a key value store
with a mongo db backend. Same thing with a file system on :class:`fito.data_store.file.FileDataStore` and
using just a python dictionary on :class:`fito.data_store.dict.DictDataStore`
