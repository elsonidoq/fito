# Fito's interactive rehash
This example aims to show in which case an interactive data store rehash would be useful.

The main use case is when you are on a research workflow. 
In that context, it is a very common to encounter ourselves doing some tests with different strategies.

It is also very frequent that these strategies involve steps.
Sometimes those steps take long time to process, making you want to cache them.
Other times you might just want to attach metadata (like metrics) to them.

Fito provides allows you to seamlesly to make your code hasheable.
However, that benefit can become problematic.

After having performed some runs, you decide that you want to either:
- incorporate a hyper-parameter
- test an alternative to some step
- change a default value of some strategy

The minute you perform those changes your all your previous tests stop hashing to the same bucket.
That's almost equivalent to having deleted everything you have being computing up to now.

Luckily, there's an alternative: Interactive rehashing

If you run your script like this:

```FITO_IR=1 python your_script.py```

You enter into the interactive rehash mode and all caches misses trigger the rehash commandline interface

These examples shows you how:
- You should first need to run

P.S.: As of this readme, the feature is just finished, the diff functionality is very tested,
but that's not the case for the UI.
