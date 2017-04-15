# Ok, you've just came to the idea of doing an experiment
from fito import Operation
from fito.data_store import FileDataStore
from fito.specs.fields import NumericField


# So you code your experiment class that runs depending on some parameter
class Experiment(Operation):
    some_parameter = NumericField(0)

    def apply(self, runner):
        print "Running {}...".format(self)
        return self.some_parameter * 2


def perform_some_runs(data_store, experiments):
    for exp in experiments:
        # I wrote this extra code so it's easier to follow
        # However this "autocaching" can be also performed by either:
        # - setting the default_data_store class field on the class definition
        # - creating the experiment as follows: Experiment(p, out_data_store=data_store)
        if exp not in data_store:
            data_store[exp] = exp.execute()
        else:
            print "Skipping {}".format(exp)

# Let's perform some runs
if __name__ == '__main__':
    ds = FileDataStore('caches')
    # Defining the Experiment class in the same file than the script isn't a very good idea
    #
    # Because when we hash the experiments, the inspect module is going to tell fito that
    # the class was defined on the __main__ module, thus you won't be able to import
    # this class and find it on a data store.
    #
    # Of course, that raises a warning from fito :)
    # I'm using that behaviour for the sake of this example, so I can generate misses on second_experiment.py
    experiments = [Experiment(p) for p in xrange(3)]
    perform_some_runs(ds, experiments)


