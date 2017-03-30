# Now you realized that you need a new hyperparameter
# Luckily there's a value for that parameter that generates the same behaviour as before
# so you don't want to loose your cached computations

from fito import Operation
from fito.data_store import FileDataStore
from fito.specs.fields import NumericField


# So you code your experiment class that runs depending on some parameter
class Experiment(Operation):
    some_parameter = NumericField(0)
    new_parameter = NumericField(1)

    def apply(self, runner):
        print "Running Experiment({})...".format(self.some_parameter)
        # new_parameter's value == 2 reproduces previous experiment
        return self.some_parameter * self.new_parameter

# Don't want to rewrite that code
from first_experiment import perform_some_runs

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
    experiments = [Experiment(p, q) for p in xrange(3) for q in xrange(2, 4)]
    perform_some_runs(ds, experiments)

