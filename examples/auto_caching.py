from fito import DictDataStore

ds = DictDataStore()

@ds.cache()
def f(x, y=1):
    print "Executed!"
    return x + y

def execute(*input):
    print "Calling f({})".format(', '.join(map(str, input)))
    print f(*input)
    print

print
# sohuld print executed
execute(1)

# cache hit!
execute(1)

print "Cache contents"
print ds.data
print

print "Emptying cache..."
ds.data = {}

# sohuld print executed again
execute(1)

execute(1, 2)
