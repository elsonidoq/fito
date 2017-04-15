"""
Helper functions that allows you to receive and handle collections in a uniform way
"""
import inspect


def general_new(iterable):
    return type(iterable)()


def general_append(iterable, k, v):
    if isinstance(iterable, list):
        assert k == len(iterable)
        iterable.append(v)
    elif isinstance(iterable, dict):
        iterable[k] = v
    elif isinstance(iterable, tuple):
        assert k == len(iterable)
        iterable = iterable + (v,)
    else:
        raise ValueError()
    return iterable


def general_iterator(iterable):
    if isinstance(iterable, list) or isinstance(iterable, tuple):
        return enumerate(iterable)
    elif isinstance(iterable, dict):
        return iterable.iteritems()
    else:
        raise ValueError()


def is_iterable(obj):
    return isinstance(obj, list) or isinstance(obj, dict) or isinstance(obj, tuple)


def recursive_map(iterable, callable, recursion_condition=None):
    """
    Provides a map that works on lists, dicts and tuples, and is recursive into sub collections (default behaviour)

    :param recursion_condition: Predicate specifying on which cases the function should recurse.
    Default: :py:func:`is_iterable`

    :return: A similar structure to the given iterable where :param callable: was applied

    Example:
    >>> recursive_map([[1], {"some": 3, "stuff":10}], lambda x:x+1)
    >>> [[2], {'some': 4, 'stuff': 11}]

    >>> recursive_map([[1], {2: 3, 5: 10}], lambda k, v:k+v)
    >>> [[1], {2: 5, 5: 15}]

    """
    recursion_condition = recursion_condition or is_iterable
    res = general_new(iterable)

    callable_nargs = len(inspect.getargspec(callable).args) - inspect.ismethod(callable)
    if callable_nargs == 0 or callable_nargs > 2:
        raise RuntimeError("`callable` should be a one or two argument function")

    for k, v in general_iterator(iterable):
        if recursion_condition(v):
            res = general_append(
                res,
                k,
                recursive_map(
                    callable(v),
                    callable,
                    recursion_condition
                )
            )
        else:

            if callable_nargs == 1:
                v = callable(v)
            else:
                v = callable(k, v)

            res = general_append(res, k, v)

    return res


def is_dict(obj): return isinstance(obj, dict)


def matching_fields(d1, d2):
    res = 0

    keys = set(d1).union(d2)

    for k in keys:
        in_both = k in d1 and k in d2
        res += in_both
        if in_both:
            if is_dict(d1[k]) and is_dict(d2[k]):
                res += matching_fields(d1[k], d2[k])
            else:
                res += d1[k] == d2[k]

    return res
