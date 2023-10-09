import functools
from typing import Callable, Iterable, Sequence, Union


def parameterize(
    argnames: str,
    argvalues: Iterable[Union[Sequence[object], object]]) -> Callable:
    """Parameterize a function using the same semantics as pytest.

    For example:
    
    >>> from perfcapture import parameterize
    >>> @parameterize("foo,bar", [(1, 2), (3, 4)])
    ... def f(foo, bar):
    ...     print(foo, bar)
    >>> f()
    1 2
    3 4
    
    See pytest's docs on the parameterize mark:
    https://docs.pytest.org/en/7.3.x/how-to/parametrize.html
    """
    argnames = argnames.split(',')

    # Sanity check
    if len(argnames) > 1:
        for argvalue in argvalues:
            assert len(argvalue) == len(argnames)

    def decorator_parameterize(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper_parameterize(*args, **kwargs) -> None:
            for argvalue in argvalues:
                if len(argnames) == 1:
                    kwargs[argnames[0]] = argvalue
                else:
                    for i, argname in enumerate(argnames):
                        kwargs[argname] = argvalue[i]
                func(*args, **kwargs)
        return wrapper_parameterize
    return decorator_parameterize