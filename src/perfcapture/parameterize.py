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
    >>> f(foo=0)
    0 2
    0 4
    >>> @parameterize("baz", [1, 2])
    ... def g(baz):
    ...     print(baz)
    >>> g()
    1
    2
    
    See pytest's docs on the parameterize mark:
    https://docs.pytest.org/en/7.3.x/how-to/parametrize.html
    """
    argnames = argnames.split(',')
    
    # Convert a singular argvalue to a tuple of length 1:
    if len(argnames) == 1:
        for i, argvalue in enumerate(argvalues):
            argvalues[i] = (argvalue,)

    # Sanity check
    for argvalue in argvalues:
        assert len(argvalue) == len(argnames)

    def decorator_parameterize(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper_parameterize(*args, **kwargs) -> None:
            for argvalue in argvalues:
                modified_kwargs = kwargs.copy()
                for i, argname in enumerate(argnames):
                    modified_kwargs.setdefault(argname, argvalue[i])
                func(*args, **modified_kwargs)
        return wrapper_parameterize
    return decorator_parameterize