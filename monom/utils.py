import logging
import random
import string
import time
from collections import abc
from functools import reduce, partial
from keyword import iskeyword
from operator import add
from string import capwords
from threading import Lock
from typing import (
    Dict,
    Mapping,
    MutableMapping,
    List,
    MutableSequence,
    Iterable,
    Iterator,
    Callable,
    Optional,
    Union,
    Any,
)

from bson.son import SON

__all__ = [
    'DotSon',
    'Timer',
    'pluralize',
    'random_string',
    'random_digits',
    'random_letters',
    'random_lower_letters',
    'random_upper_letters',
    'have_same_shape',
    'walk_keys',
    'to_camelcase',
    'hump_keys',
    'get_dict_item_with_dot',
    'Missing',
    'classproperty',
    'cachedproperty',
    'isclass',
    'not_none',
    'normalize_indexes',
    'default_index_name',
    'set_logger',
    'get_logger',
    'debug',
    'info',
    'warn'
]


class DotSon(abc.Mapping):
    """ A :class:`DotSon` is a special dict whose item can be accessed using dot notation.
    There will be a performance penalty when accessing deep-nested element.

    >>> d = DotSon({'name': 'foo', 'hobbits': [{'name': 'bar'}]})
    >>> d.name
    'foo'
    >>> d.hobbits[0].name
    'bar'
    >>> type(d.hobbits[0]) == DotSon
    True
    """

    def __new__(cls, obj: Any) -> Any:
        if isinstance(obj, abc.Mapping):
            return super().__new__(cls)
        elif isinstance(obj, abc.MutableSequence):
            # noinspection PyCallingNonCallable
            return [cls(item) for item in obj]
        else:
            return obj

    def __init__(self, mapping: Mapping):
        # create a shallow copy for security
        self._data = {}
        for key, value in mapping.items():
            if not key.isidentifier():
                raise AttributeError("invalid identifier: {!r}".format(key))
            if iskeyword(key):
                key += '_'
            self._data[key] = value

    def __getattr__(self, name: str) -> Any:
        if hasattr(self._data, name):
            return getattr(self._data, name)
        try:
            return DotSon(self._data[name])
        except KeyError:
            raise AttributeError('{!r} has no attribute {!r}'.format(self, name))

    def __getitem__(self, item: str) -> Any:
        return self._data[item]

    def keys(self) -> Iterable:
        return self._data.keys()

    def values(self) -> Iterable:
        return self._data.values()

    def items(self) -> Iterable:
        return self._data.items()

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def __len__(self) -> int:
        return len(self._data)

    def __iter__(self) -> Iterator:
        return iter(self._data)

    def __str__(self) -> str:
        return str(self._data)


class Timer:
    """Record the time that a task has taken"""

    def __init__(self, func: Callable = time.perf_counter):
        self.elapsed = 0.0
        self._func = func
        self._start = None

    def start(self) -> None:
        if self._start is not None:
            raise RuntimeError('Already started')
        self._start = self._func()

    def stop(self) -> None:
        if self._start is None:
            raise RuntimeError('Not started')
        end = self._func()
        self.elapsed += end - self._start
        self._start = None

    def reset(self) -> None:
        self.elapsed = 0.0

    @property
    def running(self) -> bool:
        return self._start is not None

    def __enter__(self) -> 'Timer':
        self.start()
        return self

    def __exit__(self, *args) -> None:
        self.stop()


def random_string(length: int = 10, type: str = 'all') -> str:
    choices = {
        'digit': string.digits,
        'letter': string.ascii_letters,
        'uppercase': string.ascii_uppercase,
        'lowercase': string.ascii_lowercase,
        'all': string.digits + string.ascii_letters
    }
    return ''.join(random.choice(choices[type]) for _ in range(length))


random_digits = partial(random_string, type='digit')
random_letters = partial(random_string, type='letter')
random_upper_letters = partial(random_string, type='uppercase')
random_lower_letters = partial(random_string, type='lowercase')


ABERRANT_PLURAL_MAP = {
    'appendix': 'appendices',
    'barracks': 'barracks',
    'cactus': 'cacti',
    'child': 'children',
    'criterion': 'criteria',
    'deer': 'deer',
    'echo': 'echoes',
    'elf': 'elves',
    'embargo': 'embargoes',
    'focus': 'foci',
    'fungus': 'fungi',
    'goose': 'geese',
    'hero': 'heroes',
    'hoof': 'hooves',
    'index': 'indices',
    'knife': 'knives',
    'leaf': 'leaves',
    'life': 'lives',
    'man': 'men',
    'mouse': 'mice',
    'nucleus': 'nuclei',
    'person': 'people',
    'phenomenon': 'phenomena',
    'potato': 'potatoes',
    'self': 'selves',
    'syllabus': 'syllabi',
    'tomato': 'tomatoes',
    'torpedo': 'torpedoes',
    'veto': 'vetoes',
    'woman': 'women',
}
VOWELS = set('aeiou')


def pluralize(singular: str) -> str:
    """Return plural form of given lowercase singular word.
    Refer to: http://code.activestate.com/recipes/577781-pluralize-word-convert-singular-word-to-its-plural/

    >>> pluralize('user')
    'users'
    """

    if not singular:
        return ''
    plural = ABERRANT_PLURAL_MAP.get(singular)
    if plural:
        return plural
    root = singular
    try:
        if singular[-1] == 'y' and singular[-2] not in VOWELS:
            root = singular[:-1]
            suffix = 'ies'
        elif singular[-1] == 's':
            if singular[-2] in VOWELS:
                if singular[-3:] == 'ius':
                    root = singular[:-2]
                    suffix = 'i'
                else:
                    root = singular[:-1]
                    suffix = 'ses'
            else:
                suffix = 'es'
        elif singular[-2:] in ('ch', 'sh'):
            suffix = 'es'
        else:
            suffix = 's'
    except IndexError:
        suffix = 's'
    plural = root + suffix
    return plural


def have_same_shape(obj1: Any, obj2: Any) -> bool:
    """Determine if two (json) objects have same shapes.

    >>> have_same_shape(1, 1.0)
    True
    >>> have_same_shape({'a': {'b': [{'c': [1, 2, 3]}, 1]}}, {'a': {'b': [{'c': (1, 2, 3)}, 1.0]}})
    True
    """

    if obj1 == obj2:
        return True

    if isinstance(obj1, abc.Mapping) and isinstance(obj2, abc.Mapping):
        if set(obj1) != set(obj2):
            return False
        return all(have_same_shape(obj1[key], obj2[key]) for key in obj1.keys())
    elif isinstance(obj1, (abc.MutableSequence, tuple)) and isinstance(obj2, (abc.MutableSequence, tuple)):
        if len(obj1) != len(obj2):
            return False
        return all(have_same_shape(item[0], item[1]) for item in zip(obj1, obj2))
    else:
        return False


def walk_keys(fn: Callable, obj: Any) -> Any:
    """Walk keys of a (json) object, mapping them with a function

    >>> walk_keys(lambda x: x.upper(), {'foo': 1, 'bar': [{'fox': 2}]})
    {'FOO': 1, 'BAR': [{'FOX': 2}]}
    """

    if isinstance(obj, abc.Mapping):
        return {fn(k): walk_keys(fn, v) for k, v in obj.items()}
    elif isinstance(obj, (abc.MutableSequence, tuple)):
        return [walk_keys(fn, item) for item in obj]
    else:
        return obj


def to_camelcase(s: str) -> str:
    """Convert a string to its camelcase form.

    >>> to_camelcase('foo_bar')
    'fooBar'
    >>> to_camelcase('foo_bar__')
    'fooBar'
    >>> to_camelcase('__Foo__Bar')
    'fooBar'
    """

    s1 = s.strip('_')
    if s1 == '':
        return ''

    arr = []
    cur = []
    prev = ''

    for c in s1:
        if c == '_' and prev != '_':
            arr.append(''.join(cur))
            cur = []
        elif c and c != '_':
            cur.append(c)
        prev = c

    arr.append(''.join(cur))
    return arr[0].lower() + ''.join(map(capwords, arr[1:]))


def hump_keys(data: MutableMapping) -> MutableMapping:
    """Convert a dict's keys to camelcase

    >>> hump_keys({'a_b': {'a_b': 1}})
    {'aB': {'aB': 1}}
    """

    return walk_keys(to_camelcase, data)


def get_dict_item_with_dot(data: MutableMapping, name: str) -> Any:
    """Get a dict item using dot notation

    >>> get_dict_item_with_dot({'a': {'b': 42}}, 'a')
    {'b': 42}
    >>> get_dict_item_with_dot({'a': {'b': 42}}, 'a.b')
    42
    """
    if not name:
        return data
    item = data
    for key in name.split('.'):
        item = item[key]
    return item


def isclass(obj: Any) -> bool:
    """Determine if an object is a `class` object

    >>> isclass(type)
    True
    >>> isclass(object)
    True
    >>> isclass({})
    False
    """

    return issubclass(type(obj), type)


def not_none(value: Any) -> bool:
    """Return true if value is not none or all items in value are not none.

    >>> not_none(None)
    False
    >>> not_none('abc')
    True
    >>> not_none([1, 2, None])
    False
    """
    if not isinstance(value, abc.Sequence):
        value = [value]
    return all([item is not None for item in value])


class Missing:
    """A Singleton which indicates a value does not exist. NEVER try to subclass it.

    >>> Missing() == Missing()
    True
    """

    _instance = None

    def __new__(cls, *args, **kw):
        if cls._instance is None:
            cls._instance = super().__new__(cls, *args, **kw)
        return cls._instance

    def __str__(self):
        return '<Missing>'

    __repr__ = __str__


# noinspection PyPep8Naming
class classproperty:
    """Class property which behaves like `property`."""

    def __init__(self, fget: Callable = None):
        self.fget = fget
        self.__doc__ = fget.__doc__

    def __get__(self, instance, cls=None) -> Any:
        return self.fget(cls)

    def getter(self, fget: Callable) -> 'classproperty':
        self.fget = fget
        return self


# noinspection PyPep8Naming
class cachedproperty:
    """Decorator that converts a method with a single self argument into a property cached on the instance.
    Use fget, fset and fdel attributes to mimic `property`.
    """

    fset = fdel = None

    def __init__(self, fget: Callable):
        self.fget = fget
        self.__doc__ = fget.__doc__

    def __get__(self, instance, cls=None) -> Any:
        if instance is None:
            return self
        res = instance.__dict__[self.fget.__name__] = self.fget(instance)
        return res


def normalize_indexes(indexes: List[Union[str, tuple, list, MutableMapping]]) -> List[Dict]:
    """Convert the abbr to the arguments that can be used by :meth:`pymongo.collection.Collection.create_index`:
    [{'key': [('a', 1), ('b', -1), ...], 'option1': value1, ...}, ...]

    >>> normalize_indexes([])
    []
    >>> normalize_indexes(['a'])
    [{'key': [('a', 1)]}]
    >>> normalize_indexes([('a', -1)])
    [{'key': [('a', -1)]}]
    >>> normalize_indexes(['a', ('b', -1)])
    [{'key': [('a', 1)]}, {'key': [('b', -1)]}]
    >>> normalize_indexes([[('a', 1), ('b', 1)]])
    [{'key': [('a', 1), ('b', 1)]}]
    >>> normalize_indexes([['a', ('b', 1), ('c', -1)]])
    [{'key': [('a', 1), ('b', 1), ('c', -1)]}]
    >>> normalize_indexes([{'key': [('a', 1)], 'expire_after_seconds': 3600}])
    [{'expireAfterSeconds': 3600, 'key': [('a', 1)]}]
    """

    def normalize(idx: Union[str, tuple, list]):
        if isinstance(idx, str):
            return [(idx, 1)]
        elif isinstance(idx, tuple) and len(idx) == 2 and type(idx[0]) is str and idx[1] in (1, -1):
            return [idx]
        elif isinstance(idx, list):
            return reduce(add, [normalize(x) for x in idx])
        else:
            raise ValueError("cannot parse {!r} to standard index format".format(idx))

    rv = []
    for index in indexes:
        if isinstance(index, (str, tuple, list)):
            rv.append({'key': normalize(index)})
        elif isinstance(index, MutableMapping):
            key = normalize(index.pop('key'))
            index = hump_keys(index)
            index['key'] = key
            rv.append(index)
    return rv


def default_index_name(index_key: Union[Dict, MutableSequence]) -> str:
    """Get mongo's default index name.

    >>> default_index_name([('a', 1), ('b', 1)])
    'a_1_b_1'
    >>> default_index_name([('a', -1), ('b', 1)])
    'a_-1_b_1'
    >>> default_index_name([('a', 1), ('b', -1)])
    'a_1_b_-1'
    """

    if isinstance(index_key, MutableSequence):
        index_key = SON(index_key)
    rv = []
    for k, v in index_key.items():
        if isinstance(v, float):
            v = int(v)
        rv.append(k + '_' + str(v))
    return '_'.join(rv)


# Logging utils
DEFAULT_LOGGER_NAME = 'monom'
DEFAULT_LOGGING_FORMAT = '[%(asctime)s] %(levelname)s in `monom`: %(message)s'
DEFAULT_LOGGING_LEVEL = logging.WARNING

_logger: Optional[logging.Logger] = None
_lock = Lock()


def set_logger(logger: logging.Logger) -> None:
    global _logger
    _logger = logger


def get_logger() -> logging.Logger:
    if _logger is None:
        _set_default_logger()
    return _logger


def _set_default_logger() -> None:
    global _logger
    with _lock:
        _logger = logging.getLogger(DEFAULT_LOGGER_NAME)
        formatter = logging.Formatter(DEFAULT_LOGGING_FORMAT)
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        _logger.addHandler(handler)
        _logger.setLevel(DEFAULT_LOGGING_LEVEL)


def debug(msg: str, *args, **kw) -> None:
    logger = get_logger()
    logger.log(logging.DEBUG, msg, *args, **kw)


def info(msg: str, *args, **kw) -> None:
    logger = get_logger()
    logger.log(logging.INFO, msg, *args, **kw)


def warn(msg: str, *args, **kw) -> None:
    logger = get_logger()
    logger.log(logging.WARNING, msg, *args, **kw)


if __name__ == '__main__':
    import doctest
    doctest.testmod()
