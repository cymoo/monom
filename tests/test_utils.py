import doctest
import logging
import time
from collections import OrderedDict
from collections.abc import Iterable, Mapping

import pytest
from bson.son import SON

import monorm.utils
from monorm.utils import *

doctest.testmod(monorm.utils)


class TestDotSon:
    data = {
        'name': 'cymoo',
        'addr': {'city': 'shanghai', 'district': 'pudong'},
        'skills': [
            {'name': 'java', 'score': 7},
            {'name': 'python', 'score': 9},
            {'name': 'javascript', 'score': 9},
        ]
    }

    @staticmethod
    def do_assert(data):
        dd = DotSon(data)
        assert dd.name == 'cymoo'
        assert dd['name'] == 'cymoo'
        assert type(dd.addr) == DotSon
        assert type(dd.skills[0] == DotSon)
        assert dd.skills[1].name == 'python'

        assert list(dd.keys()) == list(data.keys())
        assert list(dd.values()) == list(data.values())
        assert list(dd.items()) == list(data.items())
        assert dd.get('name') == 'cymoo'
        assert len(dd) == 3
        assert isinstance(iter(dd), Iterable)
        assert isinstance(dd, Mapping)

    def test_with_dict(self):
        self.do_assert(self.data)

    def test_with_ordered_dict(self):
        self.do_assert(OrderedDict(self.data))

    def test_with_son(self):
        self.do_assert(SON(self.data))

    def test_raise_type_error(self):
        dd = DotSon(self.data)
        with pytest.raises(TypeError):
            dd['foo'] = 'bar'


def test_timer():
    with Timer() as timer:
        assert timer.running
        with pytest.raises(RuntimeError):
            timer.start()
        time.sleep(0.1)
    assert 0.1 < timer.elapsed < 0.11
    assert not timer.running
    with pytest.raises(RuntimeError):
        timer.stop()
    timer.reset()
    assert timer.elapsed == 0


def test_pluralize():
    assert pluralize('goose') == 'geese'
    assert pluralize('dolly') == 'dollies'
    assert pluralize('user') == 'users'
    assert pluralize('pass') == 'passes'
    assert pluralize('x') == 'xs'
    assert pluralize('church') == 'churches'
    assert pluralize('jone') == 'jones'
    assert pluralize('genius') == 'genii'


def test_same_shape():
    same = have_same_shape
    assert same(1, 1.0)
    assert same([1, 2, 3], (1, 2, 3))
    assert same({'a': 1, 'b': 3}, {'b': 3, 'a': 1})
    assert not same('123', [1, 2, 3])
    assert not same([1, 2, 3], [1, 3, 2])
    assert same(
        {'a': 1, 'b': [{'c': [1, 2, 3, {'d': [4, 5, {'e': {'f': [6, 7]}}]}]}]},
        {'a': 1, 'b': [{'c': [1, 2, 3, {'d': [4, 5, {'e': {'f': [6, 7]}}]}]}]}
    )
    assert same({'a': 1, 'b': 2}, OrderedDict([('a', 1), ('b', 2)]))
    assert same({'a': 1, 'b': 2}, SON([('a', 1), ('b', 2)]))


def test_walk_keys():
    def to_upper(x: str):
        return x.upper()

    dk1 = {'foo': 1, 'bar': {'fox': 2}}
    dk2 = walk_keys(to_upper, dk1)
    assert dk2['FOO'] == 1 and dk2['BAR']['FOX'] == 2

    arr1 = [1, 2, 3]
    arr2 = walk_keys(to_upper, arr1)
    assert arr1 == arr2

    arr3 = [{'a': 1}, {'b': 1}]
    arr4 = walk_keys(to_upper, arr3)
    assert arr4[0]['A'] == 1 and arr4[1]['B'] == 1


def test_to_camelcase():
    assert to_camelcase('___') == ''
    assert to_camelcase('foo_bar') == 'fooBar'
    assert to_camelcase('__Foo__Bar') == 'fooBar'
    assert to_camelcase('foo_bar__') == 'fooBar'


def test_hump_keys():
    hump = hump_keys
    assert hump({'expire_after_seconds': 1}) == {'expireAfterSeconds': 1}
    assert hump({'a_b': 1, '__b_c__': {'__a_b_c__': 1}}) == {'aB': 1, 'bC': {'aBC': 1}}


def test_isclass():
    class Meta(type):
        pass

    class Foo:
        pass

    class Bar(metaclass=Meta):
        pass

    f = Foo()
    assert isclass(Foo)
    assert isclass(Bar)
    assert isclass(Meta)
    assert isclass(object)
    assert isclass(type)
    assert not isclass(f)


def test_not_none():
    assert not not_none(None)
    assert not_none('abc')
    assert not not_none([1, 2, None])
    assert not_none([1, 2, 3])


def test_class_property():
    class Foo:
        @classproperty
        def bar(cls):
            return 42

    assert Foo.bar == 42


def test_cached_property():
    class Foo:
        i = 1

        @cachedproperty
        def bar(self):
            self.i += 1
            return 42
    f = Foo()
    assert f.i == 1
    assert f.bar == 42
    assert f.i == 2
    assert f.bar == 42
    assert f.i == 2


def test_default_index_name():
    assert default_index_name([('a', 1), ('b', 1)]) == 'a_1_b_1'
    assert default_index_name([('a', -1), ('b', 1)]) == 'a_-1_b_1'
    assert default_index_name([('a', 1), ('b', -1)]) == 'a_1_b_-1'


def test_normalize_indexes():
    assert normalize_indexes([]) == []
    assert normalize_indexes(['a']) == [{'key': [('a', 1)]}]
    assert normalize_indexes([('a', -1)]) == [{'key': [('a', -1)]}]
    assert normalize_indexes(['a', ('b', -1)]) == [{'key': [('a', 1)]}, {'key': [('b', -1)]}]
    assert normalize_indexes([[('a', 1), ('b', 1)]]) == [{'key': [('a', 1), ('b', 1)]}]
    assert normalize_indexes([['a', ('b', 1), ('c', -1)]]) == [{'key': [('a', 1), ('b', 1), ('c', -1)]}]
    assert normalize_indexes([{'key': [('a', 1)], 'expire_after_seconds': 3600}]) == \
        [{'expireAfterSeconds': 3600, 'key': [('a', 1)]}]


class TestLogger:
    def test_get_logger(self, caplog):
        logger = get_logger()
        logger.warning('hello')
        record = caplog.records[0]

        assert 'hello' in record.message
        assert record.name == 'monorm'

    def test_set_logger(self, caplog):
        set_logger(logging.getLogger('foobar'))
        logger = get_logger()
        logger.warning('hi')
        record = caplog.records[0]

        assert 'hi' in record.message
        assert record.name == 'foobar'


if __name__ == '__main__':
    pytest.main()
