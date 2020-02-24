from collections import abc
from collections import OrderedDict
from datetime import datetime
from typing import List, Any

import pytest
from bson.objectid import ObjectId
from bson.son import SON

from monom import BaseModel, EmbeddedModel
from monom.fields import *


class TestBaseField:
    def test_str(self):
        class MM(BaseModel):
            f: str

        obj = MM(f='a')
        assert isinstance(obj.f, str)

        with pytest.raises(ValidationError):
            MM(f=1)

    def test_str_max_length(self):
        class MM(BaseModel):
            f = StringField(max_length=3)

        MM(f='ab')
        MM(f='abc')
        with pytest.raises(ValidationError) as err:
            MM(f='abcd')
        assert 'greater than' in err.value.msg

    def test_str_min_length(self):
        class MM(BaseModel):
            f = StringField(min_length=3)

        MM(f='abcd')
        MM(f='abc')
        with pytest.raises(ValidationError) as err:
            MM(f='ab')
        assert 'less than' in err.value.msg

    def test_int(self):
        class MM(BaseModel):
            f: int

        obj = MM(f=1)
        assert isinstance(obj.f, int)

        with pytest.raises(ValidationError):
            MM(f='a')

    def test_int_max_value(self):
        class MM(BaseModel):
            f = IntField(max_value=13)

        MM(f=12)
        MM(f=13)
        with pytest.raises(ValidationError) as err:
            MM(f=14)
        assert 'greater than' in err.value.msg

    def test_int_min_value(self):
        class MM(BaseModel):
            f = IntField(min_value=13)

        MM(f=14)
        MM(f=13)
        with pytest.raises(ValidationError) as err:
            MM(f=12)
        assert 'less than' in err.value.msg

    def test_float(self):
        class MM(BaseModel):
            f: float

        obj = MM(f=3.14)
        assert isinstance(obj.f, float)

        with pytest.raises(ValidationError):
            MM(f='a')

    def test_float_max_value(self):
        class MM(BaseModel):
            f = FloatField(max_value=3.14)

        MM(f=3.13)
        MM(f=3.14)
        with pytest.raises(ValidationError) as err:
            MM(f=3.15)
        assert 'greater than' in err.value.msg

    def test_float_min_value(self):
        class MM(BaseModel):
            f = FloatField(min_value=3.14)

        MM(f=3.15)
        MM(f=3.14)
        with pytest.raises(ValidationError) as err:
            MM(f=3.13)
        assert 'less than' in err.value.msg

    def test_bool(self):
        class MM(BaseModel):
            f: bool

        obj = MM(f=True)
        assert isinstance(obj.f, bool)

        with pytest.raises(ValidationError):
            MM(f=1)

    def test_bytes(self):
        class MM(BaseModel):
            f: bytes

        obj = MM(f=b'a')
        assert isinstance(obj.f, bytes)

        with pytest.raises(ValidationError):
            MM(f='a')

    def test_datetime(self):
        class MM(BaseModel):
            f: datetime

        obj = MM(f=datetime.utcnow())
        assert isinstance(obj.f, datetime)

        with pytest.raises(ValidationError):
            MM(f='20200202')

    def test_object_id(self):
        class MM(BaseModel):
            f: ObjectId

        obj = MM(f=ObjectId())
        assert isinstance(obj.f, ObjectId)

        with pytest.raises(ValidationError):
            MM(f='abc')

    def test_dict(self):
        class MM(BaseModel):
            f: dict

        obj = MM(f={'a': 1})
        assert isinstance(obj.f, dict)

        with pytest.raises(ValidationError):
            MM(f='abc')

    def test_list(self):
        class MM(BaseModel):
            f: list

        obj = MM(f=[1, 'a'])
        assert isinstance(obj.f, list)

        with pytest.raises(ValidationError) as err:
            MM(f='abc')


def test_any_field():
    class MainModel(BaseModel):
        f: Any

    MainModel(f=1)
    MainModel(f={})
    MainModel(f=[1, 2, 3])


class TestEmbeddedField:
    def test_attr_proxy_with_set_get(self):
        class SubSubModel(EmbeddedModel):
            f: str

        class SubModel(EmbeddedModel):
            f1: str
            f2: SubSubModel

        class MainModel(BaseModel):
            f1: SubModel

        obj = MainModel(f1={'f1': 'hello', 'f2': {'f': 'hello again'}})

        assert isinstance(obj.f1, SubModel)
        assert obj.f1.f1 == 'hello'

        assert isinstance(obj.f1.f2, SubSubModel)
        assert obj.f1.f2.f == 'hello again'

        with pytest.raises(AttributeError):
            _ = obj.f2
        with pytest.raises(AttributeError):
            _ = obj.f1.f3

    def test_feed_with_embedded_model_obj(self):
        class SubModel(EmbeddedModel):
            f1: str

        class MainModel(BaseModel):
            f1: SubModel

        obj = MainModel(f1=SubModel(f1='hello'))
        assert obj.f1.f1 == 'hello'
        obj.f1 = SubModel(f1='hi')
        assert obj.f1.f1 == 'hi'

    def test_feed_with_embedded_model_obj_list(self):
        class SubModel(EmbeddedModel):
            f1: str

        class MainModel(BaseModel):
            f1: List[SubModel]

        obj = MainModel(f1=[SubModel(f1='a'), SubModel(f1='b'), SubModel(f1='c')])
        assert isinstance(obj.f1[0], SubModel)
        assert obj.f1[1].f1 == 'b'

        obj.f1 = [SubModel(f1='1'), SubModel(f1='2'), SubModel(f1='3')]
        assert obj.f1[1].f1 == '2'

    def test_feed_with_embedded_model_skip_validate(self):
        class SubModel(EmbeddedModel):
            f1: str

        class MainModel(BaseModel):
            f1: str
            f2: SubModel
            f3: List[SubModel]

        sub1 = SubModel(f1='foo')
        sub2 = SubModel(f1='bar')
        assert not hasattr(sub1.to_dict(), '_skip_validate')
        assert not hasattr(sub2.to_dict(), '_skip_validate')

        obj = MainModel(f1='foobar', f2=sub1)
        assert hasattr(sub1.to_dict(), '_skip_validate')

        obj.f3 = [sub2]
        assert hasattr(sub2.to_dict(), '_skip_validate')

    def test_pass_in_wrong_type(self):
        with pytest.raises(TypeError) as err:
            class Foo:
                pass

            class FooModel(BaseModel):
                f = EmbeddedField(Foo)
        assert 'EmbeddedModel' in err.value.args[0]


class TestArrayField:
    def test_basic_field(self):
        class MainModel(BaseModel):
            f1: List[int]

        obj = MainModel(f1=[1, 2, 3])
        assert obj.f1[2] == 3

        with pytest.raises(ValueError):
            MainModel(f1='abc')

        with pytest.raises(ValidationError):
            MainModel(f1=[1, 'a'])

    def test_nested_basic_field(self):
        class MainModel(BaseModel):
            f1: List[List[int]]

        obj = MainModel(f1=[[1, 2, 3], [4, 5, 6]])
        assert obj.f1[1][1] == 5
        with pytest.raises(IndexError):
            _ = obj.f1[1][5]

    def test_embedded_field(self):
        class SubSubModel(EmbeddedModel):
            f1: str
            f2: List[int]

        class SubModel(EmbeddedModel):
            f1: str
            f2: List[SubSubModel]

        class MainModel(BaseModel):
            f1: List[SubModel]
        obj = MainModel(f1=[
            {'f1': 'a', 'f2': [{'f1': 'b', 'f2': [1, 2, 3]}]},
            {'f1': 'c', 'f2': [{'f1': 'd', 'f2': [4, 5, 6]}]}
        ])
        assert obj.f1[1].f1 == 'c'
        assert obj.f1[1].f2[0].f1 == 'd'
        assert obj.f1[1].f2[0].f2[1] == 5

    def test_nested_embedded_field(self):
        class SubSubModel(EmbeddedModel):
            f1: str
            f2: List[int]

        class SubModel(EmbeddedModel):
            f1: str
            f2: List[SubSubModel]

        class MainModel(BaseModel):
            f1: List[List[SubModel]]

        obj = MainModel(f1=[
            [{'f1': 'a', 'f2': [{'f1': 'b', 'f2': [1, 2, 3]}]}],
            [{'f1': 'c', 'f2': [{'f1': 'd', 'f2': [4, 5, 6]}]}]
        ])

        assert obj.f1[1][0].f1 == 'c'
        assert obj.f1[1][0].f2[0].f2[1] == 5
        assert isinstance(obj.f1[1][0], SubModel)
        assert isinstance(obj.f1[1][0].f2[0], SubSubModel)

    def test_pass_in_wrong_type(self):
        with pytest.raises(TypeError) as err:
            class Foo:
                pass

            class FooModel(BaseModel):
                f = ArrayField(Foo)
        assert 'EmbeddedModel' in err.value.args[0]


def test_invalid_field_type():
    class Foo:
        pass
    with pytest.raises(TypeError) as err:
        class MainModel(BaseModel):
            x: Foo
    assert 'cannot convert' in err.value.args[0]


class TestFieldDefaultValue:
    def test_field_in_model(self):
        class MainModel(BaseModel):
            f1: int = 42
            f2: list = [1, 2, 'a']
            f3: dict = {'a': 1}
            f4: List[int] = [1, 3]

        obj = MainModel()
        assert obj.f1 == 42
        assert obj.f2 == [1, 2, 'a']
        assert obj.f3 == {'a': 1}
        assert obj.f4 == [1, 3]

    def test_field_in_embedded_model(self):
        class SubModel(EmbeddedModel):
            f1: int = 13
            f2: int

        class MainModel(BaseModel):
            f1: SubModel

        obj = MainModel(f1={'f2': 31})
        assert obj.f1.f1 == 13

    def test_field_in_array_embedded_model(self):
        class SubModel(EmbeddedModel):
            f1: int = 13
            f2: int

        class MainModel(BaseModel):
            f1: List[SubModel]

        obj = MainModel(f1=[{'f2': 31}])
        assert obj.f1[0].f1 == 13


def test_field_exist_in_meta():
    with pytest.raises(ValueError) as err:
        class MainModel(BaseModel):
            f: int

            class Meta:
                required = ['f1']
    assert 'not defined' in err.value.args[0]


class TestFieldAliases:
    def test_field_in_model(self):
        class MainModel(BaseModel):
            f1: int

            class Meta:
                aliases = [('f1', '1f')]

        obj = MainModel(f1=13)
        assert obj.f1 == 13
        assert obj.to_dict()['1f'] == 13

    def test_field_in_embedded_model(self):
        class SubModel(EmbeddedModel):
            f1: int

            class Meta:
                aliases = [('f1', '1f')]

        class MainModel(BaseModel):
            f1: int
            f2: SubModel

            class Meta:
                aliases = [('f1', '1f'), ('f2', '2f')]
        obj = MainModel(f2={'f1': 13})
        assert obj.f2.f1 == 13
        assert obj.to_dict()['2f'] == {'1f': 13}

    def test_field_in_array_embedded_model(self):
        class SubModel(EmbeddedModel):
            f1: int

            class Meta:
                aliases = [('f1', '1f')]

        class MainModel(BaseModel):
            f1: int
            f2: List[SubModel]

            class Meta:
                aliases = [('f1', '1f'), ('f2', '2f')]
        obj = MainModel(f2=[{'f1': 13}, {'f1': 42}])
        assert obj.f2[1].f1 == 42
        assert obj.to_dict()['2f'][1]['1f'] == 42


class TestFieldRequired:
    def test_field_in_model(self):
        class MainModel(BaseModel):
            f1: int
            f2: int

            class Meta:
                required = ['f1']

        with pytest.raises(ValidationError) as err:
            MainModel(f2=42)
        assert 'missing' in err.value.msg
        obj = MainModel(f1=13)
        assert obj.f1 == 13

    def test_field_in_embedded_model(self):
        class SubModel(EmbeddedModel):
            f1: int
            f2: int

            class Meta:
                required = ['f1']

        class MainModel(BaseModel):
            f1: SubModel

        MainModel()
        MainModel(f1={'f1': 13})
        with pytest.raises(ValidationError) as err:
            MainModel(f1={'f2': 42})
        assert 'missing' in err.value.msg

    def test_field_in_array_embedded_model(self):
        class SubModel(EmbeddedModel):
            f1: int
            f2: int

            class Meta:
                required = ['f1']

        class MainModel(BaseModel):
            f1: List[SubModel]
        MainModel(f1=[{'f1': 13}])
        with pytest.raises(ValidationError) as err:
            MainModel(f1=[{'f2': 42}])
        assert 'missing' in err.value.msg


def test_field_converters():
    class SubModel(EmbeddedModel):
        f: str

        class Meta:
            converters = {
                'f': lambda x: x.upper()
            }

    class MainModel(BaseModel):
        f1: str
        f2: SubModel
        f3: List[SubModel]

        class Meta:
            converters = {
                'f1': lambda x: x.upper()
            }

    obj1 = MainModel(f1='hello')
    assert obj1.f1 == 'HELLO'

    obj2 = MainModel(f2={'f': 'hello'})
    assert obj2.f2.f == 'HELLO'

    obj3 = MainModel(f3=[{'f': 'hello'}])
    assert obj3.f3[0].f == 'HELLO'


def test_field_validators():
    class SubModel(EmbeddedModel):
        f: str

        class Meta:
            validators = {
                'f': lambda x: len(x) > 3
            }

    class MainModel(BaseModel):
        f1: str
        f2: SubModel
        f3: List[SubModel]

        class Meta:
            validators = {
                'f1': lambda x: len(x) > 3
            }

    MainModel(f1='hello')

    with pytest.raises(ValidationError) as err:
        MainModel(f1='a')
    assert 'not accepted' in err.value.msg

    with pytest.raises(ValidationError):
        MainModel(f2={'f': 'a'})

    with pytest.raises(ValidationError):
        MainModel(f3=[{'f': 'a'}])


def test_model_iter():
    class SubModel(EmbeddedModel):
        f: int

    class MainModel(BaseModel):
        f1: int
        f2: SubModel
        f3: List[SubModel]

    obj = MainModel(f1=1, f2={'f': 2}, f3=[{'f': 3}, {'f': 4}])

    assert isinstance(obj, abc.Iterable)

    obj_iter = iter(obj)
    assert list(obj_iter) == ['f1', 'f2', 'f3']

    obj_iter1 = iter(obj.f2)
    assert list(obj_iter1) == ['f']


def test_model_to_dict():
    class SubModel(EmbeddedModel):
        f: int

    class MainModel(BaseModel):
        f1: int
        f2: SubModel
        f3: List[SubModel]

    obj1 = MainModel(f1=1, f2={'f': 2}, f3=[{'f': 3}])
    assert obj1.to_dict() == {'f1': 1, 'f2': {'f': 2}, 'f3': [{'f': 3}]}

    obj1 = MainModel(f1=1, f2=SubModel(f=2), f3=[SubModel(f=3)])
    assert obj1.to_dict() == {'f1': 1, 'f2': {'f': 2}, 'f3': [{'f': 3}]}


def test_model_to_json():
    class SubModel(EmbeddedModel):
        f: int
        f2: datetime = datetime.utcnow

    class MainModel(BaseModel):
        f1: int
        f2: SubModel
        f3: List[SubModel]
        f4: ObjectId = ObjectId

    obj = MainModel(f1=1, f2={'f': 2}, f3=[{'f': 3}])
    j_data = obj.to_json()
    assert isinstance(j_data, str)

    obj1 = MainModel(f1=1, f2=SubModel(f=2), f3=[SubModel(f=3)])
    j_data1 = obj1.to_json()
    assert isinstance(j_data1, str)


def test_model_get():
    class SubModel(EmbeddedModel):
        f: int

    class MainModel(BaseModel):
        f1: SubModel
        f2: List[SubModel]

    obj = MainModel(f1=SubModel(f=1), f2=[SubModel(f=2), SubModel(f=3)])
    assert obj.get('f1') == {'f': 1}
    assert obj.get('f2') == [{'f': 2}, {'f': 3}]
    assert obj.get('f3', 'foo') == 'foo'


def test_model_from_data_directly():
    class MainModel(BaseModel):
        f1: int
        f2: List[str]

    obj = MainModel._from_clean_data({'f1': [1, 2, 3], 'f2': 13})
    with pytest.raises(ValueError):
        _ = obj.f2


def test_model_property_setter():
    class SubModel(EmbeddedModel):
        name: str

        @property
        def foo(self):
            raise AttributeError('readonly')

        @foo.setter
        def foo(self, value):
            self.name = value

    class MainModel(BaseModel):
        name: str
        password_hash: str
        sub: SubModel

        @property
        def password(self):
            raise AttributeError('readonly')

        @password.setter
        def password(self, value):
            self.password_hash = value.upper()

    obj = MainModel(name='aaa', password='abc')
    assert obj.password_hash == 'ABC'

    obj = MainModel(sub=SubModel(foo='abc'))
    assert obj.sub.name == 'abc'

    obj = MainModel(sub={'foo': 'abc'})
    with pytest.raises(AttributeError):
        _ = obj.sub.name

    with pytest.raises(ValidationError):
        MainModel(sub=SubModel(foo=123))


class TestTrackFieldModify:
    def test_field_set(self):
        class SubModel(EmbeddedModel):
            f1: str

        class MainModel(BaseModel):
            f1: str
            f2: SubModel

        obj = MainModel(f1='foo', f2={'f1': 'bar'})
        obj.f1 = 'foo1'
        assert obj._combine_tracked_fields()[0] == {'f1'}

        obj.f2.f1 = 'bar1'
        assert obj._combine_tracked_fields()[0] == {'f1', 'f2.f1'}

        obj.f2 = {'f1': 'bar2'}
        assert obj._combine_tracked_fields()[0] == {'f1', 'f2'}

    def test_field_del(self):
        class SubModel(EmbeddedModel):
            f1: str

        class MainModel(BaseModel):
            f1: str
            f2: SubModel

        obj = MainModel(f1='foo', f2={'f1': 'bar'})
        del obj.f1
        assert obj._combine_tracked_fields()[1] == {'f1'}

        del obj.f2.f1
        assert obj._combine_tracked_fields()[1] == {'f1', 'f2.f1'}

        del obj.f2
        assert obj._combine_tracked_fields()[1] == {'f1', 'f2'}

    def test_field_set_del(self):
        class SubModel(EmbeddedModel):
            f1: str

        class MainModel(BaseModel):
            f1: str
            f2: SubModel

        obj = MainModel(f1='foo', f2={'f1': 'bar'})
        del obj.f1
        obj.f1 = 'foo1'
        assert obj._combine_tracked_fields()[0] == {'f1'}
        assert obj._combine_tracked_fields()[1] == set()

        obj = MainModel(f1='foo', f2={'f1': 'bar'})
        obj.f2.f1 = 'bar1'
        del obj.f2
        assert obj._combine_tracked_fields()[0] == set()
        assert obj._combine_tracked_fields()[1] == {'f2'}

    def test_field_clear_tracked_fields(self):
        class SubModel(EmbeddedModel):
            f1: str

        class MainModel(BaseModel):
            f1: str
            f2: SubModel

        obj = MainModel(f1='foo', f2={'f1': 'bar'})
        obj.f1 = 'foo1'
        obj.f2.f1 = 'bar1'
        obj._clear_tracked_fields()

        assert obj._combine_tracked_fields()[0] == set()
        assert obj._combine_tracked_fields()[1] == set()


class TestModelDictClass:
    def test_dict_type(self):
        class SubModel(EmbeddedModel):
            b: int = 1
            c: int = 2
            a: int = 3

        class MainModel(BaseModel):
            b: int = 1
            c: int = 2
            a: int = 3
            d: SubModel = {}
            e: List[SubModel] = [{}, {}]

        obj = MainModel()
        assert list(obj.to_dict().keys()) == ['b', 'c', 'a', 'd', 'e']
        assert list(obj.d.to_dict().keys()) == ['b', 'c', 'a']
        assert list(obj.e[0].to_dict().keys()) == ['b', 'c', 'a']

        assert type(obj.to_dict()) == OrderedDict
        assert type(obj.d.to_dict()) == OrderedDict
        assert type(obj.e[0].to_dict()) == OrderedDict

    def test_dict_type_with_son(self):
        class SubModel(EmbeddedModel):
            b: int = 1
            c: int = 2
            a: int = 3

        class MainModel(BaseModel):
            b: int = 1
            c: int = 2
            a: int = 3
            d: SubModel = {}
            e: List[SubModel] = [{}, {}]

        MainModel.dict_class = SON
        SubModel.dict_class = SON
        obj = MainModel()
        assert type(obj.to_dict()) == SON
        assert type(obj.d.to_dict()) == SON
        assert type(obj.e[0].to_dict()) == SON


def test_model_extra_data_warning(caplog):
    class SubModel(EmbeddedModel):
        f: int

    class MainModel(BaseModel):
        f1: int
        f2: SubModel
        f3: List[SubModel]

    MainModel(f4=13)
    MainModel(f2={'f1': 13})
    MainModel(f3=[{'f1': 13}])

    assert len(caplog.records) == 3

    for record in caplog.records:
        assert record.levelname == 'WARNING'
        assert 'not defined' in record.message


def test_model_disable_extra_data_warning(caplog):
    class MainModel(BaseModel):
        f: int

    MainModel.warn_extra_data = False
    MainModel(f=13, f1=31)

    assert len(caplog.records) == 0


def test_model_mix_type_hint_with_django_style_warning(caplog):
    class MainModel(BaseModel):
        f1: str
        f2 = StringField()

    for record in caplog.records:
        assert record.levelname == 'WARNING'
        assert 'django' in record.message


if __name__ == '__main__':
    pytest.main()
