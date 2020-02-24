from collections import OrderedDict
from datetime import datetime
from typing import get_type_hints, Any, MutableMapping, Type, Union, Callable, List, Iterable, Optional, Set, Tuple

from bson.json_util import dumps
from bson.objectid import ObjectId

from .fields import *
from .utils import *

__all__ = [
    'BaseModel',
    'EmbeddedModel'
]

hint_field_map = {
    str: StringField,
    int: IntField,
    float: FloatField,
    bool: BooleanField,
    bytes: BytesField,
    dict: DictField,
    list: ListField,
    datetime: DateTimeField,
    ObjectId: ObjectIdField,
    Any: AnyField,
}


def _hint_to_field(hint_type: Union[Type, Any]) -> Field:
    if hint_type in hint_field_map:
        return hint_field_map[hint_type]()
    if isclass(hint_type) and issubclass(hint_type, EmbeddedModel):
        return EmbeddedField(hint_type)
    # In Python 3.6 it's `typing.List` while 'list' in 3.7
    if hint_type.__dict__.get('__origin__') in (list, List):
        # noinspection PyUnresolvedReferences
        arg = hint_type.__args__[0]
        if str(arg) == '~T':
            return ListField()
        else:
            return ArrayField(_hint_to_field(arg))
    raise TypeError('cannot convert {!r} to a field'.format(hint_type))


class ModelType(type):
    def __new__(mcs, name, bases, attrs):
        if '_no_parse_hints' in attrs:
            return super().__new__(mcs, name, bases, attrs)

        # track the field definition order
        field_order = []

        for key, value in attrs.items():
            if isinstance(value, Field):
                if value.name is None:
                    value.name = key
                field_order.append(key)

        attrs['_field_order'] = field_order

        return super().__new__(mcs, name, bases, attrs)

    def _parse_type_hints(cls) -> None:
        annotations = cls.__dict__.get('__annotations__')
        if annotations is None:
            return

        types = get_type_hints(cls)
        field_order = cls.__dict__['_field_order']

        if len(field_order) != 0:
            warn('You are mixing type-hint-style with django-orm-style in {!r}; '
                 'the field definition order may not be reserved.'.format(cls))

        for name, hint in types.items():
            # filter out type hints of parent classes
            if name not in annotations:
                continue

            field = _hint_to_field(hint)
            field.name = name
            field_order.append(name)

            if cls.__dict__.get(name) is not None:
                field.default = getattr(cls, name)

            setattr(cls, name, field)

    def _process_meta(cls) -> None:
        fields = {key: value for key, value in cls.__dict__.items() if isinstance(value, Field)}
        meta = cls.__dict__.get('Meta')
        aliases = getattr(meta, 'aliases', [])
        required = getattr(meta, 'required', [])
        converters = getattr(meta, 'converters', {})
        validators = getattr(meta, 'validators', {})

        def ensure_field_exist(name):
            if name not in fields:
                raise ValueError('{!r} is not defined in {!r}.'.format(name, cls))

        if isinstance(aliases, dict):
            aliases = [(key, value) for key, value in aliases.items()]

        if len(aliases) != len({alias[1] for alias in aliases}):
            raise ValueError('Duplicated alias found in {!r}.'.format(cls))

        for field_name, alias in aliases:
            ensure_field_exist(field_name)
            fields[field_name].name = alias

        for field_name in required:
            ensure_field_exist(field_name)
            fields[field_name].required = True

        for field_name, converter in converters.items():
            ensure_field_exist(field_name)
            fields[field_name].converter = converter

        for field_name, validator in validators.items():
            ensure_field_exist(field_name)
            fields[field_name].validator = validator

    def __init__(cls, name, bases, attrs):
        if '_no_parse_hints' not in cls.__dict__:
            cls._parse_type_hints()
            cls._process_meta()

        super().__init__(name, bases, attrs)

    @classmethod
    def __prepare__(mcs, name, bases) -> MutableMapping:
        # class attribute definition order is preserved from python 3.6,
        # but cls.__dict__ will be updated by `setattr` in :meth:`~ModelType._parse_type_hints`;
        # to avoid bugs or dependencies on implementation details, here we return an `OrderedDict`.
        # https://www.python.org/dev/peps/pep-0520/
        return OrderedDict()


class BaseModel(metaclass=ModelType):
    """Base class of all model classes"""

    # The underlying data of models are saved in a ordered dict-like object.
    # You can change it to `collections.OrderedDict`, `bson.son.SON` or other compatible types.
    dict_class: Type[MutableMapping] = OrderedDict

    # `json.dumps` cannot dump some values of bson object (objectId, datetime, etc.);
    # you can also use your own dump-function.
    json_dumps_func: Callable = dumps

    # Whether checks extra data that aren't declared in the model and emits some warnings.
    warn_extra_data: bool = True

    _no_parse_hints: bool = True
    __no_type_check__: bool = False

    def __new__(cls, _dirty=True, **kw):
        if _dirty:
            return cls._from_dirty_data(kw)
        else:
            instance = super().__new__(cls)
            instance._data = None
            return instance

    def __init__(self, **kw):
        self._modified_fields: Optional[Set[str]] = None
        self._deleted_fields: Optional[Set[str]] = None

    # noinspection PyCallByClass
    def to_json(self, *arg, **kw) -> str:
        """Return a json string. Some specific types (`ObjectId`, `datetime`, etc.) will be handled correctly."""
        return type(self).json_dumps_func(self.to_dict(), *arg, **kw)

    def to_dict(self) -> MutableMapping:
        """Return an ordered dict containing the instance's data with the same order as the field definition order."""
        return self._data

    def get(self, name: str, default=None) -> Any:
        """Return the value for name if it has a value, else default."""
        try:
            return self._data[name]
        except KeyError:
            return default

    @classmethod
    def _from_clean_data(cls, data: MutableMapping):
        instance = cls(_dirty=False)
        instance._data = data
        return instance

    @classmethod
    def _from_dirty_data(cls, data: MutableMapping):
        root = EmbeddedField().init_root(cls)
        data = root.convert(data)
        instance = cls._from_clean_data(data)

        for key, value in cls.__dict__.items():
            if key in data and isinstance(value, property) and value.fset:
                setattr(instance, key, data[key])
                data.pop(key)

        root.validate(data)
        return instance

    @classmethod
    def _get_clean_data(cls, data: MutableMapping, bypass_validation: bool = False) -> MutableMapping:
        root = EmbeddedField().init_root(cls)
        data = root.convert(data)
        if not bypass_validation:
            root.validate(data)
        return data

    def _init_tracked_fields(self) -> None:
        if self._modified_fields is None:
            self._modified_fields = set()
        if self._deleted_fields is None:
            self._deleted_fields = set()

    def _clear_tracked_fields(self) -> None:
        if self._modified_fields:
            self._modified_fields.clear()
        if self._modified_fields:
            self._deleted_fields.clear()

        for value in self.__dict__.values():
            if isinstance(value, EmbeddedModel):
                value._clear_tracked_fields()

    def _combine_tracked_fields(self) -> Tuple[Set[str], Set[str]]:
        modified = set()
        deleted = set()

        def combine(instance, prev, attr_name, result):
            fields = getattr(instance, attr_name)
            if fields is None:
                fields = set()

            for name in fields:
                result.add(prev + name)

            for key, value in instance.__dict__.items():
                if isinstance(value, EmbeddedModel) and key not in fields:
                    combine(value, prev + key + '.', attr_name, result)

        combine(self, '', '_modified_fields', modified)
        combine(self, '', '_deleted_fields', deleted)
        return modified, deleted

    def __setattr__(self, name, value):
        fields = type(self).__dict__['_field_order']

        if name in fields:
            self._init_tracked_fields()
            self._modified_fields.add(name)
            self._deleted_fields.discard(name)
        return super().__setattr__(name, value)

    def __delattr__(self, name):
        fields = type(self).__dict__['_field_order']

        if name in fields:
            self._init_tracked_fields()
            self._deleted_fields.add(name)
            self._modified_fields.discard(name)
        return super().__delattr__(name)

    def __iter__(self) -> Iterable[str]:
        return iter(self._data)


class EmbeddedModel(BaseModel):
    """Base class of user-defined embedded model"""
    _no_parse_hints: bool = True
