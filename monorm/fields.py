from collections import abc
from datetime import datetime
from typing import Any, Callable, MutableMapping, MutableSequence, Union, Optional, Dict

from bson.objectid import ObjectId

from .utils import *

__all__ = [
    'Field',
    'StringField',
    'IntField',
    'FloatField',
    'BooleanField',
    'BytesField',
    'DateTimeField',
    'ObjectIdField',
    'DictField',
    'EmbeddedField',
    'ListField',
    'ArrayField',
    'AnyField',
    'ValidationError'
]


class ValidationError(Exception):
    def __init__(self, msg):
        self.msg = msg
        super().__init__(msg)


def validate_type(value: Any, expected_types: tuple) -> None:
    if not isinstance(value, expected_types):
        if len(expected_types) == 1:
            err = 'must be type {!r}'.format(expected_types[0])
        else:
            err = 'must be one of types {!r}'.format(expected_types)
        raise ValidationError('{!r} {}, not a {!r}.'.format(value, err, type(value)))


def validate_max_value(value: Any, max_value: Union[int, float]) -> None:
    if value > max_value:
        raise ValidationError('{!r} is greater than the max value {}.'.format(value, max_value))


def validate_min_value(value: Any, min_value: Union[int, float]) -> None:
    if value < min_value:
        raise ValidationError('{!r} is less than the min value {}.'.format(value, min_value))


def validate_max_length(value: Any, max_length: int) -> None:
    if len(value) > max_length:
        raise ValidationError('Length of {!r} is greater than the max value {}.'.format(value, max_length))


def validate_min_length(value: Any, min_length: int) -> None:
    if len(value) < min_length:
        raise ValidationError('Length of {!r} is less than the min value {}.'.format(value, min_length))


def validate_fn(value: Any, validator: Callable[[Any], bool]) -> None:
    if not validator(value):
        raise ValidationError('{!r} was not accepted by validator {!r}.'.format(value, validator))


class Field:
    expected_types = ()

    def __init__(
        self,
        name: str = None,
        required: bool = False,
        default: Any = None,
        converter: Callable = None,
        validator: Callable[[Any], bool] = None,
    ):
        self.name = name
        self.required = required
        self.default = default
        self.converter = converter
        self.validator = validator

    def convert(self, value: Any) -> Any:
        if value is None:
            if callable(self.default):
                value = self.default()
            else:
                value = self.default
        if value is not None and self.converter is not None:
            value = self.converter(value)
        return value

    def validate(self, value: Any) -> None:
        if value is None:
            if self.required:
                raise ValidationError('Field {!r} is missing.'.format(self.name))
        else:
            validate_type(value, self.expected_types)
            if self.validator:
                validate_fn(value, self.validator)

    def __get__(self, instance, cls) -> Any:
        if instance is None:
            return self

        try:
            value = instance.__dict__['_data'][self.name]
        except KeyError:
            raise AttributeError('Field {!r} has no value; '
                                 'did you filter it out using projection query?'.format(self.name)) from None

        if isinstance(self, EmbeddedField):
            return self.model.from_data_unchanged(value)
        elif isinstance(self, ArrayField):
            return self._walk_list(value)
        else:
            return value

    def __set__(self, instance, value):
        value = self.convert(value)
        self.validate(value)

        if value is not None:
            instance.__dict__['_data'][self.name] = value

    def __delete__(self, instance):
        instance.__dict__['_data'].pop(self.name, None)


class StringField(Field):
    expected_types = (str,)

    def __init__(self, max_length: int = None, min_length: int = None, **kw):
        super().__init__(**kw)
        self.max_length = max_length
        self.min_length = min_length

    def validate(self, value: Any) -> None:
        super().validate(value)
        if value is not None:
            if self.max_length is not None:
                validate_max_length(value, self.max_length)
            if self.min_length is not None:
                validate_min_length(value, self.min_length)


class NumberField(Field):
    expected_types = (int, float)

    def __init__(self, max_value: Union[int, float] = None, min_value: Union[int, float] = None, **kw):
        super().__init__(**kw)
        self.max_value = max_value
        self.min_value = min_value

    def validate(self, value: Any) -> None:
        super().validate(value)
        if value is not None:
            if self.max_value is not None:
                validate_max_value(value, self.max_value)
            if self.min_value is not None:
                validate_min_value(value, self.min_value)


class IntField(NumberField):
    expected_types = (int,)


class FloatField(NumberField):
    expected_types = (float,)


class BooleanField(Field):
    expected_types = (bool,)


class BytesField(Field):
    expected_types = (bytes,)


class DateTimeField(Field):
    expected_types = (datetime,)


class ObjectIdField(Field):
    expected_types = (ObjectId,)


class ListField(Field):
    expected_types = (abc.MutableSequence, tuple)

    def __init__(self, min_length: int = None, max_length: int = None, **kw):
        super().__init__(**kw)
        self.min_length = min_length
        self.max_length = max_length

    def validate(self, value: Any) -> None:
        super().validate(value)
        if value is not None:
            if self.max_length is not None:
                validate_max_length(value, self.max_length)
            if self.min_length is not None:
                validate_min_length(value, self.min_length)


class ArrayField(ListField):
    def __init__(self, field, **kw):
        from .model import EmbeddedModel
        super().__init__(**kw)

        if isinstance(field, Field):
            self.field = field
        elif isclass(field) and issubclass(field, EmbeddedModel):
            self.field = EmbeddedField(field)
        else:
            raise TypeError('`ArrayField` can only accept instance of {!r} or '
                            'subclass of `EmbeddedModel`; not a {!r}.'
                            .format(Field, EmbeddedModel, field))

    def convert(self, values: Any) -> Optional[MutableSequence]:
        values = super().convert(values)
        if values is None:
            return

        if not isinstance(values, (abc.MutableSequence, tuple)):
            raise ValueError('{!r} must be a list-like object, not a {!r}.'.format(values, type(values)))

        return [self.field.convert(value) for value in values]

    def validate(self, values: Optional[MutableSequence]) -> None:
        super().validate(values)
        if values is None:
            return

        for value in values:
            self.field.validate(value)

    def innermost(self) -> Field:
        def inner(array_field: ArrayField):
            field = array_field.field
            if not isinstance(field, ArrayField):
                return field
            else:
                return inner(field)
        return inner(self)

    def _walk_list(self, values: MutableSequence) -> MutableSequence:
        def walk(array_field: ArrayField, vals: MutableSequence):
            if not isinstance(vals, abc.MutableSequence):
                raise ValueError('{!r} must be a list-like object, not a {!r}.'.format(vals, type(vals)))

            if not isinstance(self.innermost(), EmbeddedField):
                return vals

            field = array_field.field

            if isinstance(field, EmbeddedField):
                cls = field.model
                return [cls.from_data_unchanged(value) for value in vals]

            if isinstance(field, ArrayField):
                return [walk(field, value) for value in vals]

        return walk(self, values)


class DictField(Field):
    expected_types = (abc.MutableMapping,)


class EmbeddedField(DictField):
    def __init__(self, embedded_model=None, **kw):
        from .model import EmbeddedModel
        super().__init__(**kw)
        if embedded_model is None:
            return

        if not isclass(embedded_model) or not issubclass(embedded_model, EmbeddedModel):
            raise TypeError('`EmbeddedField` can only accept subclass '
                            'of `EmbeddedModel`, not {!r}.'.format(embedded_model))
        self.model = embedded_model

    def init_root(self, model):
        self.model = model
        return self

    @property
    def fields(self) -> Dict[str, Field]:
        # dict preserves insertion order from Python 3.6
        # https://mail.python.org/pipermail/python-dev/2017-December/151283.html
        field_names = self.model.__dict__['_field_order']
        return {name: getattr(self.model, name) for name in field_names}

    def convert(self, obj: Any) -> Optional[MutableMapping]:
        if isinstance(obj, self.model):
            # noinspection PyAttributeOutsideInit
            # we can safely skip `convert` and 'validate` because it must have been done before.
            self._skip_validate = True
            return obj.to_dict()

        obj = super().convert(obj)
        if obj is None:
            return

        if not isinstance(obj, MutableMapping):
            raise ValueError('{!r} must be a dict-like object, not a {!r}.'.format(obj, type(obj)))

        fields = self.fields
        rv = self.model.dict_class()

        for name, field in fields.items():
            value = field.convert(obj.get(name))
            if value is not None:
                rv[field.name] = value

        for name, value in obj.items():
            if name not in fields:
                rv[name] = value

        return rv

    def validate(self, obj: Optional[MutableMapping]) -> None:
        if hasattr(self, '_skip_validate'):
            return

        super().validate(obj)
        if obj is None:
            return

        fields = self.fields
        for name, field in fields.items():
            field.validate(obj.get(name))

        if self.model.warn_extra_data:
            names = [field.name for field in fields.values()]
            for key, value in obj.items():
                if key not in names:
                    warn('{!r} not defined in {!r}. Did you misspell it?'.format(key, self.model))


class AnyField(Field):
    expected_types = (object,)
