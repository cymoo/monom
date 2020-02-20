from typing import Optional, Any, Union, List, Iterable, MutableMapping, TypeVar, Type

from pymongo.collation import Collation
from pymongo.collection import Collection
from pymongo.collection import ReturnDocument
from pymongo.command_cursor import CommandCursor
from pymongo.cursor import Cursor as PymongoCursor
from pymongo.database import Database
from pymongo.results import InsertOneResult, InsertManyResult, UpdateResult, DeleteResult

from .fields import *
from .model import BaseModel, ModelType
from .utils import pluralize, info, normalize_indexes, default_index_name, have_same_shape, \
    not_none, warn, get_dict_item_with_dot

__all__ = [
    'MongoModel',
]


T = TypeVar('T', bound=BaseModel)


class Cursor(PymongoCursor):
    def __init__(self, model_cls: Type[T], *args, **kw):
        super().__init__(*args, **kw)
        self.model_cls = model_cls

    def __next__(self) -> T:
        rv = super().__next__()
        return self.model_cls.from_document(rv)


# noinspection PyShadowingBuiltins,PyMethodParameters
class CollectionMixin(type):
    """Proxy frequently-used methods of :class:`pymongo:collection:Collection`.
    """

    #################################
    # Insertion
    #################################

    def insert_one(cls: Type[T],
                   document: MutableMapping,
                   bypass_document_validation: bool = False,
                   session=None) -> InsertOneResult:
        doc = cls._get_clean_data(document, bypass_validation=bypass_document_validation)
        return cls.get_collection().insert_one(
            doc, bypass_document_validation=bypass_document_validation, session=session
        )

    def insert_many(cls: Type[T],
                    documents: Iterable[MutableMapping],
                    ordered: bool = True,
                    bypass_document_validation: bool = False,
                    session=None) -> InsertManyResult:
        docs = [cls._get_clean_data(document, bypass_validation=bypass_document_validation) for document in documents]
        return cls.get_collection().insert_many(
            docs, ordered=ordered, bypass_document_validation=bypass_document_validation, session=session
        )

    #################################
    # Query
    #################################

    def find_one(cls: Type[T], filter: dict = None, *args, **kw) -> Optional[T]:
        result = cls.get_collection().find_one(filter, *args, **kw)
        if result is not None:
            return cls.from_document(result)

    def find(cls: Type[T], *args, **kw) -> Union[Cursor, Iterable[T]]:
        return Cursor(cls, cls.get_collection(), *args, **kw)

    #################################
    # Deletion
    #################################

    def delete_one(cls: Type[T], filter: dict, collation: Collation = None, session=None) -> DeleteResult:
        return cls.get_collection().delete_one(filter, collation=collation, session=session)

    def delete_many(cls: Type[T], filter: dict, collation: Collation = None, session=None) -> DeleteResult:
        return cls.get_collection().delete_many(filter, collation=collation, session=session)

    #################################
    # Update
    #################################

    def replace_one(cls: Type[T],
                    filter: dict,
                    replacement: MutableMapping,
                    upsert: bool = False,
                    bypass_document_validation: bool = False,
                    collation: Collation = None,
                    session=None) -> UpdateResult:
        doc = cls._get_clean_data(replacement, bypass_validation=bypass_document_validation)
        return cls.get_collection().replace_one(
            filter, doc, upsert=upsert, bypass_document_validation=bypass_document_validation,
            collation=collation, session=session
        )

    def update_one(cls: Type[T],
                   filter: dict,
                   update: MutableMapping,
                   upsert: bool = False,
                   bypass_document_validation: bool = False,
                   collation: Collation = None,
                   array_filters: List[dict] = None,
                   session=None) -> UpdateResult:
        update = cls._get_clean_update(update, bypass_document_validation)
        return cls.get_collection().update_one(
            filter, update, upsert=upsert, bypass_document_validation=bypass_document_validation,
            collation=collation, array_filters=array_filters, session=session
        )

    def update_many(cls: Type[T],
                    filter: dict,
                    update: MutableMapping,
                    upsert: bool = False,
                    array_filters: List[dict] = None,
                    bypass_document_validation: bool = False,
                    collation: Collation = None,
                    session=None) -> UpdateResult:
        update = cls._get_clean_update(update, bypass_document_validation)
        return cls.get_collection().update_many(
            filter, update, upsert=upsert, array_filters=array_filters,
            bypass_document_validation=bypass_document_validation, collation=collation, session=session
        )

    #################################
    # FindAndXXX
    #################################

    def find_one_and_delete(cls: Type[T],
                            filter: dict,
                            projection: Union[list, dict] = None,
                            sort: List[tuple] = None,
                            session=None, **kw) -> Optional[T]:

        result = cls.get_collection().find_one_and_delete(
            filter, projection=projection, sort=sort, session=session, **kw
        )
        if result is not None:
            return cls.from_document(result)

    def find_one_and_replace(cls: Type[T],
                             filter: dict,
                             replacement: MutableMapping,
                             bypass_document_validation: bool = False,  # extra argument
                             projection: Union[list, dict] = None,
                             sort: List[tuple] = None,
                             upsert: bool = False,
                             return_document: bool = ReturnDocument.BEFORE,
                             session=None, **kw) -> Optional[T]:
        doc = cls._get_clean_data(replacement, bypass_validation=bypass_document_validation)
        result = cls.get_collection().find_one_and_replace(
            filter, doc, projection=projection, sort=sort, upsert=upsert, return_document=return_document,
            session=session, **kw
        )
        if result is not None:
            return cls.from_document(result)

    def find_one_and_update(cls: Type[T],
                            filter: dict,
                            update: dict,
                            bypass_document_validation: bool = False,  # extra argument
                            projection: Union[list, dict] = None,
                            sort: List[tuple] = None,
                            upsert: bool = False,
                            return_document: bool = ReturnDocument.BEFORE,
                            array_filters: List[dict] = None,
                            session=None, **kw) -> Optional[T]:
        update = cls._get_clean_update(update, bypass_document_validation)
        result = cls.get_collection().find_one_and_update(
            filter, update, projection=projection, sort=sort, upsert=upsert, return_document=return_document,
            array_filters=array_filters, session=session, **kw
        )
        if result is not None:
            return cls.from_document(result)

    #################################
    # Aggregation
    #################################

    def aggregate(cls: Type[T], pipeline: List[dict], session=None, **kw) -> CommandCursor:
        return cls.get_collection().aggregate(pipeline, session, **kw)

    def estimated_document_count(cls: Type[T], **kw) -> int:
        return cls.get_collection().estimated_document_count(**kw)

    def count_documents(cls: Type[T], filter: dict, session=None, **kw) -> int:
        return cls.get_collection().count_documents(filter, session=session, **kw)

    def distinct(cls: Type[T], key: str, filter: dict = None, session=None, **kw) -> list:
        return cls.get_collection().distinct(key, filter=filter, session=session, **kw)


class MongoModelType(ModelType, CollectionMixin):
    """Base class of `MongoModel`."""


class MongoModel(BaseModel, metaclass=MongoModelType):
    # Automatic index creation or deletion can be disabled by setting this flag to false.
    # Index creation may be performed as part of a deployment system when in production
    auto_build_index: bool = True

    _db: Database = None
    _collection: Collection = None

    _no_parse_hints: bool = True

    def __init__(self, **kw):
        super().__init__(**kw)
        self._state = 'before_save'

    @property
    def pk(self) -> Optional[Any]:
        """An alias for the primary key (`_id` in MongoDB)."""
        return self._data.get('_id', None)

    def save(self, full_update: bool = False, **kw):
        """Save the document into MongoDB.
        1. The new document will be inserted into MongoDB.
        2. The existing document will be updated atomically using operator '$set' and '$unset'.
        3. `list` mutation cannot be tracked; but you can pass an keyword argument `full_update=True`
            to perform a full update.

        :return This object with the `pk` property filled if it wasn't already.
        """

        state = self._state
        collection = type(self).get_collection()

        if state == 'before_save':
            collection.insert_one(self.to_dict(), **kw)
            self._clear_tracked_fields()
            self._state = 'after_save'
        elif state in ('after_save', 'from_document'):
            doc = self.to_dict()
            if self.pk is None:
                raise RuntimeError("The document without an '_id' cannot be saved.")

            if full_update:
                collection.update_one({'_id': self.pk}, {'$set': doc}, **kw)
            else:
                modified, deleted = self._combine_tracked_fields()
                update = {}
                if modified:
                    update['$set'] = {field: get_dict_item_with_dot(doc, field) for field in modified}
                if deleted:
                    update['$unset'] = {field: '' for field in deleted}
                collection.update_one({'_id': self.pk}, update, **kw)
            self._clear_tracked_fields()
        elif state == 'deleted':
            raise RuntimeError('The document has been deleted.')

        return self

    def delete(self, **kw) -> None:
        """Delete the document from MongoDB"""

        state = self._state
        collection = type(self).get_collection()

        if state == 'before_save':
            raise RuntimeError('You cannot delete a document that was not saved.')
        if self.pk is None:
            raise RuntimeError("The document without an '_id' cannot be deleted.")

        collection.delete_one({'_id': self.pk}, **kw)
        self._state = 'deleted'
        self._clear_tracked_fields()

    @classmethod
    def from_document(cls, doc: MutableMapping):
        """Construct an instance of this class from the given document."""
        obj = cls._from_clean_data(doc)
        obj._state = 'from_document'
        return obj

    @classmethod
    def get_db(cls) -> Database:
        """Return :class:`pymongo.database.Database`."""
        return cls._db

    @classmethod
    def set_db(cls, db: Database) -> None:
        cls._db = db

    @classmethod
    def get_collection(cls) -> Collection:
        """Return :class:`pymongo.collection.Collection`."""
        if not cls.__dict__.get('_collection'):
            collection = cls.get_db().get_collection(pluralize(cls.__name__.lower()))
            cls.set_collection(collection)
        return cls._collection

    @classmethod
    def set_collection(cls, collection: Union[str, Collection], **options) -> None:
        if isinstance(collection, Collection):
            cls._collection = collection
        elif isinstance(collection, str):
            cls._collection = cls.get_db().get_collection(collection, **options)
        else:
            raise ValueError('expect a string or a {!r}, not type {!r}'.format(Collection, type(collection)))

        if cls.auto_build_index:
            info('You may disable automatic index modification when in production.')
            cls._build_indexes()

    @classmethod
    def _get_clean_update(cls, update: MutableMapping, bypass_validation: bool = False) -> MutableMapping:
        # From MongoDB 4.2, argument `update` can be an aggregation pipeline.
        if not isinstance(update, MutableMapping):
            return update

        # noinspection PyShadowingNames
        def raise_invalid_type_error(field: Field, op: str) -> None:
            raise ValidationError('not expect field type {!r} with {!r}'.format(type(field), op))

        # noinspection PyShadowingNames
        def check_dot_notation(op: str, doc: MutableMapping, field_type: Optional[Type[Field]] = None) -> None:
            for notation in doc.keys():
                field = cls._parse_dot_notation(notation)
                if field_type is not None:
                    if not isinstance(field, field_type):
                        raise_invalid_type_error(field, op)

        for op, doc in update.items():
            if op == '$set':
                for notation, value in doc.items():
                    field = cls._parse_dot_notation(notation)
                    new_value = field.convert(value)
                    if not bypass_validation:
                        field.validate(new_value)
                    doc[notation] = new_value

            elif op in ('$push', '$addToSet'):
                for notation, value in doc.items():
                    field = cls._parse_dot_notation(notation)
                    if not isinstance(field, ListField):
                        raise_invalid_type_error(field, op)
                    if isinstance(field, ArrayField):
                        item_field = field.field
                        if isinstance(value, MutableMapping) and '$each' in value:
                            values = value['$each']
                            new_values = [item_field.convert(item) for item in values]
                            if not bypass_validation:
                                for new_value in new_values:
                                    item_field.validate(new_value)
                            value['$each'] = new_values
                        else:
                            new_value = item_field.convert(value)
                            if not bypass_validation:
                                item_field.validate(new_value)
                            doc[notation] = new_value

            # check dot notation and give warnings when necessary
            elif op in ('$pop', '$pull', '$pullAll'):
                check_dot_notation(op, doc, ListField)
            elif op in ('$inc', '$mul'):
                check_dot_notation(op, doc, NumberField)
            elif op == '$currentDate':
                check_dot_notation(op, doc, DateTimeField)
            elif op in ('$min', '$max', '$rename', '$unset'):
                check_dot_notation(op, doc)
        return update

    @staticmethod
    def _is_array_placeholder(name: str):
        return name == '$' or name.startswith('$[') and name.endswith(']')

    @classmethod
    def _parse_dot_notation(cls, name: str) -> Field:
        def raise_parse_error(k: str, fld: Field):
            raise ValueError('cannot parse {!r}; not expect {!r} after {!r}'.format(name, k, fld))

        field = EmbeddedField().init_root(cls)
        for key in name.split('.'):
            if isinstance(field, AnyField):
                return AnyField()

            if type(field) == DictField:
                if key.isidentifier():
                    return AnyField()
                else:
                    raise_parse_error(key, field)

            if type(field) == ListField:
                if key.isdigit() or cls._is_array_placeholder(key):
                    return AnyField()
                else:
                    raise_parse_error(key, field)

            if key.isidentifier():
                if isinstance(field, EmbeddedField):
                    fields = field.fields
                    if key in fields:
                        field = fields[key]
                    else:
                        warn('{!r} not defined in model {!r}. Did you misspell it?'.format(key, field.model))
                        return AnyField()
                else:
                    raise_parse_error(key, field)
            elif key.isdigit() or cls._is_array_placeholder(key):
                if isinstance(field, ArrayField):
                    field = field.field
                else:
                    raise_parse_error(key, field)
            else:
                raise ValueError('cannot parse {!r}; not a valid identifier {!r}'.format(name, key))
        return field

    @classmethod
    def _build_indexes(cls) -> None:
        collection = cls.get_collection()
        indexes = normalize_indexes(getattr(cls.__dict__.get('Meta'), 'indexes', []))

        old = {default_index_name(index['key']): index for index in collection.list_indexes()}
        new = {default_index_name(index['key']): index for index in indexes}

        existing = [(new[name], old[name]) for name in set(new) & set(old)]
        missing = [new[name] for name in set(new) - set(old)]
        extra = [old[name] for name in set(old) - set(new)]

        # create new indexes
        for index in missing:
            key = index.pop('key')
            collection.create_index(key, **index)

        # drop old indexes
        for index in extra:
            name = index['name']
            if name != '_id_':
                collection.drop_index(name)

        # modify existing indexes
        # 1. drop and recreate the index, or
        # 2. modify via the `collMod` command (TTL indexes only)
        for new_index, old_index in existing:
            key = new_index.pop('key')
            new_index.setdefault('name', default_index_name(key))

            old_index.pop('key')
            old_index.pop('v', None)
            old_index.pop('ns', None)

            new_ttl = new_index.pop('expireAfterSeconds', None)
            old_ttl = old_index.pop('expireAfterSeconds', None)

            same_index_option = have_same_shape(new_index, old_index)

            if same_index_option and new_ttl == old_ttl:
                continue
            elif same_index_option and not_none([new_ttl, old_ttl]):
                # use collMod to modify the index
                cls.get_db().command({
                    'collMod': collection.name,
                    'index': {'name': new_index['name'], 'expireAfterSeconds': new_ttl}
                })
            else:
                # drop and recreate the index
                if new_ttl is not None:
                    new_index['expireAfterSeconds'] = new_ttl
                collection.drop_index(old_index['name'])
                collection.create_index(key, **new_index)
