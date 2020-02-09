from typing import Optional, Any, Union, List, Iterable, MutableMapping, TypeVar, Type

from pymongo.collation import Collation
from pymongo.collection import Collection
from pymongo.collection import ReturnDocument
from pymongo.command_cursor import CommandCursor
from pymongo.cursor import Cursor as PymongoCursor
from pymongo.database import Database
from pymongo.results import InsertOneResult, InsertManyResult, UpdateResult, DeleteResult

from .model import BaseModel
from .utils import pluralize, info, normalize_indexes, default_index_name, have_same_shape, not_none

__all__ = [
    'Model',
]


T = TypeVar('T', bound=BaseModel)


class Cursor(PymongoCursor):
    def __init__(self, model_cls: Type[T], *args, **kw):
        super().__init__(*args, **kw)
        self.model_cls = model_cls

    def __next__(self) -> T:
        rv = super().__next__()
        return self.model_cls.from_document(rv)


# noinspection PyShadowingBuiltins
class CollectionMixin:
    """Proxy frequently-used methods of :class:`pymongo:collection:Collection`.
    """

    #################################
    # Insertion
    #################################

    @classmethod
    def insert_one(cls: Type[T],
                   document: MutableMapping,
                   bypass_document_validation: bool = False,
                   session=None) -> InsertOneResult:
        doc = cls.clean(document, bypass_validation=bypass_document_validation)
        return cls.get_collection().insert_one(
            doc, bypass_document_validation=bypass_document_validation, session=session
        )

    @classmethod
    def insert_many(cls: Type[T],
                    documents: Iterable[MutableMapping],
                    ordered: bool = True,
                    bypass_document_validation: bool = False,
                    session=None) -> InsertManyResult:
        docs = [cls.clean(document, bypass_validation=bypass_document_validation) for document in documents]
        return cls.get_collection().insert_many(
            docs, ordered=ordered, bypass_document_validation=bypass_document_validation, session=session
        )

    #################################
    # Query
    #################################

    @classmethod
    def find_one(cls: Type[T], filter: dict = None, *args, **kw) -> Optional[T]:
        result = cls.get_collection().find_one(filter, *args, **kw)
        if result is not None:
            return cls.from_document(result)

    @classmethod
    def find(cls: Type[T], *args, **kw) -> Union[Cursor, Iterable[T]]:
        return Cursor(cls, cls.get_collection(), *args, **kw)

    #################################
    # Deletion
    #################################

    @classmethod
    def delete_one(cls: Type[T], filter: dict, collation: Collation = None, session=None) -> DeleteResult:
        return cls.get_collection().delete_one(filter, collation=collation, session=session)

    @classmethod
    def delete_many(cls: Type[T], filter: dict, collation: Collation = None, session=None) -> DeleteResult:
        return cls.get_collection().delete_many(filter, collation=collation, session=session)

    #################################
    # Update
    #################################

    @classmethod
    def replace_one(cls: Type[T],
                    filter: dict,
                    replacement: MutableMapping,
                    upsert: bool = False,
                    bypass_document_validation: bool = False,
                    collation: Collation = None,
                    session=None) -> UpdateResult:
        doc = cls.clean(replacement, bypass_validation=bypass_document_validation)
        return cls.get_collection().replace_one(
            filter, doc, upsert=upsert, bypass_document_validation=bypass_document_validation,
            collation=collation, session=session
        )

    @classmethod
    def update_one(cls: Type[T],
                   filter: dict,
                   update: MutableMapping,
                   upsert: bool = False,
                   bypass_document_validation: bool = False,
                   collation: Collation = None,
                   array_filters: List[dict] = None,
                   session=None) -> UpdateResult:
        return cls.get_collection().update_one(
            filter, update, upsert=upsert, bypass_document_validation=bypass_document_validation,
            collation=collation, array_filters=array_filters, session=session
        )

    @classmethod
    def update_many(cls: Type[T],
                    filter: dict,
                    update: MutableMapping,
                    upsert: bool = False,
                    array_filters: List[dict] = None,
                    bypass_document_validation: bool = False,
                    collation: Collation = None,
                    session=None) -> UpdateResult:
        return cls.get_collection().update_many(
            filter, update, upsert=upsert, array_filters=array_filters,
            bypass_document_validation=bypass_document_validation, collation=collation, session=session
        )

    #################################
    # FindAndXXX
    #################################

    @classmethod
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

    @classmethod
    def find_one_and_replace(cls: Type[T],
                             filter: dict,
                             replacement: MutableMapping,
                             bypass_document_validation: bool = False,
                             projection: Union[list, dict] = None,
                             sort: List[tuple] = None,
                             upsert: bool = False,
                             return_document: bool = ReturnDocument.BEFORE,
                             session=None, **kw) -> Optional[T]:
        doc = cls.clean(replacement, bypass_validation=bypass_document_validation)
        result = cls.get_collection().find_one_and_replace(
            filter, doc, projection=projection, sort=sort, upsert=upsert, return_document=return_document,
            session=session, **kw
        )
        if result is not None:
            return cls.from_document(result)

    @classmethod
    def find_one_and_update(cls: Type[T],
                            filter: dict,
                            update: dict,
                            projection: Union[list, dict] = None,
                            sort: List[tuple] = None,
                            upsert: bool = False,
                            return_document: bool = ReturnDocument.BEFORE,
                            array_filters: List[dict] = None,
                            session=None, **kw) -> Optional[T]:
        result = cls.get_collection().find_one_and_update(
            filter, update, projection=projection, sort=sort, upsert=upsert, return_document=return_document,
            array_filters=array_filters, session=session, **kw
        )
        if result is not None:
            return cls.from_document(result)

    #################################
    # Aggregation
    #################################

    @classmethod
    def aggregate(cls: Type[T], pipeline: List[dict], session=None, **kw) -> CommandCursor:
        return cls.get_collection().aggregate(pipeline, session, **kw)

    @classmethod
    def estimated_document_count(cls: Type[T], **kw) -> int:
        return cls.get_collection().estimated_document_count(**kw)

    @classmethod
    def count_documents(cls: Type[T], filter: dict, session=None, **kw) -> int:
        return cls.get_collection().count_documents(filter, session=session, **kw)

    @classmethod
    def distinct(cls: Type[T], key: str, filter: dict = None, session=None, **kw) -> list:
        return cls.get_collection().distinct(key, filter=filter, session=session, **kw)


class Model(BaseModel, CollectionMixin):
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

    def save(self, **kw):
        """Save the document into MongoDB.

        If there is no value for the primary key on this Model instance, the
        instance will be inserted into MongoDB. Otherwise, the entire document
        will be replaced with this version.

        :return This object with the `pk` property filled if it wasn't already.
        """

        state = self._state
        collection = type(self).get_collection()

        if state == 'before_save':
            collection.insert_one(self.to_dict(), **kw)
            self._state = 'after_save'
        elif state == 'after_save':
            collection.replace_one({'_id': self.pk}, self.to_dict(), **kw)
        elif state == 'from_document':
            if self.pk is None:
                raise RuntimeError("The document without an '_id' cannot be saved.")
            collection.replace_one({'_id': self.pk}, self.to_dict(), **kw)

        return self

    @classmethod
    def from_document(cls, doc: MutableMapping):
        """Construct an instance of this class from the given document."""
        obj = cls.from_data(doc)
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
