from contextlib import contextmanager
from typing import Union, Type

from pymongo.collection import Collection
from pymongo.database import Database

from . import Model

__all__ = [
    'switch_db',
    'switch_collection'
]


@contextmanager
def switch_db(model: Type[Model], db: Database):
    """Switch database and collection temporarily, not thread safe"""

    prev_db = model.get_db()
    prev_coll = model.get_collection()

    coll_name = prev_coll.name
    coll_options = prev_coll.options()

    model.set_db(db)
    model.set_collection(coll_name, **coll_options)

    try:
        yield
    finally:
        model.set_db(prev_db)
        model.set_collection(prev_coll)


@contextmanager
def switch_collection(model: Type[Model], collection: Union[str, Collection], **option):
    """Switch collection temporarily, not thread safe"""

    prev = model.get_collection()
    model.set_collection(collection, **option)

    try:
        yield
    finally:
        model.set_collection(prev)
