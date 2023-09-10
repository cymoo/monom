import pytest
from pymongo import MongoClient

from monom import Model, switch_collection, switch_db


def test_switch_db():
    class User(Model):
        pass

    db = MongoClient().get_database('monom-test')
    User.set_db(db)

    assert User.get_db().name == 'monom-test'
    assert User.get_collection().name == 'users'

    old_coll = User.get_collection()

    with switch_db(User, MongoClient().get_database('monom-test1')):
        assert User.get_db().name == 'monom-test1'
        assert (
            User.get_collection().name == 'users' and User.get_collection() != old_coll
        )

    assert User.get_db().name == 'monom-test'
    assert User.get_collection().name == 'users'


def test_switch_collection(db):
    class User(Model):
        pass

    User.set_db(db)
    assert User.get_collection().name == 'users'

    with switch_collection(User, 'members'):
        assert User.get_collection().name == 'members'

    assert User.get_collection().name == 'users'


if __name__ == '__main__':
    pytest.main()
