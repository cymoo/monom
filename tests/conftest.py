import pytest
from pymongo import MongoClient


@pytest.fixture
def db():
    client = MongoClient()
    db = client.get_database('monom-test')
    yield db
    client.drop_database('monom-test')
    client.close()
