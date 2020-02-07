import pytest
from pymongo import MongoClient


@pytest.fixture
def db():
    client = MongoClient()
    db = client.get_database('monorm-test')
    yield db
    client.drop_database('monorm-test')
    client.close()
