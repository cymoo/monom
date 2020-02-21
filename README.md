# Monom: An object mapper for MongoDB with type hints

## Installation

```bash
$ pip install monom
```

## About

Monom is designed to manage your models clearly and easily, which is as simple and thin as possible.

* schema declaration using type hints

* document validation on insert and update

* default value, field alias, index declaration, custom converter and validator

* minimum api, least memory burden

-----

## A Quick Example

```python
from monom import *

class User(EmbeddedModel):
    name: str
    email: str

class Post(Model):
    user: User
    title: str
    content: str
    tags: List[str]
    rank: int
    visible: bool = True
    created_on: datetime = datetime.utcnow
    placeholder: Any

    class Meta:
        required = ['user', 'title']
        indexes = ['title']
        converters = {
            'title': lambda x: x.capitalize()
        }
        validators = {
            'title': lambda x: len(x) > 5
        }

    @property
    def url(self):
        return '/posts/' + str(self.pk)

    @classmethod
    def find_posts_by_lucy(cls):
        return Post.find({'user.name': 'Lucy'}).sort('created_on')

db = MongoClient().get_database('demo')
Post.set_db(db)
post = Post(
    user={'name': 'Lucy', 'email': 'foo@example.com'},
    # same as the above
    # user=User(name='Lucy', email='foo@example.com'),
    title='hello world',
    content='monom is awesome...',
    tags=['life', 'art']
)
post.save()
assert post.visible is True
assert isinstance(post.user, User)
assert post.user.name == 'Lucy'
```

-----

## Guide

### Connection

No extra connection methods. Just pass pymongo's `Database` instance to your model.

```python
from monom import Model, MongoClient

class MyModel(Model):
    pass

db = MongoClient().get_database('demo')
MyModel.set_db(db)
```

-----

### Field Type

* `str`, `int`, `float`, `bool`, `bytes`, `datetime`, `ObjectId`: you are familiar with them already
  
* `dict`: accepts a `dict` regardless of its items
  
* `list`: accepts a `list` regardless of its items
  
* subclass of `EmbeddedModel`: represents MongoDB's embedded document
  
* `List`: `List`[*the above type*] or `List`[`List`[*the above type*]] or any nested depth
  
* `Any`: any type that can be saved into MongoDB
  
-----

### Model

#### Model Instance

To create a new model object, provide values for its fields as constructor keyword arguments.
Monom will convert and validate these values when constructing the new instance.

```python
from monom import Model

class User(Model):
    name: str
    visible: bool = True

user = User(name='foo')
```

You can declare a field with an initial value, which acts as the field's default value.
If the value is a `callable`, it will be called on each saving or inserting.

#### Methods

* `save(full_update=False, *kw)`

Save the data into MongoDB.

1. The new document will be inserted into MongoDB.

2. The existing document will be updated atomically using operator '$set' and '$unset'.

3. `list` mutation cannot be tracked; but you can pass an keyword argument `full_update=True` to perform a full update.

```python
from monom import *

class User(Model):
    name: str
    email: str
    hobbits: List[str]
    active: bool = True
    created_on: datetime = datetime.utcnow

# connect to mongodb
User.set_db(MongoClient().get_database('demo'))

# insert a doc
User(name='Lucy', email='lucy@foo.com', hobbits=['music', 'sport']).save()

# find a doc filtering out some fields
user = User.find_one({}, {'created_on': False})

user.name = 'foobar'
del user.email

# saved with an atomic update
user.save()

user.hobbits.append('programming')
# to save a doc with changed list items, you should set `full_update=True`
user.save(full_update=True)

```

* `delete(**kw)`

Delete the data from MongoDB.

* `pk`

An alias for the primary key (`_id` in MongoDB).

* `to_dict()`

Return an ordered dict containing the instance's data with the same order as the field definition order.

* `to_json(**kw)`

Return a json string. Some specific types (`ObjectId`, `datetime`, etc.) will be handled correctly.

* `get(name, default=None)`

Return the value for name if it has a value, else default.

#### Class Methods

* `set_db(db)`

Pass a `pymongo.database.Database` to the model.

* `set_collection(collection)`

Pass a string or a `pymongo.collection.Collection` to the model.

If it isn't called explicitly, plural form of the model's name will be the collection name.

* `get_db()`

* `get_collection()`

#### CRUD Methods

Monom adds no extra methods to operate MongoDB.

It proxies a subset of methods in `pymongo.collection:Collection`, which will perform data cleaning and convert the data from query operations to the model object.

* `insert_one`, `insert_many`, `replace_one`, `update_one`, `update_many`, `find_one_and_update`, `find_one_and_replace` will perform data conversion and validation.

* `find_one`, `find`, `find_one_and_delete`, `find_one_and_replace`, `find_one_and_update` will convert query results to the corresponding model instance.

__`find` returns a `Cursor` of model instances instead of dicts. Before dump your documents to json, remember to do a small conversion.__

```python
from monom import *

class BinData(Model):
    name: str
    data: bytes

BinData.set_db(MongoClient().get_database('demo'))
BinData(name='foo', data=b'abc').save()
BinData(name='bar', data=b'xyz').save()

json_dumps([bd.to_dict() for bd in BinData.find()])
# or call pymongo's methods directly
json_dumps(BinData.get_collection().find())
```

-----

#### Meta

You can add extra constraints for your models by defining an inner class named `Meta` in your model or embedded model.

* `required`: the field must exist in your data

```python
from monom import Model

class User(Model):
    name: str
    email: str

    class Meta:
        required = ['name']
```

* `validators` and `converters`

```python
from monom import Model

class User(Model):
    name: str
    age: int

    class Meta:
        validators = {
            'age': lambda x: x < 200
        }
        converters = {
            'name': lambda x: x.strip()
        }
```

* `aliases`: sometimes you may want to save some fields in another names

```python
from monom import Model

class User(Model):
    id: int
    first_name: str

    class Meta:
        aliases = [
            ('id', '_id'),
            ('first_name', 'firstName'),
        ]

user = User(id=42, first_name='Lucy')
user.id
# 42
user.to_dict()
# {'_id': 42, 'firstName': 'Lucy'}
```

* `Indexes`

```python
from monom import Model, DESCENDING

class FancyModel(Model):
    class Meta:
        indexes = [
            'f1',  # a single key ascending index
            ('f2', DESCENDING),  # a single key descending index
            ['f3', 'f4'],  # a compound index both ascending
            ['f5', ('f6', DESCENDING)],  # a compound index on 'f5' ascending and 'f6' descending
            [('f7', DESCENDING), ('f8', DESCENDING)],  # a compound index both descending
            {'key': 'f9', 'expire_after_seconds': 3600, 'unique': True}  # a single key ascending index with ttl and unique property
        ]
```

__Index declaration cannot appear in embedded model.__

-----

#### Options

* `dict_class`

The underlying data of model instance are saved in an ordered dict. You may change it to `bson.son.SON` or other compatible types.
Default value is `collections.OrderedDict`.

* `warn_extra_data`

Whether checks extra data that aren't declared in the model and emits some warnings.
Default value is `True`.

* `auto_build_index`

Whether enables auto index creation or deletion.
You may disable it when in production because index management may be performed as part of a deployment system.
Default value is `True`.

__Theses options can be set on `Model` or the subclass of `Model`; if set on `Model`, all subclasses will inherit them.__

```python
from monom import Model
from bson.son import SON

Model.dict_class = SON

class User(Model):
    name: str

user = User(name='foo')
assert isinstance(user.to_dict(), SON)
```

-----

### Helpers

* `switch_db`: switch to a different database temporarily

```python
from monom import Model, MongoClient, switch_db

class FancyModel(Model):
    pass

foo_db = MongoClient().get_database('foo')
FancyModel.set_db(foo_db)

bar_db = MongoClient().get_database('bar')

with switch_db(FancyModel, bar_db):
    assert FancyModel.get_db().name == 'bar'
```

* `switch_collection`: switch to a different collection temporarily

```python
from monom import Model, MongoClient, switch_collection

class FancyModel(Model):
    pass

db = MongoClient().get_database('my-db')
FancyModel.set_db(db)

with switch_collection(FancyModel, 'foobar'):
    assert FancyModel.get_collection().name == 'foobar'
```

### Logging

In several cases, some warnings will be emitted. If that's annoying, you can change the logger level or set a new logger.

```python
import logging
from monom import get_logger, set_logger

# change level
get_logger().setLevel(logging.ERROR)

# or set a new logger
set_logger(logging.getLogger('foobar'))
```

-----

### Good Old Django-Style

If you like the classical style, here you are.

```python
from monom.fields import *
from monom import Model, EmbeddedModel, datetime

class Comment(EmbeddedModel):
    content = StringField()
    created_on = DateTimeField(default=datetime.utcnow)

class Post(Model):
    id = ObjectIdField(name='_id')
    title = StringField(required=True, converter=lambda x: x.capitalize())
    content = StringField(validator=lambda x: len(x) > 50)
    comments = ArrayField(EmbeddedField(Comment))
    rank = IntField(max_value=100)
    visible = BooleanField(default=True)
```

Using this style, you can pass `name` (`alias` aka), `required`, `default`, `converter`, `validator` as constructor keyword arguments.

#### One-to-one match with type hints

* `str` -> `StringField`

* `int` -> `IntField`

* `float` -> `FloatField`

* `bool` -> `BooleanField`

* `bytes` -> `BytesField`

* `datetime` -> `DateTimeField`

* `ObjectId` -> `ObjectIdField`

* `dict` -> `DictField`

* subclass of `EmbeddedModel` -> `EmbeddedField`

* `list` -> `ListField`

* `List` -> `ArrayField`

* `Any` -> `AnyField`

-----

### Caveats

* Inheritance of fields through class inheritance cannot work, for it will cause confusing relationships between model and embedded model.

* You'd better not mix type-hints style with django-orm style; if you insist on that the field definition order may not be reserved.

## Tests

To run the test suite, ensure you are running a local MongoDB instance on the default port and have pytest installed.

```bash
$ pytest
```

## Dependencies

* Python >= 3.6
* pymongo >= 3.7

## License

MIT
