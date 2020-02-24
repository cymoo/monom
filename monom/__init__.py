"""
An object mapper for MongoDB with type hints.
~~~~~~~~~~~~
"""

from .model import BaseModel, EmbeddedModel
from .mongo import MongoModel as Model
from .helpers import switch_collection, switch_db
from .utils import DotSon, get_logger, set_logger

from pymongo import MongoClient, ASCENDING, DESCENDING
from bson.objectid import ObjectId
from bson.json_util import loads as json_loads, dumps as json_dumps

from datetime import datetime
from typing import List, Any

__version__ = '1.1.0'
