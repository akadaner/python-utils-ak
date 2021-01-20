import copy
from datetime import datetime

from bson.objectid import ObjectId
from mongoengine import *

from utils_ak.time import cast_str
from utils_ak.mongoengine import *


class Job(Document):
    AUTO_FIELDS = ['created', '_id']
    type = StringField(required=True)
    payload = DictField()
    created = DateTimeField(default=datetime.utcnow)
    meta = {'allow_inheritance': True}


class Execution(Document):
    AUTO_FIELDS = ['created', '_id']
    job = ReferenceField('Job')
    created = DateTimeField(default=datetime.utcnow)


class Product(Document):
    AUTO_FIELDS = ['created', 'version', '_id']
    execution = ReferenceField('Execution')
    created = DateTimeField(default=datetime.utcnow)
