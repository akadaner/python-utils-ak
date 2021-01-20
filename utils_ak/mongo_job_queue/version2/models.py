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
    status = StringField(required=True, default='pending', choices=['pending', 'locked', 'error', 'success'])
    response = StringField()

    locked_by = ReferenceField('Execution')
    locked_at = DateTimeField(default=datetime.utcnow)

    executions = ListField(ReferenceField('Execution'))
    meta = {'allow_inheritance': True}


class Execution(Document):
    AUTO_FIELDS = ['created', '_id']
    job = ReferenceField(Job)
    config = DictField()
    created = DateTimeField(default=datetime.utcnow)
    status = StringField(required=True, default='pending', choices=['pending', 'running', 'serving', 'error', 'success'])
    response = StringField()

    meta = {'allow_inheritance': True}
