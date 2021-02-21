from mongoengine import *

from datetime import datetime


class Job(Document):
    AUTO_FIELDS = ["created", "_id"]
    type = StringField(required=True)
    payload = DictField()

    created = DateTimeField(default=datetime.utcnow)

    locked_by = ReferenceField("Execution")
    locked_at = DateTimeField()

    image = StringField(required=True)
    params = DictField()

    workers = ListField(ReferenceField("Worker"))

    meta = {"allow_inheritance": True}


class Worker(Document):
    AUTO_FIELDS = ["created", "_id"]
    job = ReferenceField(Job)
    config = DictField()
    created = DateTimeField(default=datetime.utcnow)
    status = StringField(
        required=True,
        default="pending",
        choices=["pending", "initializing", "running", "error", "success", "stalled"],
    )
    response = StringField()

    meta = {"allow_inheritance": True}
