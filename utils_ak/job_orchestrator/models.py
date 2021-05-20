from mongoengine import *


from datetime import datetime


class Job(Document):
    AUTO_FIELDS = ["created", "_id"]
    type = StringField(required=True)
    payload = DictField()

    created = DateTimeField(default=datetime.utcnow)

    locked_by = ReferenceField("Worker")
    locked_at = DateTimeField()

    runnable = DictField(required=True)
    params = DictField()

    running_timeout = IntField()  # how much time allowed for the whole job runtime.
    initializing_timeout = IntField(
        default=20.0, required=True
    )  # how much time allowed for job initialization.

    status = StringField(
        required=True,
        default="pending",
        choices=[
            "pending",
            "initializing",
            "running",
            "error",
            "success",
            "stalled",
            "terminated",
        ],
    )

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
        choices=[
            "pending",
            "initializing",
            "running",
            "error",
            "success",
            "stalled",
            "terminated",
        ],
    )
    response = StringField()

    meta = {"allow_inheritance": True}
