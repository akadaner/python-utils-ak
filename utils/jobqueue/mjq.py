import pymongo
from pymongo import ReturnDocument, MongoClient

from bson.json_util import dumps
from utils.serialization import js as json

from datetime import datetime, timedelta
import traceback

# todo: create smart builtin
# todo: use tailable cursor like in pymjq
# todo: handle mongodb connection errors!

STATUS_PENDING = 0
STATUS_LOCKED = 1
STATUS_ERROR = 2
STATUS_SUCCESS = 3

DEFAULT_INSERT = {
    'status': STATUS_PENDING,
    'attempts': 0,
    'locked_by': None,
    'locked_at': None,
    'progress': None,
    'payload': None,
    'priority': None,
    'tag': None,
    'comment': None
}


class MongoJobQueue(object):
    """ A queue class. """

    def __init__(self, conn, timeout=300, max_attempts=5, database='jobqueue'):
        """
        :param conn: tuple: (mongodb_connection_string: str, queue_name: str)
        :param consumer_id: str
        :param timeout: int
        :param max_attempts: int
        """
        self.conn = conn
        mongodb_cs, queue_name = conn
        self.cli = MongoClient(mongodb_cs)
        self.collection = self.cli[database][queue_name]
        self.timeout = timeout
        self.max_attempts = max_attempts

    @property
    def col(self):
        return self.collection

    def close(self):
        """ Close the in memory queue connection. """
        self.cli.close()

    def clear_if_completed(self):
        """ Clear the queue if all jobs are completed successfully. """
        if self.col.find({'status': STATUS_SUCCESS}).count() == self.size():
            self.clear()
            return True
        return False

    def get_state(self):
        state = {}
        for i, status in enumerate(['pending', 'locked', 'error', 'success']):
            state[status] = self.col.find({'status': i}).count()
        return state

    def clear(self):
        """ Clear the queue. """
        return self.collection.drop()

    def size(self):
        """ Total size of the queue. """
        return self.collection.count()

    def repair(self, stalled=True, error=True):
        """ Clear out stale locks.
        Increments per job attempt counter.
        Set ERROR status for jobs with too many attempts
        """

        # increase failed attempts for stalled jobs
        if stalled:
            self.collection.update_many(
                {
                    'status': STATUS_LOCKED,

                    'locked_by': {'$ne': None},
                    'locked_at': {'$lt': datetime.now() - timedelta(self.timeout)}},
                {
                    '$set': {'locked_by': None,
                             'locked_at': None,
                             'status': STATUS_PENDING},
                    '$inc': {'attempts': 1}
                }
            )

        if error:
            self.collection.update_many(
                {'attempts': {'$gte': self.max_attempts}},
                {
                    '$set': {'locked_by': None, 'locked_at': None, 'status': STATUS_ERROR}
                }
            )

    def reset_error(self):
        self.collection.update_many({'status': STATUS_ERROR}, {'$set': {'locked_by': None,
                                                                        'locked_at': None,
                                                                        'status': STATUS_PENDING,
                                                                        'attempts': 0}})

    def reset_locked(self):
        self.collection.update_many({'status': STATUS_LOCKED}, {'$set': {'locked_by': None,
                                                                         'locked_at': None,
                                                                         'status': STATUS_PENDING,
                                                                         'attempts': 0}})

    def find(self, *args, **kwargs):
        return self.collection.find(*args, **kwargs)

    def fetch(self, *args, **kwargs):
        return list(self.find(*args, **kwargs))

    def fetch_one(self, *args, **kwargs):
        return self.collection.find_one(*args, **kwargs)

    def delete(self, *args, **kwargs):
        return self.collection.delete_many(*args, **kwargs)

    def drop_error(self):
        """ Drop error jobs. """
        self.collection.delete_many({'status': STATUS_ERROR})

    def drop_success(self):
        """ Drop success jobs. """
        self.collection.delete_many({'status': STATUS_SUCCESS})

    def drop_pending(self):
        """ Drop pending elements. """
        self.collection.delete_many({'status': STATUS_PENDING})

    def drop_non_pending(self):
        """ Drop non-pending jobs. """
        self.collection.delete_many({'status': {'$ne': STATUS_PENDING}})

    def drop_locked(self):
        """ Drop pending elements. """
        self.collection.delete_many({'status': STATUS_LOCKED})

    def put(self, payloads, priority=0, tag=None, comment=None, upsert=False):
        """
        :param payloads: messages
        :param priority:
        :param tag: additional info for the job
        :param comment: additional info for the job
        :param upsert: upsert will make the queue to add job only if is not already in the queue. So this is safe to run many times without worrying to create duplicates in the quee
        :return:
        """
        if not isinstance(payloads, list):
            payloads = [payloads]
        payloads = [self._serialize(payload) for payload in payloads]
        return self._put_many(payloads, priority, tag, comment, upsert=upsert)

    def _serialize(self, payload):
        if isinstance(payload, str):
            return payload
        else:
            try:
                return json.dumps(payload)
            except:
                raise Exception('Failed to serialize payload')

    def get(self, consumer_id):
        """ Get next queue object. """
        query = {'status': STATUS_PENDING,
                 'locked_by': None,
                 'locked_at': None,
                 'attempts': {'$lt': self.max_attempts}}

        update = {'$set': {'locked_by': consumer_id,
                           'locked_at': datetime.now(),
                           'status': STATUS_LOCKED}}

        doc = self.collection.find_one_and_update(query, update, return_document=ReturnDocument.AFTER, sort=[('priority', pymongo.DESCENDING)])
        return self._wrap_one(doc)

    def _wrap_one(self, data):
        if not data:
            return
        return Job(self, data)

    def _put_many(self, payloads, priority=0, tag=None, comment=None, upsert=False):
        jobs = [dict(DEFAULT_INSERT) for _ in payloads]
        for i, payload in enumerate(payloads):
            jobs[i].update({'payload': payload, 'priority': priority, 'tag': tag, 'comment': comment})

        if upsert:
            cur_jobs = self.col.find({'payload': {'$in': payloads}})
            cur_payloads = [job['payload'] for job in cur_jobs]
            jobs = [job for job in jobs if job['payload'] not in cur_payloads]

            # remove duplicate payloads
            _jobs = []
            for job in jobs:
                if job['payload'] not in [_job['payload'] for _job in _jobs]:
                    _jobs.append(job)
            jobs = _jobs

        if jobs:
            return self.col.insert_many(jobs)


class Job(object):
    def __init__(self, queue, data):
        """
        """
        self._queue = queue
        self._data = data

    @property
    def payload(self):
        return self._data['payload']

    @property
    def job_id(self):
        return self._data["_id"]

    @property
    def priority(self):
        return self._data["priority"]

    @property
    def attempts(self):
        return self._data["attempts"]

    @property
    def locked_by(self):
        return self._data["locked_by"]

    @property
    def locked_at(self):
        return self._data["locked_at"]

    ## Job Control
    def update(self, req):
        self._data = self._queue.col.find_one_and_update({"_id": self.job_id}, req, return_document=ReturnDocument.AFTER)
        return self

    def complete(self):
        """ Job has been completed. """
        req = {"$set": {"locked_by": None, "locked_at": None, 'status': STATUS_SUCCESS}}
        return self.update(req)

    def error(self, message=None):
        """ Note an error processing a job, and return it to the queue. """
        req = {"$set": {"locked_by": None, "locked_at": None, "status": STATUS_PENDING},
               "$inc": {"attempts": 1}}
        return self.update(req)

    def set_progress(self, value='0'):
        """ Note progress on a long running task. """
        req = {"$set": {"progress": value, "locked_at": datetime.now()}}
        return self.update(req)

    def release(self):
        """ Put the job back into_queue. """
        req = {"$set": {"locked_by": None, "locked_at": None, 'status': STATUS_PENDING},
               "$inc": {"attempts": 1}}
        return self.update(req)

    def update_dict(self, dic):
        req = {'$set': dic}
        return self.update(req)

    def set_tag(self, tag):
        return self.update_dict({'tag': tag})

    def set_comment(self, comment):
        return self.update_dict({'comment': comment})

    ## Context Manager support
    def __enter__(self):
        return self._data

    def __exit__(self, type, value, tb):
        if (type, value, tb) == (None, None, None):
            self.complete()
        else:
            error = traceback.format_exc()
            self.error(error)

    def reset(self):
        self._data['_id'] = None

    def __repr__(self):
        return dumps(self._data)

    def __str__(self):
        return self.__repr__()
