
from datetime import datetime
from pprint import pprint


class MonitorActor:
    def __init__(self, microservice, heartbeat_timeout=10):
        self.microservice = microservice
        self.workers = {}  # {id: {status, state, last_heartbeat}}
        self.heartbeat_timeout = heartbeat_timeout

        self.microservice.add_callback('monitor', '', self.on_monitor)
        self.microservice.add_timer(self.update_stalled, 3.0)

    def update_status(self, worker_id, status):
        if status != self.workers[worker_id].get('status'):
            self.microservice.publish_json('monitor_out', 'status_change', {'id': worker_id, 'old_status': self.workers[worker_id].get('status'), 'new_status': status})
            self.workers[worker_id]['status'] = status

    def update_stalled(self):
        for worker_id in self.workers:
            if 'last_heartbeat' in self.workers[worker_id] and (datetime.utcnow() - self.workers[worker_id]['last_heartbeat']).total_seconds() > self.heartbeat_timeout:
                self.update_status(worker_id, 'stalled')

    def on_monitor(self, topic, msg):
        self.microservice.logger.info(('On monitor', topic, msg))

        worker_id = msg['id']
        if worker_id not in self.workers:
            self.microservice.publish_json('monitor_out', 'new', {'id': worker_id})
            self.workers[worker_id] = {}

        if topic == 'heartbeat':
            self.workers[worker_id]['last_heartbeat'] = datetime.utcnow()
        elif topic == 'state':
            self.update_status(worker_id, msg['status'])
            self.workers[worker_id]['state'] = msg['state']
        elif topic in ['status_change', 'new']:
            pass
        else:
            raise ValueError(topic)

        self.microservice.logger.info(('Current monitor state', self.workers))