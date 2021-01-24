import sys
from multiprocessing import Process
from utils_ak.simple_microservice.rabbitmqservice import Microservice
import time

class PingPong(Microservice):
    def __init__(self, pub_queue, sub_queue, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pub_queue = pub_queue
        self.sub_queue = sub_queue
        self.add_callback(callback=self.callback)

    def callback(self):
        time.sleep(1.0)
        print('Publish message {} to {}'.format(self.pub_queue, self.pub_queue))
        self.publish(queue=self.pub_queue, msg=self.pub_queue)

    def run(self):
        self.subscribe(self.sub_queue)
        self.subscribe(self.pub_queue)
        print('Queues created')
        self.publish(queue=self.pub_queue, msg=self.sub_queue)
        time.sleep(1.0)
        self.start_consuming(self.sub_queue)