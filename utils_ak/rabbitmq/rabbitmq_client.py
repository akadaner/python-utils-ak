import pika
import sys
import threading

'''
    Simple way to run rabitmq localy 
    sudo docker run -d -e RABBITMQ_DEFAULT_VHOST=test --name rabbitmq  rabbitmq:3-management
'''
DEFAULT_CONFIG = {
    'RABBITMQ_USERNAME': 'guest',
    'RABBITMQ_PASSWORD': 'guest',
    'RABBITMQ_HOST': '172.17.0.2',
    'RABBITMQ_VHOST': 'test',
    'RABBITMQ_PORT': 5672
}


class RabbitMQClient:
    def __init__(self, config=DEFAULT_CONFIG, callback=None):
        credentials = pika.PlainCredentials(username=config['RABBITMQ_USERNAME'],
                                            password=config['RABBITMQ_PASSWORD'])
        connection_params = pika.ConnectionParameters(host=config['RABBITMQ_HOST'],
                                                      virtual_host=config['RABBITMQ_VHOST'],
                                                      port=config['RABBITMQ_PORT'],
                                                      credentials=credentials)
        connection = pika.BlockingConnection(parameters=connection_params)
        self.channel = connection.channel()
        self.callback = callback

    def create_queue(self, queue):
        self.channel.queue_declare(queue=queue)

    def create_queues(self, queues):
        for queue in queues:
            self.create_queue(queue)

    def subscribe(self, queue):
        self.channel.queue_declare(queue=queue)

    def publish(self, queue, msg):
        self.channel.basic_publish(exchange='', routing_key=queue, body=msg)
        # todo: check connection

    def handler(self, channel, method, properties, body):
        thread = threading.Thread(target=self.callback, args=(body,))
        thread.start()

        while thread.is_alive():
            channel._connection.sleep(1.0)
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def start_consuming(self, queue):
        self.channel.basic_consume(queue=queue, on_message_callback=self.handler)
        self.channel.start_consuming()