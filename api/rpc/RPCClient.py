import uuid

import pika

from django.conf import settings
from ..settings import api_settings
from .BasicPikaClient import PikaRabbitMQConfig

class RPCClientSaveS3FileToDB:
    def __init__(self):
        if api_settings.RABBIT_MQ_ENV == "local":
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=api_settings.RABBIT_MQ_LOCAL_URL))
        
        else:
            basic_pika_publisher = PikaRabbitMQConfig(
                api_settings.RABBIT_MQ_BROKER_ID, 
                api_settings.RABBIT_MQ_USERNAME, 
                api_settings.RABBIT_MQ_PASSWORD, 
                api_settings.RABBIT_MQ_REGION
            )
            self.connection = basic_pika_publisher._get_connection()
            
        self.channel = self.connection.channel()
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, file):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key='rpc_catv_consumer_update_file',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=str(file))
        while self.response is None:
            self.connection.process_data_events()
        self.connection.close()
        return self.response

class RPCClientFetchIndicators:
    def __init__(self):
        if api_settings.RABBIT_MQ_ENV == "local":
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=api_settings.RABBIT_MQ_LOCAL_URL))
        
        else:
            basic_pika_publisher = PikaRabbitMQConfig(
                api_settings.RABBIT_MQ_BROKER_ID, 
                api_settings.RABBIT_MQ_USERNAME, 
                api_settings.RABBIT_MQ_PASSWORD, 
                api_settings.RABBIT_MQ_REGION
            )
            self.connection = basic_pika_publisher._get_connection()

        self.channel = self.connection.channel()
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, request_dict):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key='rpc_catv_consumer_fetch_indicators',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=str(request_dict))
        while self.response is None:
            self.connection.process_data_events()
        self.connection.close()
        return self.response

class RPCClientCARAReports:
    def __init__(self):
        if api_settings.RABBIT_MQ_ENV == "local":
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=api_settings.RABBIT_MQ_LOCAL_URL))
        
        else:
            basic_pika_publisher = PikaRabbitMQConfig(
                api_settings.RABBIT_MQ_BROKER_ID, 
                api_settings.RABBIT_MQ_USERNAME, 
                api_settings.RABBIT_MQ_PASSWORD, 
                api_settings.RABBIT_MQ_REGION
            )
            self.connection = basic_pika_publisher._get_connection()
            
        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, result_file_ids):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key='rpc_portal_cara_reports',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=str(result_file_ids))
        while self.response is None:
            self.connection.process_data_events()
        self.connection.close()
        return self.response