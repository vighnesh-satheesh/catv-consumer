import json
import traceback
import uuid
import pika
from ..settings import api_settings
from .BasicPikaClient import PikaRabbitMQConfig


class RPCClient:
    def __init__(self):
        if api_settings.RABBIT_MQ_ENV == "local":
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=api_settings.RABBIT_MQ_LOCAL_URL))
        else:
            basic_pika_publisher = PikaRabbitMQConfig(
                api_settings.RABBIT_MQ_BROKER_ID,
                api_settings.RABBIT_MQ_USERNAME,
                api_settings.RABBIT_MQ_PASSWORD,
                api_settings.RABBIT_MQ_REGION,
                api_settings.RABBIT_MQ_HOST,
                api_settings.RABBIT_MQ_PORT
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

    def call(self, message, queue):
        try:
            self.response = None
            self.corr_id = str(uuid.uuid4())
            self.channel.basic_publish(
                exchange='',
                routing_key=queue,
                properties=pika.BasicProperties(
                    reply_to=self.callback_queue,
                    correlation_id=self.corr_id,
                ),
                body=str(message))
            while self.response is None:
                self.connection.process_data_events()
            self.connection.close()
            if queue in ['rpc_catv_update_usage_error']:
                return self.response.decode("utf-8")
            return json.loads(self.response.decode("utf-8"))
        except Exception:
            traceback.format_exc()

# Need to refactor this to GCS.
def update_s3_attached_file_uid(request):
    rpc = RPCClient()
    return rpc.call(json.dumps(request), 'rpc_catv_consumer_update_file')


def fetch_indicators(request):
    rpc = RPCClient()
    return rpc.call(json.dumps(request), 'rpc_catv_consumer_fetch_indicators')


def fetch_cara_report(request):
    rpc = RPCClient()
    return rpc.call(json.dumps(request), 'rpc_portal_cara_reports')


def update_catv_usage_error(user_id):
    rpc = RPCClient()
    return rpc.call(user_id, 'rpc_catv_update_usage_error')
