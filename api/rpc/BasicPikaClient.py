import pika


class PikaRabbitMQConfig:

    def __init__(self, rabbitmq_broker_id, rabbitmq_user, rabbitmq_password, region,rabbitmq_host,rabbitmq_port):
        credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
        self.parameters = pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port, credentials=credentials)

    def _get_connection(self):
        connection = pika.BlockingConnection(self.parameters)
        return connection