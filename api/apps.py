from django.apps import AppConfig
from celery import Celery
from django.conf import settings
import requests
import json
import os

class ApiConfig(AppConfig):
    name = 'api'
    verbose_name = "ApiConfig"
    app = Celery('tasks', broker=settings.BROKER_URL)

    @classmethod
    @app.task
    def send_slack_webhook(cls):
        if settings.ENVIRONMENT == "development" and os.environ.get("CONTAINER_TYPE") == "portal_api":
            return True
        if os.environ.get("CONTAINER_TYPE") == "portal_admin":
            data = {
                "text": "PORTAL ADMIN " + settings.ALLOWED_HOSTS[0] + " is ready."
            }
        else:
            data = {
                "text": "PORTAL API " + settings.API_SETTINGS["WEB_URL"] + " is ready."
            }
        r = requests.post(
            "https://hooks.slack.com/services/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",
            data=json.dumps(data),
            headers={
                "Content-Type": "application/json"
            }
        )

    def ready(self):
        self.send_slack_webhook()
