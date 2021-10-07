from celery.registry import tasks
from celery.task import Task
from django.db import connections
from django.utils.timezone import now

from .constants import Constants


class CatvHistoryTask(Task):
    def run(self, *args, **kwargs):
        entry = kwargs['history']
        from_history = kwargs['from_history']
        query_list = [Constants.QUERIES['INSERT_USER_CATV_HISTORY'], Constants.QUERIES['UPDATE_USER_CATV_USAGE']]
        query_data = [(entry['user_id'], entry['wallet_address'], entry.get('token_address', ''),
                       entry.get('source_depth', 0), entry.get('distribution_depth', 0), entry['transaction_limit'],
                       entry['from_date'], entry['to_date'], now(), entry['token_type']),
                      (entry['user_id'],)]

        with connections['default'].cursor() as cursor:
            if not from_history:
                for query, data in zip(query_list, query_data):
                    cursor.execute(query.format(*data))
            else:
                cursor.execute(query_list[0].format(*query_data[0]))
        return True

class CatvPathHistoryTask(Task):
    def run(self, *args, **kwargs):
        entry = kwargs['history']
        from_history = kwargs['from_history']
        query_list = [Constants.QUERIES['INSERT_USER_CATV_PATH_SEARCH'], Constants.QUERIES['UPDATE_USER_CATV_USAGE']]
        query_data = [(entry['user_id'], entry['address_from'], entry['address_to'], entry['depth'],
                       entry['from_date'], entry['to_date'], now(), entry['token_type'], entry['min_tx_amount'],
                       entry['limit_address_tx'], entry['token_address']),
                      (entry['user_id'],)]

        with connections['default'].cursor() as cursor:
            if not from_history:
                for query, data in zip(query_list, query_data):
                    cursor.execute(query.format(*data))
            else:
                cursor.execute(query_list[0].format(*query_data[0]))
        return True

tasks.register(CatvHistoryTask)
tasks.register(CatvPathHistoryTask)