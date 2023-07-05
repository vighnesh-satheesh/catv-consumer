import sys
from time import sleep

from django.core.management.base import BaseCommand
from django.db import transaction, connection
from multiprocessing.pool import ThreadPool

from api.constants import Constants
from api.consumers import process_catv_messages
from api.models import CatvJobQueue, CatvCSVJobQueue
from api.serializers import CATVSerializer
from api.settings import api_settings


class Command(BaseCommand):
    help = "Starts the consumer for CATV and blocks indefinitely"

    def handle(self, *args, **options):
        try:
            print("Connecting to databasejob queue table....")
            while(True):
                with transaction.atomic():
                    pending_jobs = CatvJobQueue.objects.using('default').raw(Constants.QUERIES["SELECT_CATV_JOBS"].format(api_settings.CATV_NUM_JOBS_PICK))
                    pending__csv_jobs = CatvCSVJobQueue.objects.using('default').raw(Constants.QUERIES["SELECT_CSV_CATV_JOBS"])
                pending_jobs_arr = list(pending_jobs)
                pending_count = len(pending_jobs_arr)
                pending__csv_jobs_arr = list(pending__csv_jobs)
                pending__csv_count = len(pending__csv_jobs_arr)
                if pending_count > 0:
                    query = Constants.QUERIES['UPDATE_CATV_JOBS']
                    with connection.cursor() as cursor:
                        cursor.execute(query)
                    for job in pending_jobs_arr:
                        print(job.message)
                        process_catv_messages(job)
                elif pending__csv_count > 0:
                    query_csv = Constants.QUERIES['UPDATE_CSV_CATV_JOBS']
                    with connection.cursor() as cursor:
                        cursor.execute(query_csv)
                    pool = ThreadPool(processes=4)
                    pool.map(process_catv_messages, pending__csv_jobs_arr)
                else:
                    print("Relaxing for some time...")
                    sleep(15)
        except KeyboardInterrupt:
            self.stdout.write(self.style.ERROR("Encountered a keyboard interrupt, exiting..."))
            sys.exit(1)
