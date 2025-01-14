import sys
from multiprocessing.pool import ThreadPool
from time import sleep

from django.core.management.base import BaseCommand
from django.db import transaction, connection

from api.constants import Constants
from api.consumers import process_catv_messages
from api.models import CatvJobQueue, CatvCSVJobQueue
from api.settings import api_settings


class Command(BaseCommand):
    help = "Starts the consumer for CATV and blocks indefinitely"

    def handle(self, *args, **options):
        try:
            print("Connecting to databasejob queue table....")
            while True:
                with transaction.atomic():
                    pending_jobs = CatvJobQueue.objects.using('default').raw(Constants.QUERIES["SELECT_CATV_JOBS"].format(api_settings.CATV_NUM_JOBS_PICK))
                    pending_csv_jobs = CatvCSVJobQueue.objects.using('default').raw(Constants.QUERIES["SELECT_CSV_CATV_JOBS"].format(api_settings.CATV_NUM_JOBS_PICK))

                pending_count = 0
                pending_csv_count = 0

                if pending_jobs:
                    pending_jobs_arr = list(pending_jobs)
                    pending_job_ids = [job.id for job in pending_jobs_arr]
                    pending_count = len(pending_jobs_arr)
                if pending_csv_jobs:
                    pending_csv_jobs_arr = list(pending_csv_jobs)
                    pending_csv_job_ids = [job.id for job in pending_csv_jobs_arr]
                    pending_csv_count = len(pending_csv_jobs_arr)

                if pending_count > 0:
                    pending_job_ids = "({0})".format(pending_job_ids[0]) if pending_count == 1 else tuple(pending_job_ids)
                    query = Constants.QUERIES['UPDATE_CATV_JOBS'].format(pending_job_ids)
                    with connection.cursor() as cursor:
                        cursor.execute(query)
                    for job in pending_jobs_arr:
                        process_catv_messages(job)
                elif pending_csv_count > 0:
                    pending_csv_job_ids = "({0})".format(pending_csv_job_ids[0]) if pending_csv_count == 1 else tuple(pending_csv_job_ids)
                    query_csv = Constants.QUERIES['UPDATE_CSV_CATV_JOBS'].format(pending_csv_job_ids)
                    with connection.cursor() as cursor:
                        cursor.execute(query_csv)
                    pool = ThreadPool(processes=4)
                    pool.map(process_catv_messages, [pending_csv_jobs_arr, True])
                else:
                    print("Relaxing for some time...")
                    sleep(15)
        except KeyboardInterrupt:
            self.stdout.write(self.style.ERROR("Encountered a keyboard interrupt, exiting..."))
            sys.exit(1)
