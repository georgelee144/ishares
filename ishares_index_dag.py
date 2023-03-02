import asyncio
import datetime
import logging

import pendulum
from airflow.decorators import dag, task
from ishare_utils.constants import (
    BOND_COLUMNS,
    BUCKET_NAME,
    ISHARE_BOND_DF_SCHEMA,
    ISHARE_BOND_SCHEMA,
    ISHARE_STOCK_DF_SCHEMA,
    ISHARE_STOCK_SCHEMA,
    PROJECT_ID,
    RENAMED_BOND_COLUMNS,
    RENAMED_STOCK_COLUMNS,
    STOCK_COLUMNS,
)
from ishare_utils.helper import process_csv_and_export_gbq, run_async_get_csv

default_args = {"retries": 5, "retry_delay": datetime.timedelta(seconds=15)}


@dag(
    dag_id=f"ishare_index_data_dag",
    schedule_interval="0 1 * * 2-6",
    start_date=pendulum.datetime(2023, 1, 1, tz="EST"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=100),
    default_args=default_args,
)
def ishare_index_data_dag():
    @task
    def get_and_dump_ishare_data(data_interval_end=None):
        import requests
        from google.cloud import storage

        session = requests.Session()

        storage_client = storage.Client(PROJECT_ID)
        bucket = storage_client.bucket(BUCKET_NAME)

        date = data_interval_end.format("YYYY-MM-DD")

        asyncio.run(run_async_get_csv(session, bucket, date))

        return

    @task
    def read_ishare_stock_data(data_interval_end=None):
        import pandas as pd
        from google.cloud import storage

        storage_client = storage.Client(PROJECT_ID)
        date = data_interval_end.format("YYYY-MM-DD")
        blobs = [
            blob
            for blob in storage_client.list_blobs(
                BUCKET_NAME, prefix=f"ishare_data/stock/{date}"
            )
        ]

        logging.info(f"Reading {len(blobs)} blobs.")

        # can't make this tasks async because Google cloud storage is limiting
        for blob in blobs:
            process_csv_and_export_gbq(
                blob,
                STOCK_COLUMNS,
                RENAMED_STOCK_COLUMNS,
                ISHARE_STOCK_DF_SCHEMA,
                ISHARE_STOCK_SCHEMA,
            )
        return

    @task
    def read_ishare_bond_data(data_interval_end=None):
        import pandas as pd
        from google.cloud import storage

        storage_client = storage.Client(PROJECT_ID)
        date = data_interval_end.format("YYYY-MM-DD")
        blobs = [
            blob
            for blob in storage_client.list_blobs(
                BUCKET_NAME, prefix=f"ishare_data/bond/{date}"
            )
        ]

        logging.info(f"Reading {len(blobs)} blobs.")

        for blob in blobs:
            process_csv_and_export_gbq(
                blob,
                BOND_COLUMNS,
                RENAMED_BOND_COLUMNS,
                ISHARE_BOND_DF_SCHEMA,
                ISHARE_BOND_SCHEMA,
            )

        return

    get_and_dump_ishare_data() >> [read_ishare_stock_data(), read_ishare_bond_data()]


ishare_index_data_dag()
