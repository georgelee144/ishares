import datetime
import logging

import pendulum
from airflow.decorators import dag, task

import re

from ishare_utils.constants import (
    BASE_LINK,
    BUCKET_NAME,
    PROJECT_ID,
    STOCK_COLUMNS,
    TICKER_CSV_LINKS,
    ISHARE_STOCK_SCHEMA,
    RENAMED_STOCK_COLUMNS,
)

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
        date = data_interval_end.format("YYYY-MM-DD")
        bucket = storage_client.bucket(BUCKET_NAME)

        for ticker, csv_link in TICKER_CSV_LINKS.items():
            csv_dl_link = f"{BASE_LINK}{csv_link}"
            response = session.get(csv_dl_link)

            attempts = 0
            while response.status_code != 200 and attempts > 3:
                response = session.get(csv_dl_link)
                attempts += 1
                logging.info(f"Failed to get data for {ticker} at {csv_dl_link}")

            if response.status_code == 200:
                csv_content = response.content

                # up to the second occurance of b"\n\xc2\xa0\n" is all the info we need
                # The rest is disclosure or info we already have
                stop_points = [
                    match for match in re.finditer(b"\n\xc2\xa0\n", csv_content)
                ]
                stop_point = stop_points[1].span()[0]

                csv_content = csv_content[:stop_point]

                if re.search(b"\nTicker", csv_content) and not re.search(
                    b"YTM \(%\)", csv_content
                ):
                    file_location = f"ishare_data/stock/{date}/{ticker}.csv"

                elif re.search(b"\nName", csv_content):
                    file_location = f"ishare_data/bond/{date}/{ticker}.csv"

                else:
                    file_location = f"ishare_data/other/{date}/{ticker}.csv"
                    logging.info(
                        f"Got unexpected file {ticker} dumping as {file_location}"
                    )

                logging.info(f"Got data for {ticker} dumping as {file_location}")

                blob = bucket.blob(file_location)
                with blob.open("wb") as f:
                    f.write(csv_content)

            else:
                logging.info(f"Failed to get data for {ticker} at {csv_dl_link}")

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

        logging.info(f"Reading {len(blobs)}.")

        for blob in blobs:
            fund_ticker = blob.name.split("/")[-1][:-4]

            try:
                with blob.open("r") as f:
                    df = pd.read_csv(f, skiprows=9).dropna(subset="Name")
                    f.seek(0)
                    df["fund_ticker"] = fund_ticker
                    df["name_of_fund"] = f.readline().replace("\n", "")

                    date_string = re.search(r"\"(.*)\"", f.readline()).group(1)
                    fund_holdings_as_of_date = pendulum.from_format(
                        date_string, "MMM D, YYYY"
                    ).date()

                    df["fund_holdings_as_of"] = fund_holdings_as_of_date

                df = df[STOCK_COLUMNS]
                df.columns = RENAMED_STOCK_COLUMNS

                str_to_num = [
                    "market_value",
                    "notional_value",
                    "shares",
                    "price",
                    "fx_rate",
                ]

                for column in str_to_num:
                    df[column] = (
                        df[column].astype(str).str.replace(",", "").astype(float)
                    )

                table_export_name = f"ishare.stock_{fund_ticker}_{fund_holdings_as_of_date.format('YYYY-MM-DD')}"

                df.to_gbq(
                    table_export_name,
                    project_id=PROJECT_ID,
                    if_exists="replace",
                    table_schema=ISHARE_STOCK_SCHEMA,
                )

                logging.info(f"Exported {table_export_name}.")
            except Exception as e:
                logging.info(f"Issue sending {fund_ticker} to bigquery because {e}")

        return

    get_and_dump_ishare_data() >> [read_ishare_stock_data()]


ishare_index_data_dag()
