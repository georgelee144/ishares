import asyncio
import logging
import re
import time
from random import randint
import requests
import pandas as pd
import pendulum
from google.cloud import storage

from ishare_utils.constants import BASE_LINK, PROJECT_ID, TICKER_CSV_LINKS


async def run_async_get_csv(
    session: requests.Session(), bucket: storage.Client.bucket(), date: str
):
    tasks = [
        asyncio.create_task(async_get_csv(ticker, csv_link, session, bucket, date))
        for ticker, csv_link in TICKER_CSV_LINKS.items()
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    return results


async def async_get_csv(
    ticker: str,
    csv_link: str,
    session: requests.Session(),
    bucket: storage.Client.bucket(),
    date: str,
):
    await asyncio.sleep(randint(0, 5))

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
        stop_points = [match for match in re.finditer(b"\n\xc2\xa0\n", csv_content)]
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
            logging.info(f"Got unexpected file {ticker} dumping as {file_location}")

        logging.info(f"Got data for {ticker} dumping as {file_location}")

        blob = bucket.blob(file_location)
        with blob.open("wb") as f:
            f.write(csv_content)

    else:
        logging.error(f"Failed to get data for {ticker} at {csv_dl_link}")

        return f"Failed to get data for {ticker} at {csv_dl_link}"


def get_csv(
    ticker: str,
    csv_link: str,
    session: requests.Session(),
    bucket: storage.Client.bucket(),
    date: str,
):
    time.sleep(randint(0, 5))

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
        stop_points = [match for match in re.finditer(b"\n\xc2\xa0\n", csv_content)]
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
            logging.info(f"Got unexpected file {ticker} dumping as {file_location}")

        logging.info(f"Got data for {ticker} dumping as {file_location}")

        blob = bucket.blob(file_location)
        with blob.open("wb") as f:
            f.write(csv_content)

    else:
        logging.error(f"Failed to get data for {ticker} at {csv_dl_link}")

        return


def process_csv_and_export_gbq(
    blob: storage.Client.bucket.blob,
    info_columns: list,
    renamed_columns: list,
    df_schema: list,
    gbq_schema: list,
):
    fund_ticker = blob.name.split("/")[-1][:-4]
    type_of_fund = blob.name.split("/")[1]

    try:
        with blob.open("r") as f:
            df = pd.read_csv(f, skiprows=9, na_values="-").dropna(subset="Name")
            f.seek(0)
            df["fund_ticker"] = fund_ticker
            df["name_of_fund"] = f.readline().replace("\n", "")

            date_string = re.search(r"\"(.*)\"", f.readline()).group(1)
            fund_holdings_as_of_date = pendulum.from_format(
                date_string, "MMM D, YYYY"
            ).date()

        df = df.astype(str)

        df["fund_holdings_as_of"] = fund_holdings_as_of_date

        df = df[info_columns]
        df.columns = renamed_columns

        for column, type_var in df_schema:
            if type_var == "float" or type_var == "int":
                df[column] = df[column].astype(str).str.replace(",", "").astype(float)
            else:
                df[column] = df[column].astype(type_var, errors="ignore")

        table_export_name = f"ishare.{type_of_fund}_{fund_ticker}_{fund_holdings_as_of_date.format('YYYY-MM-DD')}"

        df.fillna(pd.NA).to_gbq(
            table_export_name,
            project_id=PROJECT_ID,
            if_exists="replace",
            table_schema=gbq_schema,
        )

        logging.info(f"Exported {table_export_name}.")
        return
    except Exception as e:
        logging.error(f"Issue sending {fund_ticker} to bigquery because {e}")
        return
