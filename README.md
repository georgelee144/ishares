# ishares

This is a airflow DAG that will that will gather information on what the underlying assets of many ishare ETFs.

It will make a request to get a .csv file for each ETF listed in the constants.py file and save only the info we care about.
ETF list is all ishare ETFs that do not have a target end date as of 24 Feb 2023.
Each csv file is saved to Google Cloud Storage.
Each csv is then read to processed to be stored to Google BigQuery.


Ignored other as many of them don't contain anything useful

Google Cloud Storage doesn't really allow for async read calls
