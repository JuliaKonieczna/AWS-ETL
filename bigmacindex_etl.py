import requests
import quandl
import os
import pandas as pd
import boto3
from loguru import logger


FILE_NAME = 'data.csv'
COUNTRY_CODES_FILE = 'economist_country_codes.csv'
DOWNLOAD_COUNTRY_CODES_URL = 'https://static.quandl.com/ECONOMIST_Descriptions/economist_country_codes.csv'
S3_BUCKET_NAME = 'juliak-big-mac-index'
quandl.ApiConfig.api_key = 'GWQfKHqq264tB8ks_W1s'
s3 = boto3.resource('s3')


def check_if_file_exists():
    if not os.path.exists(COUNTRY_CODES_FILE):
        download_country_codes()
    else:
        logger.info("File with country codes already exists, skipping download...")


def download_country_codes():
    logger.info("Downloading file with country codes...")
    response = requests.get(DOWNLOAD_COUNTRY_CODES_URL)
    with open(COUNTRY_CODES_FILE, "wb") as f:
        f.write(response.content)
    logger.info("File downloaded")


def get_countries_and_codes():
    df = pd.read_csv(COUNTRY_CODES_FILE, encoding="UTF-8", sep="|")
    return df


def fetch_data(country_code: str):
    df = quandl.get(f'ECONOMIST/BIGMAC_{country_code}', start_date='2021-07-31', end_date='2021-07-31')
    df = df.applymap(lambda x: str(x).replace('.', ','))
    df["CODE"] = country_code
    df = df.set_index("CODE")
    return df


def upload_data_to_s3(big_mac_index_data: pd.DataFrame):
    csv_buffer = big_mac_index_data.to_csv(sep=';', encoding='UTF-8', index=False).encode()
    logger.info("Uploading data to S3...")
    s3.Bucket(S3_BUCKET_NAME).put_object(Key='big_mac_index.csv', Body=csv_buffer)
    logger.info("Data uploaded")


def main():
    check_if_file_exists()
    countries_and_codes = get_countries_and_codes()
    logger.info("Fetching data for countries...")
    countries_data = pd.DataFrame()
    for country_code in countries_and_codes.CODE:
        countries_data = pd.concat((countries_data, fetch_data(country_code=country_code)))
    logger.info("Data fetched")
    big_mac_index_data = countries_and_codes.merge(countries_data, left_on='CODE', right_on='CODE')
    upload_data_to_s3(big_mac_index_data=big_mac_index_data)
    logger.info("Done")


if __name__ == '__main__':
    main()




