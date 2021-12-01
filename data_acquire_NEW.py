import time
import sched
import pandas
import logging
import requests
from io import StringIO

import utils

DOWNLOAD_URL = "https://data.cdc.gov/api/views/9mfq-cb36/rows.csv?accessType=DOWNLOAD"
MAX_DOWNLOAD_ATTEMPT = 5
DOWNLOAD_PERIOD = 10         # second
logger = logging.Logger(__name__)
utils.setup_logger(logger, 'data.log')


def download_csv(url=DOWNLOAD_URL, retries=MAX_DOWNLOAD_ATTEMPT):
    """Downloads the csv file from the web.
    """
    text = None
    for i in range(retries):
        try:
            print("try ", retries)
            req = requests.get(url, timeout=30.0)
            req.raise_for_status()
            text = req.text
        except requests.exceptions.HTTPError as e:
            #logger.warning("Retry on HTTP Error: {}".format(e))
            print("Retry on HTTP Error")
    if text is None:
        print('download_csv too many FAILED attempts')
        logger.error('download_csv too many FAILED attempts')

    df = pandas.read_csv(StringIO(text), delimiter='\t')
    df.to_csv('downloaddata.csv')


def filter_csv(csv):
    """Converts `text` to `DataFrame`, removes empty lines and descriptions
    """
    print("1")
    
    print("2")
    
    print("3")
    #return df


# def filter_bpa(text):
#     """Converts `text` to `DataFrame`, removes empty lines and descriptions
#     """
#     # use StringIO to convert string to a readable buffer
#     df = pandas.read_csv(StringIO(text), skiprows=11, delimiter='\t')
#     df.columns = df.columns.str.strip()             # remove space in columns name
#     df['Datetime'] = pandas.to_datetime(df['Date/Time'])
#     df.drop(columns=['Date/Time'], axis=1, inplace=True)
#     df.dropna(inplace=True)             # drop rows with empty cells
#     return df


def update_once():
    csv = download_csv()
    df = filter_csv(csv)
    # upsert_bpa(df)


def main_loop(timeout=DOWNLOAD_PERIOD):
    scheduler = sched.scheduler(time.time, time.sleep)

    def _worker():
        try:
            update_once()
        except Exception as e:
            logger.warning("main loop worker ignores exception and continues: {}".format(e))
        scheduler.enter(timeout, 1, _worker)    # schedule the next event

    scheduler.enter(0, 1, _worker)              # start the first event
    scheduler.run(blocking=True)


if __name__ == '__main__':
    #main_loop()
    download_csv()


