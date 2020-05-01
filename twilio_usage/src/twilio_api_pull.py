import requests
import os
import shutil
import json
import calendar
import pandas as pd
from retrying import retry
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from databases.snowflake.database import SnowFlake
from databases.snowflake.logger import Logger
import databases.snowflake.config as db_config
from databases.utils import utility as u
import argparse

PARSER = argparse.ArgumentParser(
    description="Module for loading twilio usage data to Snowflake",
    prog=""
)

PARSER.add_argument(
    '--start-date',
    help="specify the start date to use in the start_date on the twilio API endpoint in the format YYYY-MM-DD",
    dest='start_dt',
    type=str,
    default=None
)

PARSER.add_argument(
    '--end-date',
    help="specify the end date to use in the end_date on the twilio API endpoint in the format YYYY-MM-DD",
    dest='end_dt',
    type=str,
    default=None
)


PARAMS = (
    ('PageSize', '1000'),
)
HA = 'HA'
AL = 'AL'

shutil.copyfile("/src/passwords/twilio_config.json", "/src/twilio_config.json")
CONFIG = os.path.join(os.getcwd(), 'twilio_config.json')
with open(CONFIG) as c:
    config = json.load(c)

HA_ACCOUNT_SID = config['HA_ACCOUNT_SID']
HA_AUTH_TOKEN = config['HA_AUTH_TOKEN']
AL_ACCOUNT_SID = config['AL_ACCOUNT_SID']
AL_AUTH_TOKEN = config['AL_AUTH_TOKEN']
URL = 'https://api.twilio.com/2010-04-01/Accounts/{}/Usage/Records.json?StartDate={}&EndDate={}'
LAST_MONTH_URL = 'https://api.twilio.com/2010-04-01/Accounts/{}/Usage/Records/LastMonth.json?'
DATABASE = db_config.DATABASE
SCHEMA = 'TWILIO'
TABLE = 'TWILIO_USAGE'
SF = SnowFlake(schema=SCHEMA)


def get_first_last_day(date):
    first_day = str(date.replace(day=1))
    last_day = str(date.replace(day=calendar.monthrange(date.year, date.month)[1]))
    return first_day, last_day


def check_date_range(start, end):
    cur_date = start
    while cur_date < end:
        cur_date += relativedelta(months=1)
    if cur_date - timedelta(days=1) == end:
        return True
    else:
        return False


@retry(stop_max_attempt_number=2, wait_fixed=10000)
def api_pull(start, end, source, url) -> pd.DataFrame:
    """
        Used to get the sp_id from the merchant order id depending on the format
        :param start: start_date passed in to the url
        :param end: start_date passed in to the url
        :param source: AL or HA
        :param url: Endpoint URL
        :return: returns a DataFrame
    """

    print(f'Pulling Data for {start} to {end}')
    auth = (HA_ACCOUNT_SID, HA_AUTH_TOKEN)
    print(f'Requesting data for {url}')
    response = requests.get(url, params=PARAMS, auth=auth)
    if response.ok:
        data = json.loads(response.content)['usage_records']
        df = pd.DataFrame({k: v for k, v in u.flatten(kv)} for kv in data)
        print(f'Dataframe Created')
        df['source'] = source
        df = df[['account_sid', 'source', 'start_date', 'end_date', 'category', 'count', 'count_unit',
                 'description', 'price', 'price_unit', 'usage', 'usage_unit']]
        df['load_datetime'] = datetime.now()
        return df
    else:
        print('No valid API Response')
        print(response.text)


def main(start: date, end: date):
    if check_date_range(start, end):
        url = URL.format(HA_ACCOUNT_SID, start, end)
        ha_df = api_pull(start, end, HA, url)
        al_df = api_pull(start, end, AL, url)
        q = f"delete from {DATABASE}.{SCHEMA}.{TABLE} where start_date = '{start}' and end_date = '{end}'"
        SF.query(q)
        if len(ha_df) > 0:
            SF.upload_dataframe(df=ha_df, table_name=TABLE)
        if len(al_df) > 0:
            SF.upload_dataframe(df=al_df, table_name=TABLE)
        print("Data loaded to SnowFlake")

    elif start == end:
        url = LAST_MONTH_URL.format(HA_ACCOUNT_SID)
        ha_df = api_pull(start, end, HA, url)
        al_df = api_pull(start, end, AL, url)
        if len(ha_df) > 0:
            SF.upload_dataframe(df=ha_df, table_name=TABLE)
        if len(al_df) > 0:
            SF.upload_dataframe(df=al_df, table_name=TABLE)
        print("Data loaded to SnowFlake")

    else:
        raise ValueError("Please use a month date range for backpopulating "
                         "or enter the same the first date of the month to load last month data.")


if __name__ == "__main__":
    with Logger(schema=SCHEMA) as _:
        ARGS = PARSER.parse_args()
        start_date = datetime.strptime(ARGS.start_dt, '%Y-%m-%d').date()
        end_date = datetime.strptime(ARGS.end_dt, '%Y-%m-%d').date()
        main(start_date, end_date)
