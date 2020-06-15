"""
Author: John Falope
"""

import pandas as pd
import snowflake.sqlalchemy as sf_sqlalchemy
import sqlalchemy
import re
import numpy as np
from dateutil.parser import parse
from datetime import datetime, date
from azure.storage.blob import BlockBlobService
import probablepeople


# Define blob parameters
storageAccountName = "devinternalanalyticssa"
storageKey = "V0asRZHya4Gutd4jQpmBvtw4l9bDKL5CiJs3c4d7qTtn0AJ60JiVQT4imW/hFNXRQSEYvK+9mKWme+jpIWC5mg=="
containerName = "blobstage"
blobDirectory = "bullhorn/python"

# Establish connection with the blob storage account
blobService = BlockBlobService(account_name=storageAccountName,
                               account_key=storageKey
                               )
batchAccount = 'deviabatch'
batchURL = 'https://deviabatch.eastus.batch.azure.com'
primaryAccessKey = '/MTRr7/uq9BfvO8mGVUMB1QhrymlfntGVyp+1z/idSitCJ6265xAjMlqV8V+axrQeblhlLRVlSLiPMqDpIm/zQ=='


# Snowflake connection parameters
USER = 'JFALOPE'
PASSWORD = 'Shoot@34'
ACCOUNT = 'strivepartner.east-us-2.azure'
WAREHOUSE = 'ETL_WH_DEV'
DATABASE = 'IA_RAW_DEV'
SCHEMA = 'BULLHORN'
ROLE = 'SYSADMIN'
TABLE = 'CAM_NOTE_DETAIL'

# builds the snowflake connection url
connection_url = sf_sqlalchemy.URL(
    account=ACCOUNT,
    user=USER,
    password=PASSWORD,
    database=DATABASE,
    schema=SCHEMA,
    warehouse=WAREHOUSE,
    role=ROLE,
)


class SnowFlakeEngine:
    def __init__(self, connection_url: str, insecure_mode: bool = False):
        self.connection_url = connection_url
        self.insecure_mode = insecure_mode

    def __enter__(self):
        self.engine = sqlalchemy.create_engine(
            self.connection_url, connect_args={
                "insecure_mode": self.insecure_mode})
        self.engine.execution_options(isolation_level="AUTOCOMMIT")
        force_auto_commit = 'ALTER SESSION SET AUTOCOMMIT = TRUE'
        pd.read_sql_query(force_auto_commit, self.engine)
        return self.engine

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.engine.dispose()


def query(sql: str) -> pd.DataFrame:
    """
    Execute a sql query with the current SnowFlake engine

    :param sql: SQL query to execute
    :returns: result of query as a Pandas DataFrame
    """
    if sql is None:
        raise ValueError("sql param must be specified to run a query")
    with SnowFlakeEngine(connection_url) as engine:
        try:
            # print(f"Executing query: {sql}")
            result = pd.read_sql_query(sql, engine)
            # print(f"Got query result with shape: {result.shape}")
            # print(f"Sample of 5: {result.head(5)}")
        except Exception as query_exception:
            print(f"error executing query - {sql}")
            raise query_exception from None
        return result


def upload_file(filename: str, df: pd.DataFrame, table_name: str) -> None:
    """
    Upload the file from blob storage to Table using
    :param filename: Fully qualified file name
    :param df: Data frame to be uploaded
    :param table_name: The table name in Snowflake
    :return: None just logs the put and call code
    """
    stage_name = 'adf_blob_stage'
    full_stage_name = f"{DATABASE}.{SCHEMA}.{stage_name}"
    file_format = r"( COMPRESSION = 'AUTO' FIELD_DELIMITER = ',' RECORD_DELIMITER = '\n' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\042' TRIM_SPACE = TRUE ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE ESCAPE = 'NONE' ESCAPE_UNENCLOSED_FIELD = '\134' DATE_FORMAT = 'AUTO' TIMESTAMP_FORMAT = 'AUTO' NULL_IF = ('', 'NA', 'null', 'NULL')) "
    full_target_table_name = f"{DATABASE}.{SCHEMA}.{table_name}"
    col_list = ",".join(list(df.columns))
    formatted_query = f"""copy into {full_target_table_name} ({col_list})
                        from @{full_stage_name}/{blobDirectory}/{filename} file_format = {file_format}"""
    query(formatted_query)
    print(
        f"Data has moved from stage {stage_name} into target table {full_target_table_name}"
    )

class CAMNotes:
    """
        CAM note class
    """
    def __init__(
            self,
            note_id: str,
            comments: str,
            note_datetime: datetime,
            action: str,
            person_id: str
    ):
        """
            Constructor Bullhorn Note
            :param note_id:
            :param comments:
            :param note_datetime:
            :param action:
            :param person_id:
        """
        self.note_id = note_id
        self.person_id = person_id
        self.comments = comments
        self.note_datetime = note_datetime
        self.action = action
        self.prompts = None
        self.responses = None
        self.cam_date = None
        self.attendees = []
        self.description = None
        self.purpose = None
        self.updates = None
        self.opportunities = None
        self.personal_info = None
        self.next_steps = None
        self.note_df = pd.DataFrame()
        self.attendee_match_fg = None
        self.normalize_text()

    def normalize_text(self):
        """
        Used to parse the note into different prompts
        """

        prompts = self.comments.split('|')
        self.prompts = [line.replace('\n', '') for line in prompts if
                   line.isspace() == False and line != '\n' and len(line) != 0]
        # prompts = [clean_text(line) for line in prompts]
        self.responses = [self.extract_response(line) for line in self.prompts]

    def extract_response(self, text: str):
        """
        method that extracts the response from each prompt block
        :param text:
        :return: the responses
        """
        sep = ':'
        before, sep, after = text.partition(sep)

        return after

    def contains_date(self, string: str, fuzzy=True):
        """
        Return whether the string can be interpreted as a date.
        :param string: str, string to check for date
        :param fuzzy: bool, ignore unknown tokens in string if True
        """
        try:
            parse(string, fuzzy=fuzzy)
            return True
        except ValueError:
            return False

    def get_note_date(self):
        """
        Used to get the meeting date from the comment
        :return: the parsed date
        """
        for string in self.prompts:
            if self.contains_date(string):
                correct_dt = parse(string, fuzzy=True)
                correct_dt = correct_dt.date()
                if isinstance(correct_dt, date):
                    return correct_dt

    def get_attendees(self, response_text: str):
        """
        Used to parse the attendees prompt and grab each attendee and puts them in a list
        :param response_text:
        :return: list of attendees
        """

        pattern = re.compile(r'[^a-zA-Z ]+')
        str = re.sub(pattern, '|', response_text)
        names = [name for name in str.split('| ') if len(name) > 0]
        # tokenized_names = [sent_tokenize(name) for name in names if len(name) > 0 and 'insert' not in name]
        return names

    def match_attendee(self, name):
        attendee_query = f"""select employee_name_last, employee_name_first, employee_name_full
        from ia_dw_dev.dw.dim_employee
        where employee_name_full = '{name}'
        """
        print(f'checking database for attendee: {name}')
        name_df = query(attendee_query)

        if len(name_df) > 0:
            print(f'attendee name found.')
            print(name_df)
            self.attendee_match_fg = True
        else:
            print(f'attendee name not found.')
            self.attendee_match_fg = False

    def generate(self):
        """
        Main driver method for the cam note
        """

        try:
            self.cam_date = self.get_note_date()
            self.attendees = self.get_attendees(self.responses[1])

            self.description = self.responses[2].replace('\r', '')
            self.purpose = self.responses[3].replace('\r', '')
            self.updates = self.responses[4].replace('\r', '')
            self.opportunities = self.responses[5].replace('\r', '')
            self.personal_info = self.responses[6].replace('\r', '')
            self.next_steps = self.responses[7].replace('\r', '')

            for attendee_text in self.attendees:
                if len(attendee_text) > 0:
                    name = probablepeople.tag(attendee_text)
                    first_name = name[0]['GivenName']
                    last_name = name[0]['Surname']
                    attendee = f'{first_name} {last_name}'

                    # checks dim_employee table for attendee full name
                    self.match_attendee(attendee)

                    df_data = {'note_id': [self.note_id],
                               'note_datetime': [self.note_datetime],
                               'cam_date': [self.cam_date],
                               'action': [self.action],
                               'commenting_person_id': [self.person_id],
                               'attendee': [attendee],
                               'attendee_match_fg': [self.attendee_match_fg],
                               'description': [self.description],
                               'purpose': [self.purpose],
                               'updates': [self.updates],
                               'opportunities': [self.opportunities],
                               'personal_info': [self.personal_info],
                               'next_steps': [self.next_steps]
                               }
                    df = pd.DataFrame(df_data)
                    df['parse_datetime'] = datetime.now()
                    df.columns = df.columns.str.upper()
                    self.note_df = self.note_df.append(df, sort=False, ignore_index=True)
                    self.note_df = self.note_df.replace('<< insert text here >>', np.nan)

        except Exception as e:
            print(e)
        finally:
            print(f'Process completed for note id: {self.note_id}')
        print()


def parser(input_file: str):
    raw_data = pd.read_csv(input_file, sep='\t')
    parsed_df = pd.DataFrame()

    for i, row in raw_data.iterrows():
        note = CAMNotes(note_id=row['noteid'],
                        comments=row['comments'],
                        person_id=row['commentingpersonid'],
                        note_datetime=row['dateadded'],
                        action=row['action'])
        note.generate()
        parsed_df = parsed_df.append(note.note_df, ignore_index=True, sort=False)

    now = datetime.now().strftime("%Y%m%d%H%M%S")
    parsed_file = f'parsed_note_{now}.csv'
    parsed_df.to_csv(parsed_file, index=False)

    # Upload dataset to blobstage
    blobService.create_blob_from_path(containerName, f'{blobDirectory}/{parsed_file}', parsed_file)
    print(f'Parsed file: {input_file} and moved {parsed_file} to blob storage {blobDirectory}')

    upload_file(parsed_file, df=parsed_df, table_name=TABLE)

    return parsed_df


if __name__ == '__main__':
    # input_file = r'C:\DATA_ENGINEERING\misc\ClientMeetingNotes_test.csv'
    input_file = r'C:\out\ClientMeetingNotes_test.csv'
    test = pd.read_csv(input_file, sep='\t')

    df = parser(input_file)
    schema = pd.io.sql.get_schema(df, TABLE)
