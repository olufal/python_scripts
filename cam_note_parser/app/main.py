"""
Author: John Falope
"""
import os
import pandas as pd
from datetime import datetime, date, timedelta
from Strive.cam_note_parser.parser.database import SnowFlake
from Strive.cam_note_parser.parser.database import DATABASE, SCHEMA, TABLE
from azure.storage.blob import BlobClient
from Strive.cam_note_parser.parser.parser import CAMNotes

DIRECTORY = os.getcwd()
# DIRECTORY = 'C:\Consulting\Strive'

USER = 'IA_APP_USER'
PASSWORD = 'JWhtV1Ios8T2'
S = SnowFlake(user=USER, password=PASSWORD)


# Define blob parameters
storageAccountName = "devinternalanalyticssa"
storageKey = "V0asRZHya4Gutd4jQpmBvtw4l9bDKL5CiJs3c4d7qTtn0AJ60JiVQT4imW/hFNXRQSEYvK+9mKWme+jpIWC5mg=="
containerName = "blobstage"
blobDirectory = "bullhorn/python"

# Establish connection with the blob storage account
connection_string = f"DefaultEndpointsProtocol=https;AccountName={storageAccountName};AccountKey={storageKey};EndpointSuffix=core.windows.net"


def upload_file_to_Snowflake(filename: str, df: pd.DataFrame, table_name: str) -> None:
    """
    Upload the file from blob storage to Table using

    :param filename: Fully qualified file name
    :param df: Data frame to be uploaded
    :param table_name: The table name in Snowflake
    """

    stage_name = 'adf_blob_stage'
    full_stage_name = f"{DATABASE}.{SCHEMA}.{stage_name}"
    file_format = r"( COMPRESSION = 'AUTO' FIELD_DELIMITER = ',' RECORD_DELIMITER = '\n' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\042' TRIM_SPACE = TRUE ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE ESCAPE = 'NONE' ESCAPE_UNENCLOSED_FIELD = '\134' DATE_FORMAT = 'AUTO' TIMESTAMP_FORMAT = 'AUTO' NULL_IF = ('', 'NA', 'null', 'NULL')) "
    full_target_table_name = f"{DATABASE}.{SCHEMA}.{table_name}"
    col_list = ",".join(list(df.columns))
    formatted_query = f"""copy into {full_target_table_name} ({col_list})
                        from @{full_stage_name}/{blobDirectory}/{filename} file_format = {file_format}"""
    S.query(formatted_query)
    print(f"Data has moved from stage {stage_name} into target table {full_target_table_name}")


def upload_blob_to_storage(blob_file: str):
    """
    Upload the file in process to the blob storage

    :param blob_file: blob file name
    """

    # Upload dataset to blobstage
    blob_name = f'{blobDirectory}/{blob_file}'
    blobService = BlobClient.from_connection_string(conn_str=connection_string,
                                                    container_name=containerName,
                                                    blob_name=blob_name)
    with open(blob_file, "rb") as data:
        blobService.upload_blob(data)

    print(f'Parsed file: {input_file} and moved {blob_file} to blob storage {blobDirectory}')


def parser(input_file: str):
    raw_data = pd.read_csv(input_file, sep='^', lineterminator='~')
    parsed_df = pd.DataFrame()

    for i, row in raw_data.iterrows():
        note = CAMNotes(note_id=row['noteid'],
                        comments=row['comments'],
                        person_id=row['commentingpersonid'],
                        note_datetime=row['dateadded'],
                        action=row['action'])
        note.generate()
        parsed_df = parsed_df.append(note.note_df, ignore_index=True, sort=False)

    return parsed_df


def load_data(df: pd.DataFrame):
    now = datetime.now().strftime("%Y%m%d%H%M%S")
    parsed_file = f'parsed_note_{now}.csv'
    df.to_csv(parsed_file, index=False)

    upload_blob_to_storage(parsed_file)

    upload_file_to_Snowflake(parsed_file, df=df, table_name=TABLE)


if __name__ == '__main__':

    input_file = r'ClientMeetingNotes.csv'
    file_path = os.path.join(DIRECTORY, input_file)

    df = parser(file_path)
    load_data(df)
