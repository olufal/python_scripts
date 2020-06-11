import pandas as pd
from strive.note import CAMNotes
from azure.storage.blob import BlockBlobService
from datetime import datetime
import dask.dataframe as dd

# Define parameters
storageAccountName = "devinternalanalyticssa"
storageKey         = "V0asRZHya4Gutd4jQpmBvtw4l9bDKL5CiJs3c4d7qTtn0AJ60JiVQT4imW/hFNXRQSEYvK+9mKWme+jpIWC5mg=="
containerName      = "blobstage"


# Establish connection with the blob storage account
blobService = BlockBlobService(account_name=storageAccountName,
                               account_key=storageKey
                               )
batchAccount = 'deviabatch'
batchURL = 'https://deviabatch.eastus.batch.azure.com'
primaryAccessKey = '/MTRr7/uq9BfvO8mGVUMB1QhrymlfntGVyp+1z/idSitCJ6265xAjMlqV8V+axrQeblhlLRVlSLiPMqDpIm/zQ=='
blobDirectory = 'bullhorn/python'

def etl(input_file):
    raw_data = pd.read_csv(input_file)
    df = raw_data['comments']
    now = datetime.now().strftime("%Y%m%d%H%M%S")
    output_file = f'parsed_note_{now}.csv'
    df.to_csv(output_file)
    blobService.create_blob_from_text(containerName, output_file, output_file)


def main(extract_file: str):
    raw_data = pd.read_csv(extract_file)
    parsed_df = pd.DataFrame()

    for i, row in raw_data.iterrows():
        note = CAMNotes(note_id=row['noteid'],
                        comments=row['comments'],
                        person_id=row['commentingpersonid'],
                        create_datetime=row['dateadded'],
                        action=row['action'])
        note.generate()
        parsed_df = parsed_df.append(note.note_df, ignore_index=True, sort=False)

    now = datetime.now().strftime("%Y%m%d%H%M%S")
    parsed_file = f'parsed_note_{now}.csv'
    parsed_df.to_csv(parsed_file, index=False)

    # Upload iris dataset
    blobService.create_blob_from_text(containerName, parsed_file, parsed_file)


if __name__ == '__main__':
    # file_path = r'C:\out\ClientMeetingNotes_test.csv'
    # df = pd.read_csv(file_path, sep='\t')
    # main(file_path)
    input_file = 'ClientMeetingNotes.csv'
    etl(input_file)
