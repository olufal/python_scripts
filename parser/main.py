import pandas as pd
from strive.note_parser import CAMNotes
from azure.storage.blob import BlockBlobService
from datetime import datetime

# Define parameters
storageAccountName = "devinternalanalyticssa"
storageKey         = "V0asRZHya4Gutd4jQpmBvtw4l9bDKL5CiJs3c4d7qTtn0AJ60JiVQT4imW/hFNXRQSEYvK+9mKWme+jpIWC5mg=="
containerName      = "blobstage"
blobDirectory      = "bullhorn/python"

# Establish connection with the blob storage account
blobService = BlockBlobService(account_name=storageAccountName,
                               account_key=storageKey
                               )
batchAccount = 'deviabatch'
batchURL = 'https://deviabatch.eastus.batch.azure.com'
primaryAccessKey = '/MTRr7/uq9BfvO8mGVUMB1QhrymlfntGVyp+1z/idSitCJ6265xAjMlqV8V+axrQeblhlLRVlSLiPMqDpIm/zQ=='


def parser(input_file: str):
    raw_data = pd.read_csv(input_file, sep='\t')
    parsed_df = pd.DataFrame()

    for i, row in raw_data.iterrows():
        note = CAMNotes(note_id=row['noteid'],
                        comments=row['comments'],
                        person_id=row['commentingpersonid'],
                        cam_datetime=row['dateadded'],
                        action=row['action'])
        note.generate()
        parsed_df = parsed_df.append(note.note_df, ignore_index=True, sort=False)

    now = datetime.now().strftime("%Y%m%d%H%M%S")
    parsed_file = f'parsed_note_{now}.csv'
    parsed_df.to_csv(parsed_file, index=False)

    # Upload dataset to blobstage
    blobService.create_blob_from_path(containerName, f'{blobDirectory}/{parsed_file}', parsed_file)

    return parsed_df



if __name__ == '__main__':
    input_file = r'C:\out\ClientMeetingNotes_test.csv'
    df = parser(input_file)
