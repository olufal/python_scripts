import pandas as pd
from data_engineering.parser.note import CAMNotes
from azure.storage.blob import BlockBlobService
from datetime import datetime

# Define parameters
storageAccountName = "<storage-account-name>"
storageKey         = "<storage-account-key>"
containerName      = "blobstorage"

# Establish connection with the blob storage account
blobService = BlockBlobService(account_name=storageAccountName,
                               account_key=storageKey
                               )

def main(extract_file: str):
    raw_data = pd.read_csv(extract_file)
    parsed_df = pd.DataFrame()

    for i, row in raw_data.iterrows():
        note = CAMNotes(note_id=row['note_id'],
                        note_text=row['note_text'],
                        create_datetime=row['note_date'],
                        action=row['note_action'])
        note.generate()
        parsed_df = parsed_df.append(note.note_df, ignore_index=True, sort=False)

    now = datetime.now().strftime("%Y%m%d%H%M%S")
    parsed_file = f'parsed_note_{now}.csv'
    parsed_df.to_csv(parsed_file, index=False)

    # Upload iris dataset
    blobService.create_blob_from_text(containerName, parsed_file, parsed_file)


if __name__ == '__main__':
    file_path = r'C:\out\test.csv'
    main(file_path)