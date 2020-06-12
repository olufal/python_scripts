"""
Author: John Falope
"""

import pandas as pd
import os
import re
import numpy as np
from nltk.tokenize import sent_tokenize
from dateutil.parser import parse
from datetime import datetime, date
from azure.storage.blob import BlockBlobService

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

    def generate(self):
        """
        Main driver method for the cam note
        """

        try:
            self.cam_date = self.get_note_date()
            self.attendees = self.get_attendees(self.responses[1])

            self.description = self.responses[2]
            self.purpose = self.responses[3]
            self.updates = self.responses[4]
            self.opportunities = self.responses[5]
            self.personal_info = self.responses[6]
            self.next_steps = self.responses[7]

            for attendee in self.attendees:
                if len(attendee) > 0:
                    df_data = {'note_id': [self.note_id],
                               'note_datetime': [self.note_datetime],
                               'cam_date': [self.cam_date],
                               'action': [self.action],
                               'person_id': [self.person_id],
                               'attendee': [attendee],
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
    input_file = r'C:\DATA_ENGINEERING\misc\ClientMeetingNotes_test.csv'
    test = pd.read_csv(input_file, sep='\t')

    df = parser(input_file)

