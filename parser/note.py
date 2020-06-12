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


class CAMNotes:
    """
        CAM note class
    """
    def __init__(
            self,
            note_id: str,
            comments: str,
            cam_datetime: datetime,
            action: str,
            person_id: str
    ):
        """
            Constructor Bullhorn Note
            :param note_text:
            :param create_datetime:
            :param action:
        """
        self.note_id = note_id
        self.person_id = person_id
        self.comments = comments
        self.cam_datetime = cam_datetime
        self.action = action
        self.prompts = None
        self.responses = None
        self.note_date = None
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
        :param text:
        :return: list of answers
        """

        prompts = self.comments.split('|')
        self.prompts = [line.replace('\n', '') for line in prompts if
                   line.isspace() == False and line != '\n' and len(line) != 0]
        # prompts = [clean_text(line) for line in prompts]
        self.responses = [self.extract_response(line) for line in self.prompts]

    def extract_response(self, text: str):
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
        for string in self.prompts:
            if self.contains_date(string):
                correct_dt = parse(string, fuzzy=True)
                correct_dt = correct_dt.date()
                if isinstance(correct_dt, date):
                    return correct_dt

    def get_attendees(self, response_text: str):
        pattern = re.compile(r'[^a-zA-Z ]+')
        str = re.sub(pattern, '|', response_text)
        names = [name for name in str.split('| ') if len(name) > 0]
        # tokenized_names = [sent_tokenize(name) for name in names if len(name) > 0 and 'insert' not in name]
        return names

    def generate(self):
        try:
            self.note_date = self.get_note_date()
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
                               'note_date': [self.note_date],
                               'cam_datetime': [self.cam_datetime],
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
