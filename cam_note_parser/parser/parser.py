"""
Author: John Falope
"""

import pandas as pd
import re
import numpy as np
from dateutil.parser import parse
from datetime import datetime, date, timedelta
import probablepeople
from Strive.cam_note_parser.parser.database import SnowFlake
import nltk


USER = 'IA_APP_USER'
PASSWORD = 'JWhtV1Ios8T2'

S = SnowFlake(user=USER, password=PASSWORD)


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
        self.is_valid_template = True if '|' in self.comments else False
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
        self.attendee_match_fg = False

    def normalize_text(self):
        """
        Used to parse the note into different prompts
        """

        prompts = self.comments.split('|')
        self.prompts = [line.replace('\n', '') for line in prompts if
                   line.isspace() == False and line != '\n' and len(line) != 0]
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
        Uses NLTK to parse the attendees prompt and grab each attendee and puts them in a list
        :param response_text:
        :return: list of attendees
        """

        tokens = nltk.tokenize.word_tokenize(response_text)
        pos = nltk.pos_tag(tokens)
        sentt = nltk.ne_chunk(pos, binary=False)
        person_list = []
        person = []
        name = ""
        for subtree in sentt.subtrees(filter=lambda t: t.label() == 'PERSON'):
            for leaf in subtree.leaves():
                person.append(leaf[0])
            if len(person) > 1:  # avoid grabbing lone surnames
                for part in person:
                    name += part + ' '
                if name[:-1] not in person_list:
                    person_list.append(name[:-1])
                name = ''
            person = []

        return person_list

    def match_attendee(self, name):
        """
        Used to search attendee full name in employee table
        :param name:
        """

        attendee_query = f"""select distinct employee_name_last, employee_name_first, employee_name_full
        from ia_dw_dev.dw.dim_employee
        where employee_name_full = '{name}'
        """
        print(f'checking database for attendee: {name}')
        name_df = S.query(attendee_query)

        self.attendee_match_fg = True if len(name_df) > 0 else False

        if len(name_df) > 0:
            print(f'attendee name found.')
            print(name_df)
            self.attendee_match_fg = True
        else:
            print(f'attendee name not found.')
            self.attendee_match_fg = False

    def record(self, attendee):
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
        return df

    def generate(self):
        """
        Main driver method for the cam note
        """
        if self.is_valid_template == True:
            self.normalize_text()

            self.cam_date = self.get_note_date()
            self.attendees = self.get_attendees(self.responses[1])

            self.description = self.responses[2]
            self.purpose = self.responses[3]
            self.updates = self.responses[4]
            self.opportunities = self.responses[5]
            self.personal_info = self.responses[6]
            self.next_steps = self.responses[7]
            try:

                if len(self.attendees) == 0:
                    attendee = None
                    print('No CAM Attendees in the note.')
                    record_df = self.record(attendee)
                    self.note_df = self.note_df.append(record_df, sort=False, ignore_index=True)
                    self.note_df = self.note_df.replace('<< insert text here >>', np.nan)


                else:
                    for attendee_text in self.attendees:
                        name = probablepeople.tag(attendee_text)
                        if len(name[0]) == 0:
                            attendee = None
                            print('No CAM Attendees in the note.')
                        else:
                            first_name = name[0]['GivenName']
                            last_name = name[0]['Surname']
                            attendee = f'{first_name} {last_name}'

                            # checks dim_employee table for attendee full name
                            self.match_attendee(attendee)

                        record_df = self.record(attendee)

                        self.note_df = self.note_df.append(record_df, sort=False, ignore_index=True)
                        self.note_df = self.note_df.replace('<< insert text here >>', np.nan)

            except Exception as e:
                print(e)
            finally:
                print(f'Process completed for note id: {self.note_id}')
            print()

        else:
            print(f'Invalid template for note_id: {self.note_id}')


