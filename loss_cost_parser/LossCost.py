"""
Author: John Oluwaseun Falope
Loss Cost PDF class that helps parses a loss cost or rate filing pdf for different Bureaus
"""

import pandas as pd
import os
import re
from tika import parser
from nltk.tokenize import sent_tokenize
from dateutil.parser import parse
from datetime import datetime as dt
import datetime

LOSS_COST_DIRECTORY = r'\\uter\services\external\Class_Codes\LAE Raw pdf\Archived Circulars'
LAE_FILE = 'lae_raw.xlsx'
LAE_LCM_STATUS_FILE = 'LAE_LCM_StatusByState.xls'

class LossCost:
    """
        Loss cost class
    """
    def __init__(
            self,
            pdf_file: str
    ):
        """
            Constructor for a Loss Cost event
            :param pdf_file:
            :param create_datetime:
            :param bureau:
        """
        self.pdf_file = pdf_file
        self.pdf_lines = None
        self.create_datetime = dt.now()
        self.effective_date = None
        self.state_code = None
        self.bureau = None
        self.file_path = os.path.join(LOSS_COST_DIRECTORY, pdf_file)
        self.lae_description = None
        self.lae_value = None
        self.lcm_description = None
        self.lcm_value = None
        self.format = None
        self.get_attributes()
        self.loss_cost_df = None

    def get_attributes(self):
        def_df = pd.read_excel(os.path.join(r'\\uter\services\external\Class_Codes\LAE Raw pdf', LAE_FILE))

        lae_status_df = pd.read_excel(os.path.join(r'C:\out', LAE_LCM_STATUS_FILE))
        lae_status_df = lae_status_df[['state_code', 'lae_label', 'lcm_label']]

        df_merge = pd.merge(def_df, lae_status_df, on='state_code')
        lae_data = [(row['state_code'], row['lae_label'], row['lcm_label'], row['bureau']) for index, row in
                    df_merge.iterrows()]
        lae_data = list(set(lae_data))
        lae_data.sort()
        for state_code, lae_keyword, lcm_keyword, bureau in lae_data:
            if state_code.upper() == self.pdf_file[:2].upper():
                self.state_code = state_code
                self.bureau = bureau
                self.format = bureau
                self.lae_description = str(lae_keyword).replace(' (proposed)', '')
                if self.lae_description == 'nan':
                    self.lae_description = None
                self.lcm_description = str(lcm_keyword).replace(' (proposed)', '')
                if self.lcm_description == 'nan':
                    self.lcm_description = None

    def normalize_pdf(self):
        """
        Used to parse a pdf for a keyword

        :param pdf:
        :param format:
        :return: list of lines
        """
        raw = parser.from_file(self.file_path)
        text = raw['content']
        line_split = '\n'

        lines = text.split(f"{line_split}")
        lines = [line.replace('\n', '') for line in lines if line.isspace() == False and line != '\n' and len(line) != 0]
        lines = [line.replace(u'\xa0', u' ') for line in lines]

        return lines

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

    def get_effective_date(self, pdf_lines: list, format: str):
        word_list = []
        for i, line in enumerate(pdf_lines):
            if format == 'NCCI':
                if 'Proposed Effective' in line and len(line.lstrip()) <= 20:
                    index = i + 1
                    word_list.append(pdf_lines[index])
                elif 'Proposed Effective' in line or 'proposed to be effective' in line:
                    word_list.append(line)
            elif format == 'WCIRB':
                if 'Effective' in line:
                    tokenized_text = sent_tokenize(line)[0]
                    word_list.append(tokenized_text)
            elif format == 'NYCIRB':
                if 'effective on' in line:
                    tokenized_text = sent_tokenize(line)[0]
                    word_list.append(tokenized_text)
            else:
                if 'Effective' in line or 'effective on' in line:
                    tokenized_text = sent_tokenize(line)[0]
                    word_list.append(tokenized_text)

        for string in word_list:
            if self.contains_date(string):
                correct_dt = parse(string, fuzzy=True)
                correct_dt = correct_dt.date()
                if isinstance(correct_dt, datetime.date):
                    return correct_dt

    def _string_slicer(self, text: str, keyword: str):
        index = text.find(keyword)
        if index != -1:
            return text[index:]
        else:
            raise Exception('Sub string not found!')

    def pdf_parser(self, pdf_lines: list, keyword: str):
        """
        Used to parse a pdf for a keyword

        :param pdf_lines:
        :param keyword:
        :return:
        """
        if keyword is not None:
            matches = []
            for text in pdf_lines:
                if keyword in text:
                    tokenized_text = " ".join(sent_tokenize(text))
                    print(tokenized_text)
                    matches.append(tokenized_text)
            for text in matches:
                value = self._value_extractor(text)
                return value
        else:
            print(f'Keyword {keyword} not found in PDF file')
            return None

    def _value_extractor(self, text: str) -> float:
        try:
            value = re.findall('\d+\.\d+', text)
            if len(value) != 0:
                if len(value) > 1:
                    value = value[1]
                elif len(value) == 1:
                    value = value[0]
                return float(value)
        except ValueError as v:
            print(v)


    def generate(self):
        try:
            print(f'parsing PDF File {self.pdf_file}')
            self.pdf_lines = self.normalize_pdf()
            self.effective_date = self.get_effective_date(self.pdf_lines, format=self.format)
            print(f'Effective Date is {self.effective_date}')

            self.lae_value = self.pdf_parser(self.pdf_lines, self.lae_description)
            if self.lae_value is not None:
                if (self.lae_value/100) <= 1 and (self.lae_value/100) >= 0.1:
                    self.lae_value = self.lae_value/100

            self.lcm_value = self.pdf_parser(self.pdf_lines, self.lcm_description)
            if self.lcm_value is not None:
                if (self.lcm_value/100) <= 1 and (self.lcm_value/100) >= 0.1:
                    self.lcm_value = self.lcm_value/100

            df_data = {'state_code': [self.state_code],
                       'bureau': [self.bureau],
                       'effective_date': [self.effective_date],
                       'lae_description': [self.lae_description],
                       'lae': [self.lae_value],
                       'lcm_description': [self.lcm_description],
                       'lcm': [self.lcm_value],
                       'source_file': [self.pdf_file]
                       }
            self.loss_cost_df = pd.DataFrame(df_data)
            self.loss_cost_df['create_datetime'] = dt.now()
        except Exception as e:
            print(e)
        finally:
            print(f'Process completed for {self.pdf_file}')
        print()