import re
from nltk.tokenize import sent_tokenize, word_tokenize
import os
from automated_tasks.LossCost import LossCost, LOSS_COST_DIRECTORY
import datetime
from dateutil.parser import parse

FILES = os.listdir(path=LOSS_COST_DIRECTORY)
file = [
'FL-2018-03.pdf',
'FL-2019-03.pdf',
'ID-2018-01.pdf',
'ID-2018-03.pdf',
'ID-2019-01.pdf',
]


pdf = file[0]
lc = LossCost(pdf_file=pdf)
# lc.generate()

pdf_lines = lc.normalize_pdf()
lae_value = lc.pdf_parser(pdf_lines, keyword=lc.lae_description)
lcm_value = lc.pdf_parser(pdf_lines, keyword=lc.lcm_description)


def pdf_parser(self, pdf_lines: list, keyword: str):
    """
    Used to parse a pdf for a keyword (lae, lcm)

    :param pdf_lines:
    :param keyword:
    :return:
    """
    if keyword is not None:
        keyword = lc.lae_description
        matches = []
        for text in pdf_lines:
            if keyword in text:
                tokenized_text = " ".join(sent_tokenize(text))
                matches.append(tokenized_text)
        for text in matches:
            value = lc._value_extractor(text)
            return value
    else:
        print(f'Keyword {keyword} not found in PDF file')
        return None

