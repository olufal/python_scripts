"""
Author: John Falope
"""
import os
from Strive.cam_note_parser.app.main import parser
import pandas as pd


input_file = r'ClientMeetingNotes.csv'
file_path = os.path.join(r"C:\Consulting\Strive", input_file)
raw_data = pd.read_csv(file_path, sep='^', lineterminator='~')

df = parser(input_file=file_path)

