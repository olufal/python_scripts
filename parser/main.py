import pandas as pd
from data_engineering.parser.note import CAMNotes
from datetime import date, datetime


NOTE = """| Date of Meeting (mm/dd/yyyy): 06/02/2020



| CAM Attendees (FirstName LastName):

1. John Falope

2. Doug Peterson

3. << insert text here >>




| Meeting Description (Lunch, coffee, drinks, meeting in office, etc.):

Lunch



| Purpose (Follow up meeting, first meeting, intro to Data & Analytics, etc.):

First Meeting



| Updates  (Please provide a recap of what was discussed):

Got a feel of client expectation





| Opportunities (Describe an opportunity uncovered, if any):





| Personal (Non-work related info - kids, family, hobbies, etc.):

<< insert text here >>



| Next steps (Send thank you note, touch base in a week, schedule lunch, etc.):

<< insert text here >>
"""

path = r'C:\out\test.csv'

note = CAMNotes(note_id='16278623', note_text=NOTE, create_datetime=date.today(), action='Client')
note.normalize_text()
note.generate()
table_name = 'CAM_NOTES_DETAIL'
schema = pd.io.sql.get_schema(note.note_df, table_name)
# note.note_df.to_csv(path)
