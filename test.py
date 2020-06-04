import pandas as pd
from strive.note import CAMNotes
from datetime import date, datetime


NOTE = """| Date of Meeting (mm/dd/yyyy): 06/02/2020



| CAM Attendees (FirstName LastName):

1. John Falope

2.  Doug P.

3.  Maria Doe

4.  << insert text here >>

5.  << insert text here >>



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

note = CAMNotes(note_text=NOTE, create_datetime=date.today(), action='Client')
note.normalize_text()
note.generate()
