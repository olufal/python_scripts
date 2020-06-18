# cam_note_parser

#### HIGH LEVEL DESCRIPTION:

This project was created to parse CAM notes, validate and match the attendee name against an internal employee database and load the data into SnowFlake.
These are the steps of the pipeline:

- Extract: The initial data ingestion is within ADF, a copy data steps grabs the note data from the Bullhorn database and drops it in Azure Blob Storage in a file called 'ClientMeetingNotes.csv'
- Transform: The parser will then run and transform the data as necessary into the rows needed
- Load: I have created a load process within the application that will push a blob to Azure Blob Storage and then loads it into the table.

### ENVIRONMENT VARIABLES NEEDED:
None as of now. Might implement for Security

### SQL Commands:
DDL in the script is for matching Attendee full name on the DIM_Employee table. See parser.py

### Recovery Plan:
This is an image so recovery is not necessary. As for data recovery, the pipeline should get the input file from the COPY data step in ADF

### MOUNTS:
None

### CONFIG FILES:
None

### DOCKER COMMANDS:

`docker run --rm imagename python main.py`

### SCHEDULE:
The main.py script will be scheduled to run every 30 minutes
