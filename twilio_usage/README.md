# twilio_usage

Weekly pull from twilio API to get usage data

#### HIGH LEVEL DESCRIPTION:

This project pulls data from the twilio API and pushes the usage data to Snowflake for analysis. The cadence is monthly 
so it should have data in monthly increments

### ENVIRONMENT VARIABLES NEEDED:
0. SNOWFLAKEUSER: STRING
0. SNOWFLAKEPWD: BASE64ENCODED STRING
0. LANDING: STRING
0. MAINT: STRING
0. ACCOUNT: ACCOUNT
0. DATABASE: STRING
0. WAREHOUSE: STRING
0. ROLE: STRING


### DDL:
Cloning tables from dev to test

`create or replace table <database>.twilio.twilio_usage clone landing_dev.twilio.twilio_usage;`


### MOUNTS:
The directory that the twilio_config.json file exist needs to be mounted. 

### CONFIG FILES:
twilio_config.json

### DOCKER COMMANDS:
`docker login git.homeadvisor.com:4567`

`docker pull git.homeadvisor.com:4567/data_engineering/twilio_usage:master-0.0.2`

`docker run --rm -v <config/passwords_file_directory>:/src/passwords -e SNOWFLAKEUSER -e SNOWFLAKEPWD -e LANDING -e DATABASE -e MAINT -e ACCOUNT -e WAREHOUSE -e ROLE 
git.homeadvisor.com:4567/data_engineering/twilio_usage:master-0.0.2 python twilio_api_pull.py --start-date 2020-01-01 --end-date 2020-01-01`

### DATA RECOVERY:
for backpops, pass in the dates in the docker run command. Please make sure the date ranges are monthly increments, i.e 2019-06-01 to 2019-06-30. 
The job will remove records for that date range and load them into the table. Warning, if the date range is not a month, the job will fail.
`docker run --rm -v <config/passwords_file_directory>:/src/passwords -e SNOWFLAKEUSER -e SNOWFLAKEPWD -e LANDING -e DATABASE -e MAINT -e ACCOUNT -e WAREHOUSE -e ROLE 
git.homeadvisor.com:4567/data_engineering/twilio_usage:master-0.0.2 python twilio_api_pull.py --start-date 2020-01-01 --end-date 2020-01-31`


### SCHEDULE:
Please run this within the first three days of every month. Timing of the pull is not critical as long as it is consistent  