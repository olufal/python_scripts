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


### MOUNTS:
The directory that the twilio_config.json file exist needs to be mounted. 

### CONFIG FILES:
twilio_config.json

### DOCKER COMMANDS:
`docker login github.com/olufal/data_engineering/tree/master/twilio_usage:4567`

`docker pull github.com/olufal/data_engineering/tree/master/twilio_usage:master-0.0.2`

`docker run --rm -v <config/passwords_file_directory>:/src/passwords -e SNOWFLAKEUSER -e SNOWFLAKEPWD -e LANDING -e DATABASE -e MAINT -e ACCOUNT -e WAREHOUSE -e ROLE 
github.com/olufal/data_engineering/tree/master/twilio_usage:master-0.0.2 python twilio_api_pull.py --start-date 2020-01-01 --end-date 2020-01-01`

