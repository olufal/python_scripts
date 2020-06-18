from automated_tasks.LossCost import LossCost, LOSS_COST_DIRECTORY
import os
import pandas as pd
from vapydb.mssql.database import SqlServer

SERVER = 'servicesdb2'
DSN = 'SQL Server'
DATABASE = 'Sandbox_Jfalope'
SCHEMA = 'dbo'
TABLE = 'Loss_Cost_Parse_Test'

S = SqlServer(server=SERVER, database=DATABASE, schema=SCHEMA, dsn=DSN)

FILES = os.listdir(path=LOSS_COST_DIRECTORY)

df = pd.DataFrame()
for file in FILES:
    lc = LossCost(pdf_file=file)
    if lc.state_code is not None:
        lc.generate()
    df = df.append(lc.loss_cost_df, sort=False, ignore_index=True)


S.upload_DataFrame(df=df, table_name=TABLE)
# this is a c

