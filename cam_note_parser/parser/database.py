"""
Author: John Falope
"""
import pandas as pd
import snowflake.sqlalchemy as sf_sqlalchemy
import sqlalchemy


# Snowflake connection parameters
ACCOUNT = 'strivepartner.east-us-2.azure'
WAREHOUSE = 'ETL_WH_DEV'
DATABASE = 'IA_RAW_DEV'
SCHEMA = 'BULLHORN'
ROLE = 'SYSADMIN'
TABLE = 'CAM_NOTE_DETAIL'


class SnowFlakeEngine:
    def __init__(self, connection_url: str, insecure_mode: bool = False):
        self.connection_url = connection_url
        self.insecure_mode = insecure_mode

    def __enter__(self):
        self.engine = sqlalchemy.create_engine(
            self.connection_url, connect_args={
                "insecure_mode": self.insecure_mode})
        self.engine.execution_options(isolation_level="AUTOCOMMIT")
        force_auto_commit = 'ALTER SESSION SET AUTOCOMMIT = TRUE'
        pd.read_sql_query(force_auto_commit, self.engine)
        return self.engine

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.engine.dispose()


class SnowFlake:
    """
    SnowFlake is a wrapper for a sqlalchemy.engine connecting to SnowFlake

    This class exposes convenience methods for pandas DataFrames
    """

    def __init__(
            self,
            user: str = None,
            password: str = None,
    ):
        """
        Constructor for connecting to SnowFlake

        :param user: Snowflake login
        :param password: Snowflake password, base64 encoded
        :param account: Snowflake account
        :param database: database
        :param warehouse: warehouse
        :param role: role
        """
        self.user = user
        self.password = password

        self.connection_url = sf_sqlalchemy.URL(
            account=ACCOUNT,
            user= self.user,
            password=self.password,
            database=DATABASE,
            schema=SCHEMA,
            warehouse=WAREHOUSE,
            role=ROLE,
        )

    def query(self, sql: str) -> pd.DataFrame:
        """
        Execute a sql query with the current SnowFlake engine

        :param sql: SQL query to execute
        :returns: result of query as a Pandas DataFrame
        """
        if sql is None:
            raise ValueError("sql param must be specified to run a query")
        with SnowFlakeEngine(self.connection_url) as engine:
            try:
                # print(f"Executing query: {sql}")
                result = pd.read_sql_query(sql, engine)
                # print(f"Got query result with shape: {result.shape}")
                # print(f"Sample of 5: {result.head(5)}")
            except Exception as query_exception:
                print(f"error executing query - {sql}")
                raise query_exception from None
            return result


