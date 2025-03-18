# snowflake_connector.py
import snowflake.connector
from config import SNOWFLAKE_ACCOUNT, SNOWFLAKE_DATABASE, SNOWFLAKE_PASSWORD, SNOWFLAKE_ROLE, SNOWFLAKE_SCHEMA, SNOWFLAKE_STAGE_TABLE, SNOWFLAKE_USER, SNOWFLAKE_WAREHOUSE  # All your credentials

# 1. Basic Connection Function
def get_connection():

    """Connecting to Snowflake using the provided credentials."""

    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE
    )
