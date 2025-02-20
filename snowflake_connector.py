# snowflake_connector.py
import snowflake.connector
from config import *  # All your credentials

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
