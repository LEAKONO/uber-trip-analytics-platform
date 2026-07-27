import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    # Snowflake
    SNOWFLAKE_ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]
    SNOWFLAKE_USER      = os.environ["SNOWFLAKE_USER"]
    SNOWFLAKE_PASSWORD  = os.environ["SNOWFLAKE_PASSWORD"]
    SNOWFLAKE_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    SNOWFLAKE_DATABASE  = os.environ.get("SNOWFLAKE_DATABASE", "UBER_ANALYTICS")

    # Source file — absolute path to the CSV
    SOURCE_FILE_PATH = os.environ["SOURCE_FILE_PATH"]