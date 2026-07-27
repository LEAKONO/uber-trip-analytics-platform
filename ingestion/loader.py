import uuid
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from ingestion.config import Config
from ingestion.logger import get_logger

logger = get_logger(__name__)


class TripLoader:
    """
    Loads Uber trip CSV into Snowflake using write_pandas().
    Skips internal staging (PUT) to avoid S3 connectivity issues.
    Loads flat typed columns directly — no VARIANT/JSON conversion.
    """

    def __init__(self):
        try:
            self.conn = snowflake.connector.connect(
                account=Config.SNOWFLAKE_ACCOUNT,
                user=Config.SNOWFLAKE_USER,
                password=Config.SNOWFLAKE_PASSWORD,
                warehouse=Config.SNOWFLAKE_WAREHOUSE,
                database=Config.SNOWFLAKE_DATABASE,
                schema="RAW",
            )
            logger.info("Connected to Snowflake for raw data loading.")
        except Exception:
            logger.exception("Failed to connect to Snowflake.")
            raise

    def load_all(self, pipeline_run_id, batch_id):
        """
        Reads CSV with pandas, adds metadata columns,
        loads into RAW.TRIPS_STAGED using write_pandas().
        No PUT/stage/S3 involved — direct connection only.
        """
        logger.info("Reading CSV file...")
        df = pd.read_csv(
            Config.SOURCE_FILE_PATH,
            dtype=str,
        )
        logger.info("CSV loaded: %s rows.", len(df))

        # Rename columns to match Snowflake table
        df.columns = [
            "VENDOR_ID", "PICKUP_DATETIME", "DROPOFF_DATETIME",
            "PASSENGER_COUNT", "TRIP_DISTANCE",
            "PICKUP_LONGITUDE", "PICKUP_LATITUDE",
            "RATE_CODE_ID", "STORE_FWD_FLAG",
            "DROPOFF_LONGITUDE", "DROPOFF_LATITUDE",
            "PAYMENT_TYPE", "FARE_AMOUNT", "EXTRA",
            "MTA_TAX", "TIP_AMOUNT", "TOLLS_AMOUNT",
            "IMPROVEMENT_SURCHARGE", "TOTAL_AMOUNT",
        ]

        # Add metadata columns
        df["RAW_ID"] = [str(uuid.uuid4()) for _ in range(len(df))]
        df["BATCH_ID"] = batch_id
        df["PIPELINE_RUN_ID"] = pipeline_run_id
        df["SOURCE_FILE"] = "uber_data.csv"
        df["INGESTION_TIMESTAMP"] = pd.Timestamp.utcnow()

        # Create table if not exists
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS RAW.TRIPS_STAGED (
                    RAW_ID                STRING,
                    BATCH_ID              STRING,
                    PIPELINE_RUN_ID       STRING,
                    SOURCE_FILE           STRING,
                    INGESTION_TIMESTAMP   TIMESTAMP_NTZ,
                    VENDOR_ID             STRING,
                    PICKUP_DATETIME       STRING,
                    DROPOFF_DATETIME      STRING,
                    PASSENGER_COUNT       STRING,
                    TRIP_DISTANCE         STRING,
                    PICKUP_LONGITUDE      STRING,
                    PICKUP_LATITUDE       STRING,
                    RATE_CODE_ID          STRING,
                    STORE_FWD_FLAG        STRING,
                    DROPOFF_LONGITUDE     STRING,
                    DROPOFF_LATITUDE      STRING,
                    PAYMENT_TYPE          STRING,
                    FARE_AMOUNT           STRING,
                    EXTRA                 STRING,
                    MTA_TAX               STRING,
                    TIP_AMOUNT            STRING,
                    TOLLS_AMOUNT          STRING,
                    IMPROVEMENT_SURCHARGE STRING,
                    TOTAL_AMOUNT          STRING
                )
            """)

            # Idempotency — truncate before reload
            cursor.execute("TRUNCATE TABLE RAW.TRIPS_STAGED")
            logger.info("Table truncated for idempotent load.")

        finally:
            cursor.close()

        # Reorder columns to match table
        col_order = [
            "RAW_ID", "BATCH_ID", "PIPELINE_RUN_ID", "SOURCE_FILE",
            "INGESTION_TIMESTAMP", "VENDOR_ID", "PICKUP_DATETIME",
            "DROPOFF_DATETIME", "PASSENGER_COUNT", "TRIP_DISTANCE",
            "PICKUP_LONGITUDE", "PICKUP_LATITUDE", "RATE_CODE_ID",
            "STORE_FWD_FLAG", "DROPOFF_LONGITUDE", "DROPOFF_LATITUDE",
            "PAYMENT_TYPE", "FARE_AMOUNT", "EXTRA", "MTA_TAX",
            "TIP_AMOUNT", "TOLLS_AMOUNT", "IMPROVEMENT_SURCHARGE",
            "TOTAL_AMOUNT",
        ]
        df = df[col_order]

        logger.info("Loading %s rows via write_pandas...", len(df))
        success, nchunks, nrows, _ = write_pandas(
            conn=self.conn,
            df=df,
            table_name="TRIPS_STAGED",
            schema="RAW",
            database=Config.SNOWFLAKE_DATABASE,
            quote_identifiers=False,
        )

        if not success:
            raise RuntimeError("write_pandas returned failure.")

        logger.info(
            "write_pandas complete: %s rows in %s chunks.",
            nrows, nchunks,
        )
        return nrows, 0

    def close(self):
        try:
            self.conn.close()
            logger.info("Loader connection closed.")
        except Exception:
            logger.exception("Error closing loader connection.")