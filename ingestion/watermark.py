import snowflake.connector
from ingestion.config import Config
from ingestion.logger import get_logger

logger = get_logger(__name__)

SOURCE_NAME = "uber_trips_csv"

class WatermarkManager:
    def __init__(self):
        try:
            self.conn = snowflake.connector.connect(
                account=Config.SNOWFLAKE_ACCOUNT,
                user=Config.SNOWFLAKE_USER,
                password=Config.SNOWFLAKE_PASSWORD,
                warehouse=Config.SNOWFLAKE_WAREHOUSE,
                database=Config.SNOWFLAKE_DATABASE,
                schema="CONTROL",
            )
            logger.info("Connected to Snowflake for watermark tracking.")
        except Exception:
            logger.exception("Failed to connect to Snowflake for watermark tracking.")
            raise

    def get_last_processed_row(self):
        """Returns the last successfully processed row number."""
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                "SELECT last_processed_row, total_rows_loaded "
                "FROM CONTROL.WATERMARKS WHERE source_name = %s",
                (SOURCE_NAME,),
            )
            row = cursor.fetchone()
            if row is None:
                logger.warning("No watermark found for %s. Starting from row 0.", SOURCE_NAME)
                return 0, 0
            last_row, total_loaded = row
            logger.info(
                "Watermark check: last_processed_row=%s, total_rows_loaded=%s",
                last_row, total_loaded,
            )
            return last_row, total_loaded
        finally:
            cursor.close()

    def update_watermark(self, last_processed_row, total_rows_loaded, source_file, status="SUCCESS"):
        """Updates watermark after successful batch load."""
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                """
                UPDATE CONTROL.WATERMARKS
                SET last_processed_row = %s,
                    total_rows_loaded  = %s,
                    last_processed_file = %s,
                    status             = %s,
                    updated_at         = CURRENT_TIMESTAMP()
                WHERE source_name = %s
                """,
                (last_processed_row, total_rows_loaded, source_file, status, SOURCE_NAME),
            )
            self.conn.commit()
            logger.info(
                "Watermark updated: last_row=%s, total_loaded=%s, status=%s",
                last_processed_row, total_rows_loaded, status,
            )
        finally:
            cursor.close()

    def close(self):
        try:
            self.conn.close()
            logger.info("Watermark connection closed.")
        except Exception:
            logger.exception("Error closing watermark connection.")