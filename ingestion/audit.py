import snowflake.connector
from ingestion.config import Config
from ingestion.logger import get_logger

logger = get_logger(__name__)


class AuditLogger:
    """Writes pipeline run outcomes to AUDIT_LOGS.PIPELINE_RUNS."""

    def __init__(self):
        try:
            self.conn = snowflake.connector.connect(
                account=Config.SNOWFLAKE_ACCOUNT,
                user=Config.SNOWFLAKE_USER,
                password=Config.SNOWFLAKE_PASSWORD,
                warehouse=Config.SNOWFLAKE_WAREHOUSE,
                database=Config.SNOWFLAKE_DATABASE,
                schema="AUDIT_LOGS",
            )
            logger.info("Connected to Snowflake for audit logging.")
        except Exception:
            logger.exception("Failed to connect to Snowflake for audit logging.")
            raise

    def log_run(
        self,
        pipeline_run_id,
        execution_start,
        execution_end,
        rows_extracted,
        rows_loaded,
        rows_rejected,
        execution_status,
        failure_reason=None,
    ):
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO AUDIT_LOGS.PIPELINE_RUNS (
                    pipeline_run_id, execution_start, execution_end,
                    rows_extracted, rows_loaded, rows_rejected,
                    execution_status, failure_reason
                )
                SELECT %s, %s, %s, %s, %s, %s, %s, %s
                """,
                (
                    pipeline_run_id, execution_start, execution_end,
                    rows_extracted, rows_loaded, rows_rejected,
                    execution_status, failure_reason,
                ),
            )
            self.conn.commit()
            logger.info(
                "Audit log written. run_id=%s, status=%s, "
                "extracted=%s, loaded=%s, rejected=%s",
                pipeline_run_id, execution_status,
                rows_extracted, rows_loaded, rows_rejected,
            )
        except Exception:
            logger.exception("Failed to write audit log. run_id=%s", pipeline_run_id)
            self.conn.rollback()
            raise
        finally:
            cursor.close()

    def close(self):
        try:
            self.conn.close()
            logger.info("Audit connection closed.")
        except Exception:
            logger.exception("Error closing audit connection.")