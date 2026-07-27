import uuid
from datetime import datetime, timezone
from ingestion.config import Config
from ingestion.logger import get_logger
from ingestion.loader import TripLoader
from ingestion.watermark import WatermarkManager
from ingestion.audit import AuditLogger
logger = get_logger(__name__)
def run():
    pipeline_run_id  = str(uuid.uuid4())
    execution_start  = datetime.now(timezone.utc)

    logger.info("Starting Uber Trip pipeline. run_id=%s", pipeline_run_id)

    loader    = TripLoader()
    watermark = WatermarkManager()
    audit     = AuditLogger()

    rows_extracted   = 0
    rows_loaded      = 0
    rows_rejected    = 0
    execution_status = "FAILED"
    failure_reason   = None

    try:
        last_row, total_previously_loaded = watermark.get_last_processed_row()

        if total_previously_loaded > 0:
            logger.info(
                "Data already loaded (%s rows). Skipping.",
                total_previously_loaded,
            )
            execution_status = "SKIPPED"
            return

        # Load via COPY INTO
        batch_id = "batch_uber_data_csv_full"
        rows_loaded, rows_rejected = loader.load_all(
            pipeline_run_id=pipeline_run_id,
            batch_id=batch_id,
        )
        rows_extracted = rows_loaded

        # Update watermark
        watermark.update_watermark(
            last_processed_row=rows_loaded,
            total_rows_loaded=rows_loaded,
            source_file="uber_data.csv",
        )

        execution_status = "SUCCESS"
        logger.info(
            "Pipeline complete. extracted=%s, loaded=%s, rejected=%s",
            rows_extracted, rows_loaded, rows_rejected,
        )

    except Exception as e:
        failure_reason = str(e)
        logger.exception("Pipeline failed. run_id=%s", pipeline_run_id)
        raise

    finally:
        execution_end = datetime.now(timezone.utc)

        try:
            audit.log_run(
                pipeline_run_id=pipeline_run_id,
                execution_start=execution_start,
                execution_end=execution_end,
                rows_extracted=rows_extracted,
                rows_loaded=rows_loaded,
                rows_rejected=rows_rejected,
                execution_status=execution_status,
                failure_reason=failure_reason,
            )
        except Exception:
            logger.exception("Failed to write audit log.")

        try:
            loader.close()
            watermark.close()
            audit.close()
        except Exception:
            logger.exception("Error during cleanup.")

        logger.info(
            "Pipeline resources closed. run_id=%s",
            pipeline_run_id,
        )
if __name__ == "__main__":
    run()