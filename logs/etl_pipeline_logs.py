"""
This module generate logs while processing data from the Steam Charts
website. This will provide audit nd/or logs to support troubleshooting of the ETL
Pipeline.
"""
import pandas as pd
from datetime import datetime

def etl_pipeline_logs(
        job: str,
        job_description: str,
        status: str,
        error_message: str | None
) -> None:
    """
    ETL Pipeline logger to record the logs after executing pipeline job using different
    functions from different modules inside the package of `etl/extract/`,
    `etl/transform/`, and `etl/load/`.

    :param job: ETL Pipeline job to record the logs: `EXTRACT/TRANSFORM/LOAD`
    :type job: str

    :param job_description: Description of the ETL Pipeline job
    :type job_description: str

    :param status: The status after performing the job: `SUCCESSFUL/FAILED`
    :type status: str

    :param error_message: The error messsage that indicates the proper description on
        why does the ETL Pipeline job failed, None non-existent
    :type error_message: str | None
    """
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d %M:%M:%S")

    current_logs = pd.read_csv('logs/logs.csv')
    initial_logs = pd.DataFrame({
        "job": [job],
        "job_description": [job_description],
        "status": [status],
        "error_message": [error_message],
        "timestamp": [timestamp]
    })

    updated_logs = pd.concat([
        current_logs,
        initial_logs
    ], ignore_index=False)
    updated_logs.to_csv('logs/logs.csv', index=False)