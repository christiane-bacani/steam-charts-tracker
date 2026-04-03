"""
Python module to create log/s per job executed inside the ETL Pipeline.
"""
import pandas as pd
from datetime import datetime

def provide_logs(
        job: str,
        description: str,
        status: str,
        error_message: str
) -> None:
    """
    Provide logs per job executed inside the ETL Pipeline.

    Args:
        job (str): The job inside the ETL Pipeline.

        description (str): Description of the job.

        status (str): The status to determine if the job is successfully
            executed.

        error_message (str): Error message to provide more context about
            why the job was unsuccessfully executed.
    """
    logs = pd.read_csv("logs/logs.csv")
    new_logs = pd.DataFrame({
        "job": [job],
        "description": [description],
        "status": [status],
        "error_message": [pd.NA if error_message is None else error_message],
        "timestamp": [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
    })

    # Store the new pipeline logs to a dedicated csv file using pandas dataframe
    logs = pd.concat([
        logs,
        new_logs
    ], ignore_index=True)

    logs.to_csv("logs/logs.csv", index=False)