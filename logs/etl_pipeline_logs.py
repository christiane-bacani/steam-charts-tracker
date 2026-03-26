import pandas as pd

def provide_log(
        job: str,
        description: str,
        status: str = "Successful",
        error_message: str = "None"
) -> None:
    logs = pd.read_csv("logs/logs.csv")
    new_logs = pd.DataFrame({
        "job": [job],
        "description": [description],
        "status": [status],
        "error_message": [error_message]
    })

    logs = pd.concat([
        logs,
        new_logs
    ], ignore_index=True)

    logs.to_csv("logs/logs.csv", index=False)