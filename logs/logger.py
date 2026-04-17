"""
Python module to create log/s per job executed inside the ETL Pipeline.
"""
def info(message: str) -> None:
    """
    Display logs in the terminal for the ETL Pipeline.

    Args:
        message (str): The information provided for the logs.
    """
    print(message)