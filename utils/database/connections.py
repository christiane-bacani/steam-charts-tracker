"""
Python module to establish database connection to PostgreSQL.
"""
import psycopg2

from logs import logger

def init_connection(user: str, host: str, password: str, port: int = 5432) -> object:
    """
    Establish a PostgreSQL database connection using Pyscopg2.

    Args:
        user (str): Username required to authenticate.
        host (str): Database server address.
        password (str): Password used to authenticate.
        port (str): Connection port number (defaults to 5432 if not provided).
    """
    logger.info("Establishing database connection.")
    conn = psycopg2.connect(
        user=user,
        host=host,
        password=password,
        port=port
    )
    logger.info("Successsfully established a database connection.")
    return conn