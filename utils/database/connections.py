"""
Python module to establish database connection to PostgreSQL.
"""
import psycopg2

from logs import logger

def init_connection(host: str, 
                    port: int,
                    database: str, 
                    user: str, 
                    password: str) -> object:
    """
    Establish a PostgreSQL database connection using Pyscopg2.

    Args:
        host (str): Database server address.
        port (int): Connection port number.
        database: The name of the database.
        user (str): Username required to authenticate.
        password (str): Password used to authenticate.
    """
    logger.info("Establishing database connection.")
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )
    logger.info("Successsfully established a database connection.")
    return conn