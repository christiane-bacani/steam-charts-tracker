"""
Python module to establish connection to PostgreSQL Database and
Snowflake Data Warehouse.
"""
import snowflake.connector
from snowflake.connector.connection import SnowflakeConnection
from sqlalchemy import Engine, create_engine

def init_connection_to_postgres(user: str,
                                password: str,
                                host: str,
                                port: str,
                                database: str) -> Engine:
    """
    Establish a PostgreSQL database connection using SQLAlchemy and Pyscopg2.

    Args:
        user (str): Username required to authenticate.
        password (str): Password used to authenticate.
        host (str): Database server address.
        port (int): Connection port number.
        database (str): The name of the database.

    Returns:
        engine (Engine): SQLAlchemy Engine.
    """
    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    )
    return engine

def init_connection_to_snowflake(user: str,
                                 password: str,
                                 account: str) -> SnowflakeConnection:
    """
    Establish a Snowflake Data Warehouse connection using Snowflake Connector.

    Args:
        user (str): Username required to authenticate.
        password (str): Password used to authenticate.
        account (str): Account identifier to authenticate.        

    Returns:
        conn (SnowflakeConnection): Snowflake Data Warehouse Connection.
    """
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account
    )

    return conn