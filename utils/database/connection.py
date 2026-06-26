"""
Python module to establish database connection to PostgreSQL.
"""
import snowflake.connector
from snowflake.connector.connection import SnowflakeConnection
from sqlalchemy import Engine, create_engine

def init_connection_to_postgres(host: str, 
                    port: int,
                    database: str, 
                    user: str, 
                    password: str) -> Engine:
    """
    Establish a PostgreSQL database connection using SQLAlchemy and Pyscopg2.

    Args:
        host (str): Database server address.
        port (int): Connection port number.
        database (str): The name of the database.
        user (str): Username required to authenticate.
        password (str): Password used to authenticate.

    Returns:
        engine (Engine): SQLAlchemy Engine.
    """
    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    )
    return engine

def init_connection_to_snowflake(warehouse: str,
                                 database: str,
                                 schema: str,
                                 account: str,
                                 user: str,
                                 password: str) -> SnowflakeConnection:
    """
    Establish a Snowflake Data Warehouse connection using Snowflake Connector.

    Args:
        warehouse (str): The name of the data warehouse.
        database (str): The name of the database.
        schema (str): The name of the database schema.
        account (str): Account identifier to authenticate.
        user (str): Username required to authenticate.
        password (str): Password used to authenticate.

    Returns
        conn (SnowflakeConnection): Snowflake Data Warehouse Connection.
    """