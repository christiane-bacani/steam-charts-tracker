"""
Python module to establish database connection to PostgreSQL.
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
                                 account: str,
                                 warehouse: str,
                                 database: str,
                                 schema: str) -> SnowflakeConnection:
    """
    Establish a Snowflake Data Warehouse connection using Snowflake Connector.

    Args:
        user (str): Username required to authenticate.
        password (str): Password used to authenticate.
        account (str): Account identifier to authenticate.        
        warehouse (str): The name of the data warehouse.
        database (str): The name of the database.
        schema (str): The name of the database schema.

    Returns:
        conn (SnowflakeConnection): Snowflake Data Warehouse Connection.
    """
    conn = snowflake.connector.connection(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema
    )

    return conn