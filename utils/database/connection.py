"""
Python module to establish database connection to PostgreSQL.
"""
from sqlalchemy import Engine, create_engine

def init_connection(host: str, 
                    port: int,
                    database: str, 
                    user: str, 
                    password: str) -> Engine:
    """
    Establish a PostgreSQL database connection using SQLAlchemy and Pyscopg2.

    Args:
        host (str): Database server address.
        port (int): Connection port number.
        database: The name of the database.
        user (str): Username required to authenticate.
        password (str): Password used to authenticate.
    """
    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    )
    return engine