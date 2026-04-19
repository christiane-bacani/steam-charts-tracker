"""
Python module to establish database connection to PostgreSQL.
"""
import sqlalchemy

def init_connection(host: str, 
                    port: int,
                    database: str, 
                    user: str, 
                    password: str) -> sqlalchemy.Engine:
    """
    Establish a PostgreSQL database connection using Pyscopg2.

    Args:
        host (str): Database server address.
        port (int): Connection port number.
        database: The name of the database.
        user (str): Username required to authenticate.
        password (str): Password used to authenticate.
    """
    engine = sqlalchemy.create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    )
    return engine