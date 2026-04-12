"""
Python module to navigate SQLite3 database connection.
"""
import sqlite3

def open_db_connection(database_file: str) -> None:
    """
    Open an SQLite3 database connection.

    Args:
        database_file (str): The filepath of the Database file
    """
    conn = sqlite3.connect(database_file)
    return conn

def close_db_connection(conn: sqlite3.Connection) -> None:
    """
    Close an SQLite3 database connection.

    Args:
        database_file (str): The filepath of the Database file
    """
    conn.close()