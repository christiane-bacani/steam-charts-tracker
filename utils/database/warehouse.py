"""
Python module to create warehouse (compute engine) of the Snowflake
Data Warehouse that represents the gold/mart data layer.
"""
import os
from dotenv import load_dotenv

from utils.database.connection import init_connection_to_snowflake

from logs import logger

def create_warehouse(warehouse_name: str) -> None:
    """
    Create new warehouse which is the compute engine
    of Snowflake Data Warehouse (if still does not exist)
    to use for querying data from the database from
    the gold/mart data layer.

    Args:
        warehouse_name (str): The desired name of the warehouse.
    """