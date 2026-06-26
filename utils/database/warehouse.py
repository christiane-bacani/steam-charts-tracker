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
    logger.info("Establishing a connection to Snowflake to create new warehouse.")
    load_dotenv()
    conn = init_connection_to_snowflake(os.getenv("SNOWFLAKE_USERNAME"),
                                        os.getenv("SNOWFLAKE_PASSWORD"),
                                        os.getenv("SNOWFLAKE_ACCOUNT_IDENTIFIER"))
    cursor = conn.cursor()

    logger.info(f"Creating warehouse: '{warehouse_name}'.")
    cursor.execute("""
    CREATE WAREHOUSE IF NOT EXISTS steam_charts_warehouse
    WITH WAREHOUSE_SIZE = 'LARGE'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    """)
    logger.info(f"Successfully created a new warehouse: '{warehouse_name}.'")

    cursor.close()
    conn.close()