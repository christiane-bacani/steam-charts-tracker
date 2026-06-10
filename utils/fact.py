"""
Python module to create fact tables from silver data layer  that consist of all
data tracked by Steam Charts.
"""
import pandas as pd

import os
from dotenv import load_dotenv

from utils.database.connection import init_connection

def create_fact_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create fact table from a certain DataFrame object.

    Args:
        DataFrame: The created fact table.
    """