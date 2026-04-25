"""
Python module to perform data transformation to all extracted data related
to the current top 5 trending games tracked by Steam Charts.
"""
import pandas as pd
from sqlalchemy import Engine

def extract_top10_games_raw(engine: Engine) -> pd.DataFrame:
    """
    Extract the data from the table `top10_games_raw` from the raw
    layer.

    Args:
        engine (Engine): SQLAlchemy Engine.
    """
    query = "SELECT * FROM raw.top10_games_raw;"
    df = pd.read_sql(query, con=engine)
    return df