"""
Python module to store all the scraped data related to the current top 100 games
(by current players) tracked by Steam Charts.
"""
import pandas as pd

import os
from dotenv import load_dotenv

from utils.database.connection import init_connection

from logs import logger