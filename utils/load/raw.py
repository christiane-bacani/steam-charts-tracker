"""
Python module to store all the scraped data that is tracked by Steam Charts to the data
storage layer called "raw".
"""
import pandas as pd

import os
from dotenv import load_dotenv

from utils.database.connection import init_connection

from logs import logger