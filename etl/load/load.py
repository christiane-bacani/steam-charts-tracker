"""
Python module to store all scraped, transformed, aggregrated, or modeleted data
from Steam Charts website to their corresponding data storage layer.
"""
import pandas as pd

import os
from dotenv import load_dotenv

from utils.database.connection import init_connection

from logs import logger