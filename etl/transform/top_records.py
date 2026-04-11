"""
Python module to transform the extracted data of the current top 10 records
from a JSON file.
"""
import json
import pandas as pd
from datetime import datetime

from logs.etl_pipeline_logs import provide_logs