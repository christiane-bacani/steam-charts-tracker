"""
Python module to perform web-scraping to extract all the data related to
the current top 10 records tracked by Steam Charts.
"""
from bs4 import BeautifulSoup

from logs import logger