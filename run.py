"""
Python module to run the ETL Pipeline.
"""
from etl.extract.beautiful_soup import parse_soup

# URL of the website to scrape
url = "https://steamcharts.com/"

soup = parse_soup(url)