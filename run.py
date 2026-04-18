"""
Python module to run the ETL Pipeline.
"""
from utils.database.database import create_database
from utils.database.schema import create_schema
from utils.extract.parse import parse_soup

from etl.extract.trending_games import scrape_top_5_trending_games

create_schema("raw")
create_schema("stg")
create_schema("mart")
create_database("steam_charts")

url = "https://steamcharts.com/"
soup = parse_soup(url)

top_5_trending_games = scrape_top_5_trending_games(soup)