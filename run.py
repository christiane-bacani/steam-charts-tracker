"""
Python module to run the ETL Pipeline for Steam Charts Tracker.
"""
from utils.database.database import create_database
from utils.database.schema import create_schema
from utils.database.table import create_table_for_raw_layer

from utils.extract.parse import parse_soup
from utils.load.raw import load_scraped_data_to_raw_layer

from etl.extract.trending_games import scrape_top5_trending_games
from etl.extract.top_games import scrape_top100_games
from etl.extract.top_records import scrape_top10_records



# Create Database objects
create_database("steam_charts")
create_schema("raw")
create_schema("stg")
create_schema("mart")
create_table_for_raw_layer("top5_trending_games_raw")
create_table_for_raw_layer("top100_games_raw")

# Parse BeautifulSoup object to extract trending games and top records
url = "https://steamcharts.com/"
soup = parse_soup(url)

# Extract top 5 trending games and save to `raw` layer
top5_trending_games = scrape_top5_trending_games(soup)
load_scraped_data_to_raw_layer(top5_trending_games, "top5_trending_games_raw")

# Extract top 100 games (by current players) and save to `raw` layer
top100_games = {
    "app_id":          [],
    "rank":            [],
    "name":            [],
    "current_players": [],
    "peak_players":    [],
    "hours_played":    []
}
for number in range(1, 5):
    top100_games_soup = parse_soup(f"{url}top/p.{number}")
    top100_games = scrape_top100_games(top100_games_soup, top100_games)
load_scraped_data_to_raw_layer(top100_games, "top100_games_raw")

# Extract top 10 records and save to `raw` layer
top10_records = scrape_top10_records(soup)
load_scraped_data_to_raw_layer(top10_records, "top10_records_raw")