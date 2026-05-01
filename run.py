"""
Python module to run the ETL Pipeline for Steam Charts Tracker.
"""
from utils.database.database import create_database
from utils.database.schema import create_schema
from utils.database.table import create_table_for_raw_layer
from utils.database.table import create_table_for_stg_layer

from utils.extract.parse import parse_soup
from etl.extract.extract import scrape_top5_trending_games
from etl.extract.extract import scrape_top100_games
from etl.extract.extract import scrape_top10_records

from etl.extract.extract import extract_data_from_sql_table
from etl.transform.transform import transform_top5_trending_games_raw
from etl.transform.transform import transform_top100_games_raw
from etl.transform.transform import transform_top10_records_raw
from etl.transform.validate import validate_top5_trending_games_stg
from etl.transform.validate import validate_top100_games_stg

from etl.load.load import load_data_to_schema



# Create Database objects
create_database("steam_charts")
create_schema("raw")
create_schema("stg")
create_schema("mart")
create_table_for_raw_layer("top5_trending_games_raw")
create_table_for_raw_layer("top100_games_raw")
create_table_for_raw_layer("top10_records_raw")
create_table_for_stg_layer("top5_trending_games_stg")

# Parse BeautifulSoup object to extract trending games and top records
url = "https://steamcharts.com/"
soup = parse_soup(url)

# Extract top 5 trending games and save to `raw` data layer
top5_trending_games_raw = scrape_top5_trending_games(soup)
load_data_to_schema(top5_trending_games_raw, "raw", "top5_trending_games_raw")

# Extract top 100 games (by current players) and save to `raw` data layer
top100_games_raw = {
    "app_id":          [],
    "rank":            [],
    "name":            [],
    "current_players": [],
    "peak_players":    [],
    "hours_played":    []
}
for number in range(1, 5):
    top100_games_soup = parse_soup(f"{url}top/p.{number}")
    top100_games_raw = scrape_top100_games(top100_games_soup, top100_games_raw)
load_data_to_schema(top100_games_raw, "raw", "top100_games_raw")

# Extract top 10 records and save to `raw` data layer
top10_records_raw = scrape_top10_records(soup)
load_data_to_schema(top10_records_raw, "raw", "top10_records_raw")

# Transform the extracted data of the top 5 trending games and save to 'stg' data layer
top5_trending_games_raw = extract_data_from_sql_table("raw", "top5_trending_games_raw")
top5_trending_games_stg = transform_top5_trending_games_raw(top5_trending_games_raw)
top5_trending_games_stg = validate_top5_trending_games_stg(top5_trending_games_stg)
load_data_to_schema(top5_trending_games_stg, "stg", "top5_trending_games_stg")

# Transform the extracted data of the top 100 games and save to 'stg' data layer
top100_games_raw = extract_data_from_sql_table("raw", "top100_games_raw")
top100_games_stg = transform_top100_games_raw(top100_games_raw)
top100_games_stg = validate_top100_games_stg(top100_games_stg)
load_data_to_schema(top100_games_stg, "stg", "top100_games_stg")

top10_records_raw = extract_data_from_sql_table("raw", "top10_records_raw")
top10_records_stg = transform_top10_records_raw(top10_records_raw)
load_data_to_schema(top10_records_stg, "stg", "top10_records_stg")