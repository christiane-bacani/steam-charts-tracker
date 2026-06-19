"""
Python module to run the ETL Pipeline for Steam Charts Tracker.
"""
from utils.database.database import create_database
from utils.database.schema import create_schema
from utils.database.table import create_table_for_raw_layer

from utils.parse import parse_soup
from etl.extract.extract import scrape_top5_trending_games
from etl.extract.extract import scrape_top100_games
from etl.extract.extract import scrape_top10_records

from etl.extract.extract import extract_data_from_sql_table
from etl.transform.transform import transform_top5_trending_games_raw
from etl.transform.transform import transform_top100_games_raw
from etl.transform.transform import transform_top10_records_raw
from etl.transform.transform import transform_dim_rank_number
from etl.transform.transform import transform_dim_steam_game
from etl.transform.transform import transform_dim_timestamp
from etl.transform.transform import transform_dim_peak_month
from etl.transform.transform import transform_dim_peak_year
from etl.transform.transform import transform_fact_trending_games
from etl.transform.transform import transform_fact_top_games
from etl.transform.transform import transform_fact_top_records
from etl.transform.validate import validate_top5_trending_games_stg
from etl.transform.validate import validate_top100_games_stg
from etl.transform.validate import validate_top10_records_stg
from etl.transform.validate import validate_dim_rank_number
from etl.transform.validate import validate_dim_steam_game
from etl.transform.validate import validate_dim_timestamp
from etl.transform.validate import validate_dim_peak_month
from etl.transform.validate import validate_dim_peak_year
from etl.transform.validate import validate_fact_trending_games
from etl.transform.validate import validate_fact_top_games
from etl.transform.validate import validate_fact_top_records

from etl.load.load import load_data_to_schema

from utils.dimension import create_dimension_table
from utils.fact import create_fact_table



# Create Database objects
create_database("steam_charts")
create_schema("raw")
create_schema("stg")
create_schema("mart")
create_table_for_raw_layer("top5_trending_games_raw")
create_table_for_raw_layer("top100_games_raw")
create_table_for_raw_layer("top10_records_raw")

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

"""
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

# Transform the extracted data of the top 10 records and save to 'stg' data layer
top10_records_raw = extract_data_from_sql_table("raw", "top10_records_raw")
top10_records_stg = transform_top10_records_raw(top10_records_raw)
top10_records_stg = validate_top10_records_stg(top10_records_stg)
load_data_to_schema(top10_records_stg, "stg", "top10_records_stg")
"""

"""
# Integrate 'current_rank' dimension from 'stg' data layer and save to 'mart' data layer
dim_rank_number = create_dimension_table("current_rank")
dim_rank_number = transform_dim_rank_number(dim_rank_number)
dim_rank_number = validate_dim_rank_number(dim_rank_number)
load_data_to_schema(dim_rank_number, "mart", "dim_rank_number")

# Integrate 'game_name' dimension from 'stg' data layer and save to 'mart' data layer
dim_steam_game = create_dimension_table("game_name")
dim_steam_game = transform_dim_steam_game(dim_steam_game)
dim_steam_game = validate_dim_steam_game(dim_steam_game)
load_data_to_schema(dim_steam_game, "mart", "dim_steam_game")

# Integrate 'timestamp' dmension from 'stg' data layer and save to 'mart' data layer
dim_timestamp = create_dimension_table("timestamp")
dim_timestamp = transform_dim_timestamp(dim_timestamp)
dim_timestamp = validate_dim_timestamp(dim_timestamp)
load_data_to_schema(dim_timestamp, "mart", "dim_timestamp")

# Integrate 'peak_month' dimension from 'stg' data layer and save to 'mart' data layer
dim_peak_month = create_dimension_table("peak_month")
dim_peak_month = transform_dim_peak_month(dim_peak_month)
dim_peak_month = validate_dim_peak_month(dim_peak_month)
load_data_to_schema(dim_peak_month, "mart", "dim_peak_month")

# Integrate 'peak_year' dimension from 'stg' data layer and save to 'mart' data layer
dim_peak_year = create_dimension_table("peak_year")
dim_peak_year = transform_dim_peak_year(dim_peak_year)
dim_peak_year = validate_dim_peak_year(dim_peak_year)
load_data_to_schema(dim_peak_year, "mart", "dim_peak_year")

# Extract the data of the top 5 trending games from 'stg' data layer and
# create the fact table and save to 'mart' data layer
top5_trending_games_stg = extract_data_from_sql_table("stg", "top5_trending_games_stg")
fact_trending_games = create_fact_table(top5_trending_games_stg)
fact_trending_games = transform_fact_trending_games(fact_trending_games)
fact_trending_games = validate_fact_trending_games(fact_trending_games)
load_data_to_schema(fact_trending_games, "mart", "fact_trending_games")

# Extract the data of the top 100 games from 'stg' data layer and
# create the fact table and save to 'mart' data layer
top100_games_stg = extract_data_from_sql_table("stg", "top100_games_stg")
fact_top_games = create_fact_table(top100_games_stg)
fact_top_games = transform_fact_top_games(fact_top_games)
fact_top_games = validate_fact_top_games(fact_top_games)
load_data_to_schema(fact_top_games, "mart", "fact_top_games")

# Extract the data of the top 10 records from 'stg' data layer and
# create the fact table and save to 'mart' data layer
top10_records_stg = extract_data_from_sql_table("stg", "top10_records_stg")
fact_top_records = create_fact_table(top10_records_stg)
fact_top_records = transform_fact_top_records(fact_top_records)
fact_top_records = validate_fact_top_records(fact_top_records)
load_data_to_schema(fact_top_records, "mart", "fact_top_records")
"""