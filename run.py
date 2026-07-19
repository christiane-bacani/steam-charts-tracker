"""
Python module to run the ETL Pipeline for Steam Charts Tracker.
"""
from utils.database.warehouse import create_warehouse
from utils.database.database import create_postgres_database
from utils.database.database import create_snowflake_database
from utils.database.schema import create_postgres_schema
from utils.database.schema import create_snowflake_schema
from utils.database.table import create_postgres_table_for_raw
from utils.database.table import create_snowflake_table_for_mart

from utils.parse import parse
from etl.extract.extract import ingest_top5_trending_games
from etl.extract.extract import ingest_top100_games
from etl.extract.extract import ingest_top10_records

from etl.extract.extract import extract
from etl.transform.transform import transform
from etl.transform.validate import validate
from etl.load.load import load

from utils.dimension import create_dimension_table
from utils.fact import create_fact_table



# Create Database objects of PostgreSQL and Snowflake
create_postgres_database("steam_charts")
create_postgres_schema("raw")
create_postgres_schema("stg")
create_postgres_table_for_raw("top5_trending_games_raw")
create_postgres_table_for_raw("top100_games_raw")
create_postgres_table_for_raw("top10_records_raw")
create_warehouse("steam_charts_warehouse")
create_snowflake_database("STEAM_CHARTS")
create_snowflake_schema("MART")



# Parse BeautifulSoup object to extract trending games and top records
url = "https://steamcharts.com/"
soup = parse(url)

# Ingest top 5 trending games and save the ingested data to `raw` data layer
top5_trending_games = ingest_top5_trending_games(soup)
load(top5_trending_games)

# Ingest top 100 games (by current players) and save the ingested data to
# `raw` data layer
top100_games = {
    "app_id":          [],
    "rank":            [],
    "name":            [],
    "current_players": [],
    "peak_players":    [],
    "hours_played":    []
}
for number in range(1, 5):
    top100_games_soup = parse(f"{url}top/p.{number}")
    top100_games = ingest_top100_games(top100_games_soup, top100_games)
load(top100_games)

# Ingest top 10 game records and save the ingested data to `raw` data layer
top10_records = ingest_top10_records(soup)
load(top10_records)



# Extract the ingested data of top 5 trending games from `raw` data layer,
# transform the data, validate the transformed data, and save the
# transformed/validated data to `stg` data layer
top5_trending_games_raw = extract("top5_trending_games_raw")
top5_trending_games_raw = transform(top5_trending_games_raw)
top5_trending_games_raw = validate(top5_trending_games_raw)
load(top5_trending_games_raw)

# Extract the ingested data of top 100 games from `raw` data layer,
# transform the data, validate the transformed data, and save the
# transformed/validated data to `stg` data layer
top100_games_raw = extract("top100_games_raw")
top100_games_raw = transform(top100_games_raw)
top100_games_raw = validate(top100_games_raw)
load(top100_games_raw)

# Extract the ingested data of top 10 records from `raw` data layer,
# transform the data, validate the transformed data, and save the
# transformed/validated data to `stg` data layer
top10_records_raw = extract("top10_records_raw")
top10_records_raw = transform(top10_records_raw)
top10_records_raw = validate(top10_records_raw)
load(top10_records_raw)



# Create the dimemsion table: `dim_rank_number` by integrating
# the necessary columns of different tables of `stg` data layer
top5_trending_games_stg = extract("top5_trending_games_stg")
top100_games_stg = extract("top100_games_stg")
top10_records_stg = extract("top10_records_stg")
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