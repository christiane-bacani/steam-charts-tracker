"""
Python module to run the ETL Pipeline.
"""
from utils.extract.parse import parse_soup
from utils.extract.parse import parse_top_5_trending_games
from utils.extract.parse import parse_top_10_games
from utils.extract.parse import parse_top_10_records

from etl.extract.trending_games import extract_top_5_trending_games
from etl.extract.trending_games import extract_trending_games_stats_overview
from etl.extract.trending_games import save_trending_games_stats_overview_to_json
from etl.extract.trending_games import extract_trending_games_historical_stats
from etl.extract.trending_games import save_trending_games_historical_stats_to_json

from etl.extract.top_games import extract_top_10_games
from etl.extract.top_games import extract_top_games_stats_overview
from etl.extract.top_games import save_top_games_stats_overview_to_json
from etl.extract.top_games import extract_top_games_historical_stats
from etl.extract.top_games import save_top_games_historical_stats_to_json

from etl.extract.top_records import extract_top_10_records
from etl.extract.top_records import extract_top_records_stats_overview
from etl.extract.top_records import save_top_records_stats_overview_to_json
from etl.extract.top_records import extract_top_records_historical_stats
from etl.extract.top_records import save_top_records_historical_stats_to_json

from etl.transform.trending_games import transform_top_5_trending_games
from etl.transform.trending_games import transform_trending_games_stats_overview
from etl.transform.trending_games import transform_trending_games_historical_stats

from etl.transform.top_games import transform_top_10_games

# ================================
#     EXTRACT TRENDING GAMES
# ================================

url = "https://steamcharts.com/"

# Extract the current top 5 trending games
soup = parse_soup(
    url,
    "Parse the BeautifulSoup object from Steam Charts website to extract the data of "
    "the current top 5 trending games."
)
extract_top_5_trending_games(soup)

trending_games_stats_overview = {
    "app_id":                        [],
    "game_image":                    [],
    "twenty_four_hour_peak_players": [],
    "all_time_peak_players":         [],
    "current_datetime":              []
}
trending_games_historical_stats = {}

# Parse the scraped data of the current top 5 trending games from a JSON file
top_5_trending_games = parse_top_5_trending_games(
    "data/input/top_5_trending_games.json"
)

for number, app_id in enumerate(top_5_trending_games["app_id"]):
    soup = parse_soup(
        url + "app/" + app_id,
        f"Parse the BeautifulSoup object of the current number {number + 1} trending "
        "game from Steam Charts website using the 'app_id' to extract the data of "
        "its stats overview and historical stats.",
    )

    # Scrape stats overview of all current trending games
    stats_overview = extract_trending_games_stats_overview(
        soup,
        app_id,
        f"Extract the stats overview data of the current number {number + 1} "
        "trending game from Steam Charts."
    )
    for key, value in stats_overview.items():
        trending_games_stats_overview[key].append(value)

    # Scrape historical stats of all current trending games
    historical_stats = extract_trending_games_historical_stats(
        soup,
        app_id,
        f"Extract the historical stats data of the current number {number + 1} "
        "trending game from Steam Charts.",
    )
    trending_games_historical_stats[app_id] = historical_stats[app_id]

# Save the scraped data of stats overview and historical stats of
# all current trending games
save_trending_games_stats_overview_to_json(
    trending_games_stats_overview,
    "data/input/top_5_trending_games_stats_overview.json"
)
save_trending_games_historical_stats_to_json(
    trending_games_historical_stats,
    "data/input/top_5_trending_games_historical_stats.json"
)



# ================================
#        EXTRACT TOP GAMES
# ================================

# Extract the current top 10 games (by current players)
soup = parse_soup(
    url,
    "Parse the BeautifulSoup object from Steam Charts website to extract the data of "
    "the current top 10 games (by current players)."
)
extract_top_10_games(soup)

top_games_stats_overview = {
    "app_id":                        [],
    "game_image":                    [],
    "twenty_four_hour_peak_players": [],
    "all_time_peak_players":         [],
    "current_datetime":              []
}
top_games_historical_stats = {}

# Parse the scraped data of the current top 10 games (by current players)
# from a JSON file
top_10_games = parse_top_10_games(
    "data/input/top_10_games.json"
)

for number, app_id in enumerate(top_10_games["app_id"]):
    soup = parse_soup(
        url + "app/" + app_id,
        f"Parse the BeautifulSoup object of the current number {number + 1} "
        "game (by current players) from Steam Charts website using the 'app_id' "
        "to extract the data of its stats overview and historical stats."
    )

    # Scrape stats overview of all current top games (by current players)
    stats_overview = extract_top_games_stats_overview(
        soup,
        app_id,
        f"Extract the stats overview data of the current number {number + 1} "
        "game (by current players) from Steam Charts."
    )
    for key, value in stats_overview.items():
        top_games_stats_overview[key].append(value)

    # Scrape historical stats of all current top games (by current players)
    historical_stats = extract_top_games_historical_stats(
        soup,
        app_id,
        f"Extract the historical stats of the current number {number + 1} "
        "game (by current players) from Steam Charts."
    )
    top_games_historical_stats[app_id] = historical_stats[app_id]

# Save the scraped data of stats overview and historical stats of
# all current top games (by current players)
save_top_games_stats_overview_to_json(
    top_games_stats_overview,
    "data/input/top_10_games_stats_overview.json"
)
save_top_games_historical_stats_to_json(
    top_games_historical_stats,
    "data/input/top_10_games_historical_stats.json"
)



# ================================
#     EXTRACT TOP RECORDS DATA   
# ================================

# Extract the current top 10 records
soup = parse_soup(
    url,
    "Parse the BeautifulSoup object from Steam Charts website to extract the data of "
    "the current top 10 records."
)
extract_top_10_records(soup)

top_records_stats_overview = {
    "app_id":                        [],
    "game_image":                    [],
    "twenty_four_hour_peak_players": [],
    "all_time_peak_players":         [],
    "current_datetime":              []
}
top_records_historical_stats = {}

# Parse the scraped data of the current top 10 records from a JSON file
top_10_records = parse_top_10_records(
    "data/input/top_10_records.json"
)

for number, app_id in enumerate(top_10_records["app_id"]):
    soup = parse_soup(
        url + "app/" + app_id,
        f"Parse the BeautifulSoup object of the current number {number + 1} "
        "record from Steam Charts website using the 'app_id' to extract the "
        "data of its stats overview and historical stats."
    )

    # Scrape stats overview of all current top records
    stats_overview = extract_top_records_stats_overview(
        soup,
        app_id,
        f"Extract the stats overview data of the current number {number + 1} "
        "record from Steam Charts."
    )
    for key, value in stats_overview.items():
        top_records_stats_overview[key].append(value)

    # Scrape historical stats of all current top records
    historical_stats = extract_top_records_historical_stats(
        soup,
        app_id,
        f"Extract the historical stats of the current number {number + 1} "
        "record from Steam Charts."
    )
    top_records_historical_stats[app_id] = historical_stats[app_id]

# Save the scraped data of stats overview and historical stats of
# all current top records
save_top_records_stats_overview_to_json(
    top_records_stats_overview,
    "data/input/top_10_records_stats_overview.json"
)
save_top_records_historical_stats_to_json(
    top_records_historical_stats,
    "data/input/top_10_records_historical_stats.json"
)





# =========================================
#      TRANSFORM TRENDING GAMES DATA
# =========================================

transform_top_5_trending_games(
    "data/input/top_5_trending_games.json"
)
transform_trending_games_stats_overview(
    "data/input/top_5_trending_games_stats_overview.json"
)
transform_trending_games_historical_stats(
    "data/input/top_5_trending_games_historical_stats.json"
)



# =========================================
#          TRANSFORM TOP GAMES DATA
# =========================================

transform_top_10_games(
    "data/input/top_10_games.json"
)