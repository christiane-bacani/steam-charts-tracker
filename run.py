"""
Python module to run the ETL Pipeline.
"""
from utils.extract.parse import parse_soup
from utils.extract.parse import parse_top_5_trending_games

from etl.extract.trending_games import extract_top_5_trending_games
from etl.extract.trending_games import extract_trending_games_stats_overview
from etl.extract.trending_games import extract_trending_games_historical_stats

url = "https://steamcharts.com/"

soup = parse_soup(
    url,
    "Parse the BeautifulSoup object for extracting the current top 5 trending games "
    "from Steam Charts."
)
extract_top_5_trending_games(soup)

trending_games_stats_overview = {
    "app_id":                        [],
    "game_image":                    [],
    "twenty_four_hour_peak_players": [],
    "all_time_peak_players":         []
}
trending_games_historical_stats = {}

# Parse the scraped data of the current top 5 trending games from a JSON file
top_5_trending_games = parse_top_5_trending_games(
    "data/input/top_5_trending_games.json"
)

for number, app_id in enumerate(top_5_trending_games["app_id"]):
    soup = parse_soup(
        url + "app/" + app_id,
        "Parse the BeautifulSoup object for the chart page of the current number "
        f"{number + 1} trending game from Steam Charts."
    )

    # Scrape stats overview of all current trending game
    stats_overview = extract_trending_games_stats_overview(
        soup,
        app_id,
        f"Extract the stats overview data of the current number {number + 1} "
        "trending game from Steam Charts."
    )
    for key, value in stats_overview.items():
        trending_games_stats_overview[key].append(value)

    # Scrape historical stats of all current trending game
    historical_stats = extract_trending_games_historical_stats(
        soup,
        app_id,
        f"Extract the historical stats data of the current number {number + 1} "
        "trending game from Steam Charts.",
    )