"""
Orchestrate and executes workflow of the Steam Charts ETL Pipeline.
"""
from etl.extract.extract_page_content import extract_and_parse_soup
from etl.extract.extract_trending_games import extract_trending_games_table
from etl.extract.extract_trending_games import extract_app_summary
from etl.extract.extract_trending_games import extract_historical_player_stats_table
from etl.extract.extract_top_games import extract_top_games_table

base_url = "https://steamcharts.com/"

# Top 5 current trending games
soup = extract_and_parse_soup(base_url)
trending_games_dict = extract_trending_games_table(soup)

trending_app_summary_dict = {}
trending_app_historical_player_stats_dict = {}

# Get the player concurrency data and historical player stats of every trending game
for number in range(5):
    path = trending_games_dict["app_id"][number]
    url = base_url + path

    app_name = trending_games_dict["app_name"][number]
    app_name = str(app_name)

    soup = extract_and_parse_soup(url)

    app_summary_dict = extract_app_summary(soup, number)
    trending_app_summary_dict[app_name] = app_summary_dict

    historical_player_stats_dict = extract_historical_player_stats_table(soup, number)
    trending_app_historical_player_stats_dict[app_name] = historical_player_stats_dict

# Top games by current players
base_url = "https://steamcharts.com/top"
soup = extract_and_parse_soup(base_url)
top_games_dict = extract_top_games_table(soup)

base_url = "https://steamcharts.com/"

for number in range(25):
    path = top_games_dict["app_id"][number]
    url = base_url + path

    app_name = top_games_dict["app_name"][number]
    app_name = str(app_name)

    soup = extract_and_parse_soup(url)