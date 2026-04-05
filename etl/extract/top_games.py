"""
Python module to extract the data about the top games (by current players) from
the Steam Charts website.
"""
import json
from bs4 import BeautifulSoup
from datetime import datetime
from zoneinfo import ZoneInfo

from logs.etl_pipeline_logs import provide_logs

def extract_top_10_games(soup: BeautifulSoup) -> None:
    """
    Scrape the top 10 games (by current players) from the Steam Charts website.

    Args:
        soup (BeautifulSoup): The parsed BeautifulSoup object
    """
    # Navigate the web-page to get the exact HTML elements for accurate scraping
    body_tag = soup.find("body")
    div_tag_with_content_wrapper_id = body_tag.find("div", attrs={
        "id": "content-wrapper"
    })

    table_tag = div_tag_with_content_wrapper_id.find("table", attrs={
        "id": "top-games"
    })
    tbody_tag = table_tag.find("tbody")
    table_row_tags = tbody_tag.find_all("tr")

    # Dictionary to store the scraped data
    data = {
        "rank": [],
        "app_id": [],
        "game_name": [],
        "current_players": [],
        "peak_players": [],
        "hours_played": [],
        "current_datetime": []
    }

    for table_row_tag in table_row_tags:
        table_data_tags = table_row_tag.find_all("td")

        # Extract the current rank of the game
        rank = table_data_tags[0].get_text()
        data["rank"].append(rank)

        # Extract the application ID
        app_id = table_data_tags[1].find("a")["href"]
        app_id = app_id.replace("/app/", "")
        data["app_id"].append(app_id)

        # Extract the game name
        game_name = table_data_tags[1].get_text()
        data["game_name"].append(game_name)

        # Extract the number of current players
        current_players = table_data_tags[2].get_text()
        data["current_players"].append(current_players)

        # Extract the number of peak players
        peak_players = table_data_tags[4].get_text()
        data["peak_players"].append(peak_players)

        # Extract the total number of hours played
        hours_played = table_data_tags[5].get_text()
        data["hours_played"].append(hours_played)

        # Extract the current date and time (timezone aware)
        current_datetime = datetime.now(
            ZoneInfo("Asia/Manila")
        ).strftime("%Y-%m-%d %H:%M:%S %Z%z")
        data["current_datetime"].append(current_datetime)

    provide_logs(
        "EXTRACT",
        "Extract the data of the current top 10 games (by current players) "
        "from Steam Charts",
        "SUCCESSFUL",
        None
    )
    # Store the scraped data to a JSON file from `data/input` directory
    with open("data/input/top_10_games.json", "w") as file:
        json.dump(data, file, indent=4)

def extract_top_games_stats_overview(
        soup: BeautifulSoup,
        app_id: str,
        description: str
) -> dict[str, str]:
    """
    Scrape the stats overview of a current top game (by current players).

    Args:
        soup (BeautifulSoup): The parsed BeautifulSoup object
        app_id (str): The application ID of a current top game (by current players)
        description (str): The whole description for extracting the data
            of a current top game's stats overview

    Returns:
        dict: The scraped data as a dictionary
    """
    # Navigate the web-page to get the exact HTML elements for accurate scraping
    body_tag = soup.find("body")
    div_tag_with_content_wrapper_id = body_tag.find("div", attrs={
        "id": "content-wrapper"
    })

    div_tag_with_app_heading_id = div_tag_with_content_wrapper_id.find("div", attrs={
        "id": "app-heading"
    })

    # Dictionary to store the scraped data
    data = {}

    # Extract the application ID
    data["app_id"] = app_id

    # Extract the game image as a URL
    game_image_url_path = div_tag_with_app_heading_id.find("img")["src"]
    game_image_url = "https://steamcharts.com" + game_image_url_path
    data["game_image"] = game_image_url

    div_tag_with_app_stat_classes = div_tag_with_app_heading_id.find_all("div", attrs={
        "class": "app-stat"
    })

    # Extract the number of peak players with a span of 24-hour period
    span_tag = div_tag_with_app_stat_classes[1].find("span")
    twenty_four_hour_peak_players = span_tag.get_text()
    data["twenty_four_hour_peak_players"] = twenty_four_hour_peak_players

    # Extract the number of peak players of all time
    span_tag = div_tag_with_app_stat_classes[2].find("span")
    all_time_peak_players = span_tag.get_text()
    data["all_time_peak_players"] = all_time_peak_players

    # Extract the current date and time (timezone aware)
    current_datetime = datetime.now(
        ZoneInfo("Asia/Manila")
    ).strftime("%Y-%m-%d %H:%M:%S %Z%z")
    data["current_datetime"] = current_datetime

    provide_logs(
        "EXTRACT",
        description,
        "SUCCESSFUL",
        None
    )
    return data