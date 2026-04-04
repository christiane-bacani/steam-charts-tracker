"""
Python module to extract the data about the top games from the Steam Charts website.
"""
import json
from bs4 import BeautifulSoup
from datetime import datetime
from zoneinfo import ZoneInfo

from logs.etl_pipeline_logs import provide_logs

def extract_top_10_games(soup: BeautifulSoup) -> None:
    """
    Scrape the top 10 games from the Steam Charts website.

    Args:
        soup (BeautifulSoup): The parsed BeautifulSoup object
    """
    # Navigate the web-page to get the exact HTML elements for accurate scraping
    body_tag = soup.find("body")
    div_tag_with_content_wrapper_id = body_tag.find("div", attrs={
        "id": "content-wrapper"
    })

    table_tag = div_tag_with_content_wrapper_id.find("table", attrs={"id": "top-games"})
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
        "Extract the data of the current top 10 games from Steam Charts."
        "SUCCESSFUL",
        None
    )
    # Store the scraped data to a JSON file from `data/input` directory
    with open("data/input/top_10_games.json", "w") as file:
        json.dump(data, file, indent=4)