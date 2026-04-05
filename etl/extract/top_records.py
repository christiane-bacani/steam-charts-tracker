"""
Python module to extract the data about the top records from Steam Charts website.
"""
import json
from bs4 import BeautifulSoup
from datetime import datetime
from zoneinfo import ZoneInfo

from logs.etl_pipeline_logs import provide_logs

def extract_top_10_records(soup: BeautifulSoup) -> None:
    """
    Scrape the top 10 records from the Steam Charts website.

    Args:
        soup (BeautifulSoup): The parsed BeautifulSoup object
    """
    # Navigate the web-page to get the exact HTML elements for accurate scraping
    body_tag = soup.find("body")
    div_tag_with_content_wrapper_id = body_tag.find("div", attrs={
        "id": "content-wrapper"
    })

    table_tag = div_tag_with_content_wrapper_id.find("table", attrs={
        "id": "toppeaks"
    })
    tbody_tag = table_tag.find("tbody")
    table_row_tags = tbody_tag.find_all("tr")

    # Dictionary to store the scraped data
    data = {
        "rank": [],
        "app_id": [],
        "game_name": [],
        "peak_players": [],
        "time": [],
        "current_datetime": []
    }

    for number, table_row_tag in enumerate(table_row_tags):
        table_data_tags = table_row_tag.find_all("td")
        
        # Extract the current rank of the game
        rank = f"{number + 1}."
        data["rank"].append(rank)

        # Extract the application ID
        app_id = table_data_tags[0].find("a")["href"]
        app_id = app_id.replace("/app/", "")
        data["app_id"].append(app_id)

        # Extract the game name
        game_name = table_data_tags[0].get_text()
        data["game_name"].append(game_name)

        # Extract the number of peak players
        peak_players = table_data_tags[1].get_text()
        data["peak_players"].append(peak_players)

        # Extract the time (consisting of month and year) that
        # it records the number of peak players
        time = table_data_tags[2].get_text()
        data["time"].append(time)

        # Extract the current date and time (timezone aware)
        current_datetime = datetime.now(
            ZoneInfo("Asia/Manila")
        ).strftime("%Y-%m-%d %H:%M:%S %Z%z")
        data["current_datetime"].append(current_datetime)

    provide_logs(
        "EXTRACT",
        "Extract the data of the current top 10 records from Steam Charts."
        "SUCCESSFUL",
        None
    )
    # Store the scraped data to a JSON file from `data/input` directory
    with open("data/input/top_10_records.json", "w") as file:
        json.dump(data, file, indent=4)