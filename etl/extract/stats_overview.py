"""
This module extracts statistical overview data for trending or top games from Steam
Charts using BeautifulSoup. It provides functions to retrieve game name, logo, and
player statistics for ETL processing, supporting both trending and top game
categories.
"""
from bs4 import BeautifulSoup
from logs.etl_pipeline_logs import etl_pipeline_logs

def extract_game_stats_overview(
        soup: BeautifulSoup | None,
        game_index_no: int,
        helper: str
) -> dict[str, str]:
    """
    Extract the statistics overview of a specific game on Steam Charts.

    :param soup: BeautifulSoup object representing the web-page from the url, NoneType
        if non-existent
    :type soup: BeautifulSoup | None

    :param game_index_no: Game index number reflected on the Steam Charts website
    :type game_index_no: int

    :param helper: Helper message to determine the type of game to extract:
        `Trending or Top`
    :type helper: str

    :return: Application summary dictionary:\n
        `{app_name: str, app_logo: str, peak_no_of_players_24h: int,
        all_time_peak_no_of_players: int}`
    :rtype: dict[str, str]
    """
    result = {
        "app_name": "",
        "app_logo": "",
        "peak_no_of_players_24h": "",
        "all_time_peak_no_of_players": ""
    }

    number = game_index_no + 1

    if helper == "Trending":
        job_description = f"Extract the statistics overview of the number {number} "
        "trending game on Steam Charts."

    else:
        job_description = f"Extract the statistics overview of the top {number} game "
        "on Steam Charts."

    if soup is None:
        etl_pipeline_logs(
            "EXTRACT",
            job_description,
            "FAILED",
            None
        )

    try:
        body_tag = soup.find("body")
        div_tag_with_content_wrapper_id = body_tag.find(
            "div",
            attrs={
                "id": "content-wrapper"
            }
        )

        # Extract the application name
        h1_tag_with_app_title = div_tag_with_content_wrapper_id.find(
            "h1",
            attrs={
                "id": "app-title"
            }
        )
        app_name = h1_tag_with_app_title.get_text()
        app_name = str(app_name)
        result["app_name"] = app_name

        div_tag_with_app_heading_id = div_tag_with_content_wrapper_id.find(
            "div",
            attrs={
                "id": "app-heading"
            }
        )

        # Extract the application logo
        img_tag_with_app_image_class = div_tag_with_app_heading_id.find(
            "img",
            attrs={
                "class": "app-image"
            }
        )
        app_logo_path = img_tag_with_app_image_class["src"]
        app_logo = "https://steamcharts.com" + app_logo_path
        result["app_logo"] = app_logo

        # Extract the peak number of players within the time period of 24-hours
        peak_no_of_players_24h_tag = div_tag_with_app_heading_id.find_all(
            "div",
            attrs={
                "class": "app-stat"
            }
        )[1]
        peak_no_of_players_24h = peak_no_of_players_24h_tag.get_text()
        peak_no_of_players_24h = peak_no_of_players_24h.replace("24-hour peak", "")
        peak_no_of_players_24h = int(peak_no_of_players_24h.strip())
        result["peak_no_of_players_24h"] = peak_no_of_players_24h

        # Extract the all-time peak number of players
        all_time_peak_no_of_players_tag = div_tag_with_app_heading_id.find_all(
            "div",
            attrs={
                "class": "app-stat"
            }
        )[2]

        all_time_peak_no_of_players = all_time_peak_no_of_players_tag.get_text()
        all_time_peak_no_of_players = all_time_peak_no_of_players.replace("all-time peak", "")
        all_time_peak_no_of_players = int(all_time_peak_no_of_players.strip())
        result["all_time_peak_no_of_players"] = all_time_peak_no_of_players

        etl_pipeline_logs(
            "EXTRACT",
            job_description,
            "SUCCESFUL",
            None
        )
        return result

    except Exception as error_message:
        etl_pipeline_logs(
            "EXTRACT",
            job_description,
            "FAILED",
            error_message
        )
        return result