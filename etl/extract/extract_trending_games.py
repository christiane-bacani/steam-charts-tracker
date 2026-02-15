"""
Extract top 5 current trending games from the Steam Charts.
"""
import requests
from bs4 import BeautifulSoup
from logs.etl_pipeline_logs import etl_pipeline_logs

def extract_and_parse_soup(url: str) -> BeautifulSoup | None:
    """
    Extract and parse BeautifulSoup from the Steam Charts website.

    :param url: Website URL to be extracted and parsed as a BeautifulSoup
        object
    :type url: str

    :return: BeautifulSoup object representing the web-page from the url, NoneType if
        non-existent
    :rtype: BeautifulSoup | None
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0"
    }
    response = requests.get(url=url, headers=headers)

    if response.status_code != 200:
        etl_pipeline_logs(
            "EXTRACT",
            "Extract and parse BeautifulSoup object.",
            "FAILED",
            None
        )
        return None

    soup = BeautifulSoup(response.text, "html.parser")

    etl_pipeline_logs(
            "EXTRACT",
            "Extract and parse BeautifulSoup object.",
            "SUCCESSFUL",
            None
    )
    return soup

def extract_trending_games_table(soup: BeautifulSoup | None) -> dict[str, list]:
    """
    Extract top 5 trending games table from the Steam Charts website.

    :param soup: BeautifulSoup object representing the web-page from the url, NoneType
        if non-existent
    :type soup: BeautifulSoup | None

    :return: Top 5 current trending games dictionary:\n
        `{app_id: [], app_name: [], change_24h_pct: [], current_no_of_players: []}`
    :rtype: dict[str, list]
    """
    result = {
        "app_id": [],
        "app_name": [],
        "change_24h_pct": [],
        "current_no_of_players": []
    }

    if soup is None:
        etl_pipeline_logs(
            "EXTRACT",
            "Extract the top 5 current trending games on Steam Charts.",
            "FAILED",
            None
        )
        return result

    try:
        body_tag = soup.find("body")
        div_tag_with_content_wrapper_id = body_tag.find(
            "div",
            attrs={
                "id": "content-wrapper"
            }
        )

        div_tag_with_content_class = div_tag_with_content_wrapper_id.find(
            "div",
            attrs={
                "class": "content"
            }
        )
        table_tag = div_tag_with_content_class.find(
            "table",
            attrs={
                "id": "trending-recent"
            }
        )
        tbody_tag = table_tag.find("tbody")
        list_of_all_table_row_tags  = tbody_tag.find_all("tr")

        # Iterate over the trending games table to get the data using for-loop
        for table_row_tag in list_of_all_table_row_tags:
            list_of_all_table_data_tags = table_row_tag.find_all("td")

            anchor_tag = list_of_all_table_data_tags[0].find("a")
            app_id = anchor_tag["href"]
            app_id = str(app_id)
            app_id = app_id.replace("/app", "app")

            app_name = anchor_tag.get_text()
            app_name = str(app_name)

            change_24h_pct = list_of_all_table_data_tags[1]
            change_24h_pct = change_24h_pct.get_text()
            change_24h_pct = str(change_24h_pct)

            current_no_of_players = list_of_all_table_data_tags[3]
            current_no_of_players = current_no_of_players.get_text()
            current_no_of_players = str(current_no_of_players)

            result["app_id"].append(app_id)
            result["app_name"].append(app_name)
            result["change_24h_pct"].append(change_24h_pct)
            result["current_no_of_players"].append(current_no_of_players)

        etl_pipeline_logs(
            "EXTRACT",
            "Extract the top 5 current trending games on Steam Charts.",
            "SUCCESSFUL",
            None
        )
        return result

    except Exception as error_message:
        etl_pipeline_logs(
            "EXTRACT",
            "Extract the top 5 current trending games on Steam Charts.",
            "FAILED",
            error_message
        )
        return result

def extract_player_concurrency_data(
        soup: BeautifulSoup | None,
        trending_game_index: int
) -> dict[str, dict]:
    """
    Extract the player concurrency data of a specific current trending game on Steam
    Charts.

    :param soup: BeautifulSoup object representing the web-page from the url, NoneType
        if non-existent
    :type soup: BeautifulSoup | None

    :param trending_game_index: Current trending game index
    :type trending_game_index: int

    :return: Player concurrency data dictionary:\n
        `{app_name: "", app_logo: "", peak_no_of_players_24h: "",
        all_time_peak_no_of_players: ""}`
    :rtype: dict[str, str]
    """
    result = {
        "app_name": "",
        "app_logo": "",
        "peak_no_of_players_24h": "",
        "all_time_peak_no_of_players": ""
    }

    number = trending_game_index + 1

    if soup is None:
        etl_pipeline_logs(
            "EXTRACT",
            f"Extract the player concurrency data of the number {number} trending "
            "game on Steam Charts."
            "FAILED",
            None
        )
        return result

    try:
        body_tag = soup.find('body')
        div_tag_with_content_wrapper_id = body_tag.find(
            "div",
            attrs={
                "id": "content-wrapper"
            }
        )

        # Extract the application name
        app_name_tag = div_tag_with_content_wrapper_id.find(
            "h1",
            attrs={
                "id": "app-title"
            }
        )
        app_name = app_name_tag.get_text()
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

        # Extract the peak concurrent players within the time period of 24-Hours
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

        # Extract the all-time peak concurrent players
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
            f"Extract the player concurrency data of the number {number} trending "
            "game on Steam Charts."
            "SUCCESSFUL",
            None
        )
        return result

    except Exception as error_message:
        etl_pipeline_logs(
            "EXTRACT",
            f"Extract the player concurrency data of the {number} trending "
            "game on Steam Charts.",
            "FAILED",
            error_message
        )
        return result

def extract_historical_player_stats_table(
        soup: BeautifulSoup | None,
        trending_game_index_no: int
) -> dict[str, dict]:
    """
    Extract the historical player statistics table of a current trending game.

    :param soup: BeautifulSoup object representing the web-page from the url, NoneType
        if non-existent
    :type soup: BeautifulSoup | None

    :param trending_game_index_no: Current trending game index
    :type trending_game_index_no: int

    :return: Historical player data dictionary:\n
        `{period_name: [], avg_no_of_players: [],  avg_no_of_players_gain: [],
        pct_gain: [], peak_no_of_players: []}`
    :rtype: dict[str, dict]
    """
    result = {
        "period_name": [],
        "avg_no_of_players": [],
        "avg_no_of_players_gain": [],
        "pct_gain": [],
        "peak_no_of_players": []
    }

    number = trending_game_index_no + 1

    if soup is None:
        etl_pipeline_logs(
            "EXTRACT",
            f"Extract the historical player statistics of the number {number} trending "
            "game on Steam Charts.",
            "FAILED",
            None
        )
        return result

    try:
        body_tag = soup.find('body')
        div_tag_with_content_wrapper_id = body_tag.find(
            "div",
            attrs={
                "id": "content-wrapper"
            }
        )

        div_tag_with_content_class = div_tag_with_content_wrapper_id.find_all(
            "div",
            attrs={
                "class": "content"
            }
        )[2]
        table_tag = div_tag_with_content_class.find(
            "table",
            attrs={
                "class": "common-table"
            }
        )
        tbody_tag = table_tag.find("tbody")
        list_of_all_table_row_tags = tbody_tag.find_all("tr")

        # Iterate over the historical player stats table to get the data using for-loop
        for table_row_tag in list_of_all_table_row_tags:
            list_of_all_table_data_tags = table_row_tag.find_all("td")

            cells = []

            for table_data_tag in list_of_all_table_data_tags:
                cell = table_data_tag.get_text()
                cell = str(cell)
                cells.append(cell)

            result["period_name"].append(cells[0])
            result["avg_no_of_players"].append(cells[1])
            result["avg_no_of_players_gain"].append(cells[2])
            result["pct_gain"].append(cells[3])
            result["peak_no_of_players"].append(cells[4])

        etl_pipeline_logs(
            "EXTRACT",
            f"Extract the historical player statistics of the number {number} trending "
            "game on Steam Charts.",
            "SUCCESSFUL",
            None
        )
        return result

    except Exception as error_message:
        etl_pipeline_logs(
            "EXTRACT",
            f"Extract the historical player statistics of the number {number} "
            "trending game on Steam Charts.",
            "FAILED",
            error_message
        )
        return result