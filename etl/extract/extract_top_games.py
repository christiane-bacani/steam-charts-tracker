"""
Extract top games by current players from the Steam Charts.
"""
import requests
from bs4 import BeautifulSoup
from logs.etl_pipeline_logs import etl_pipeline_logs

def extract_top_games_table(soup: BeautifulSoup | None) -> dict[str, list]:
    """
    Extract top games by current players table from the Steam Charts website.

    :param soup: BeautifulSoup object representing the web-page from the url, NoneType
        if non-existent
    :type soup: BeautifulSoup | None

    :return: Top games by current players dictionary:\n
        `{app_id: [], app_name: [],
        current_no_of_players: [], peak_no_of_players_30d: [],
        total_no_of_hours_played_30d: []}`
    :rtype: dict[str, list]
    """
    result = {
        "app_id": [],
        "app_name": [],
        "current_no_of_players": [],
        "peak_no_of_players_30d": [],
        "total_no_of_hours_played_30d": []
    }

    if soup is None:
        etl_pipeline_logs(
            "EXTRACT",
            "Extract the top games by current players on Steam Charts.",
            "FAILED",
            None
        )
        return result

    try:   
        body_tag = soup.find("body")
        div_tag_with_content_wrapper = body_tag.find(
            "div",
            attrs={
                "id": "content-wrapper"
            }
        )

        div_tag_with_content_wrapper = div_tag_with_content_wrapper.find(
            "div",
            attrs={
                "class": "content"
            }
        )
        table_tag = div_tag_with_content_wrapper.find(
            "table",
            attrs={
                "id": "top-games",
                "class": "common-table"
            }
        )
        tbody_tag = table_tag.find("tbody")
        list_of_all_table_row_tags = tbody_tag.find_all("tr")

        for table_row_tag in list_of_all_table_row_tags:
            list_of_all_table_data_tags = table_row_tag.find_all("td")

            cells = []

            for cell_number, table_data_tag in enumerate(list_of_all_table_data_tags):
                if cell_number == 0 or cell_number == 3:
                    continue

                elif cell_number == 1:
                    anchor_tag = table_data_tag.find("a")

                    app_id = anchor_tag["href"]
                    app_id = str(app_id)
                    app_id = app_id.replace("/app", "app")

                    app_name = anchor_tag.get_text()

                    cells.append(app_id)
                    cells.append(app_name)

                else:
                    cell = table_data_tag.get_text()
                    cells.append(cell)

            result["app_id"].append(cells[0])
            result["app_name"].append(cells[1])
            result["current_no_of_players"].append(cells[2])
            result["peak_no_of_players_30d"].append(cells[3])
            result["total_no_of_hours_played_30d"].append(cells[4])

        etl_pipeline_logs(
            "EXTRACT",
            "Extract the top games by current players on Steam Charts.",
            "SUCCESSFUL",
            None
        )
        return result

    except Exception as error_message:
        etl_pipeline_logs(
            "EXTRACT",
            "Extract the top games by current players on Steam Charts.",
            "FAILED",
            error_message
        )
        return result