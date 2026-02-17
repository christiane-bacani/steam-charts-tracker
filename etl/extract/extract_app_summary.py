"""
Extract the application summary of a current trending game or top game on Steam Charts.
"""
from bs4 import BeautifulSoup
from logs.etl_pipeline_logs import etl_pipeline_logs

def extract_app_summary(
        soup: BeautifulSoup | None,
        game_index_no: int,
        helper: str
) -> dict[str, str]:
    """
    Extract the application summary of a specific game on Steam Charts.

    :param soup: BeautifulSoup object representing the web-page from the url, NoneType
        if non-existent
    :type soup: BeautifulSoup | None

    :param game_index_no: Steam Charts game index number
    :type game_index_no: int

    :param helper: Helper message to determine what kind of game is extracted:
        `Trending or Top`
    :type helper: str

    :return: Application summary dictionary:\n
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

    number = game_index_no + 1

    if helper == "Trending":
        job_description = "Extract the application summary data of the number "
        f"{number} trending game on Steam Charts."

    else:
        job_description = "Extract the application summary data of the number "
        f"{number} top game on Steam Charts."