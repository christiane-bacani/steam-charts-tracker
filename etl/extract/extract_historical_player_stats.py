"""
Extract the historical player statistics of a current trending game or top game on
Steam Charts.
"""
from bs4 import BeautifulSoup
from logs.etl_pipeline_logs import etl_pipeline_logs

def extract_historical_player_stats(
        soup: BeautifulSoup | None,
        game_index_no: int,
        helper: str
) -> dict[str, list]:
    """
    Extract the historical player statistics table of a current trending game or
    top game on Steam Charts.

    :param soup: BeautifulSoup object representing the web-page from the url, NoneType
        if non-existent
    :type soup: BeautifulSoup | None

    :param game_index_no: Steam Charts game index number
    :type game_index_no: int

    :param helper: Helper message to determine what kind of game is extracted:
        `Trending or Top`
    :type helper: str

    :return: Historical player data dictionary:\n
        `{period_name: [], avg_no_of_players: [],  avg_no_of_players_gain: [],
        pct_gain: [], peak_no_of_players: []}`
    :rtype: dict[str, list]
    """
    result = {
        "period_name": [],
        "avg_no_of_players": [],
        "pct_gain": [],
        "peak_no_of_players": []
    }

    number = game_index_no + 1