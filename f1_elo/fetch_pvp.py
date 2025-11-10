from pandas import (
    DataFrame,
    read_csv,
    to_datetime,
)

from f1_elo.constants import (
    PVP_DATASET_PATH,
    SEASON_DRIVERS_PATH,
)


def read_pvp_results():
    """
    Reads and processes a game listing from a CSV file.

    Returns:
        A cleaned dataset with PVP results as a pandas.DataFrame.

    Raises:
        FileNotFoundError: If the PVP results file does not exist.
    """
    try:
        results = read_csv(PVP_DATASET_PATH, sep=';')

    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {PVP_DATASET_PATH}. Please specify correct path at constants.")
        
    results['game_date'] = to_datetime(results['game_date'], format='%Y-%m-%d').map(lambda x: x.date())
    results = results.sort_values(by='game_date')
    results['season'] = results['season'].astype(str)

    results['result'] = results['score'].map(_calc_result)
                        
    return results


def read_season_drivers():
    """
    Reads handled drivers per season indormation from a CSV file.

    Returns:
        Handled drivers per season dataset as a pandas.DataFrame.

    Raises:
        FileNotFoundError: If the drivers per season file does not exist.
    """
    try:
        season_drivers = read_csv(SEASON_DRIVERS_PATH, dtype={'season': 'string'})

    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {SEASON_DRIVERS_PATH}. Please specify correct path at constants.")

    return season_drivers


def _calc_result(score):
    """
    Determines the result of a match based on the score.

    Arguments:
        score (str): The match score in the format "X : Y", where X is the home team's score and Y is the away team's score.

    Returns:
        str: 'win1' if the home team wins, 'draw' if the match is a draw, or 'win2' if the away team wins.
    """
    scores = [int(x.strip()) for x in score.split(':')]
    if scores[0] > scores[1]:
        return 'win1'

    elif scores[0] == scores[1]:
        return 'draw'

    else:
        return 'win2'
