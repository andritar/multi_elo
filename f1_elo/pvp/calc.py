def calc_pvp_results(results):
    """
    Generate pairwise comparisons of drivers per race dataset.

    Arguments:
        results (pandas.DataFrame): Dataset with finished race results.

    Returns:
        pairwise driver comparison dataset as a pandas.DataFrame.
    """
    pvp_results = _combine_pvp_results(results=results)
    pvp_results = _format_output(pvp_results=pvp_results)

    return pvp_results


def filter_results(results):
    """
    Filter race results to include only finished races and convert positions to integers.

    Arguments:
        results (DataFrame): dataset containing race results.

    Returns:
        dataset with only finished race results and integer positions as a pandas.DataFrame.
    """
    finished_results = results.loc[results['position'] != '\\N']
    finished_results['position'] = finished_results['position'].astype(int)

    return finished_results


def _combine_pvp_results(results):
    """
    Compute pairwise comparisons of drivers in each race, generating a score string.

    Arguments:
        results (DataFrame): DataFrame containing finished race results.

    Returns:
        dataset with pairwise driver comparisons per race as a pandas.DataFrame.
    """
    teams_1 = results[['resultId', 'raceId', 'driver', 'position']].copy()
    teams_1 = teams_1.rename(columns={'resultId': 'resultId1', 'driver': 'driver1', 'position': 'position1'})

    teams_2 = results[['resultId', 'raceId', 'driver', 'position']].copy()
    teams_2 = teams_2.rename(columns={'resultId': 'resultId2', 'driver': 'driver2', 'position': 'position2'})

    pvp_results = teams_1.merge(teams_2, how='inner', on='raceId')

    pvp_results = pvp_results.loc[pvp_results['resultId1'] < pvp_results['resultId2']]
    pvp_results = pvp_results.loc[pvp_results['position1'] != pvp_results['position2']]
    pvp_results = pvp_results.loc[pvp_results['driver1'] != pvp_results['driver2']]

    pvp_results['score'] = pvp_results['position2'].astype(str) + ' : ' + pvp_results['position1'].astype(str)

    pvp_results = pvp_results.drop(columns=['resultId1', 'resultId2'])

    race_info = results[['raceId', 'season', 'round', 'game_date']].drop_duplicates()
    pvp_results = pvp_results.merge(race_info, how='left', on='raceId')

    return pvp_results


def _format_output(pvp_results):
    """
    Format pairwise driver comparison dataset to match the standardized match-style schema.

    Arguments:
        pvp_results (pandas.DataFrame): Pairwise driver comparison dataset.

    Returns:
        pandas.DataFrame: Formatted dataset with renamed columns and standardized fields.
    """
    pvp_results = pvp_results.rename(columns={
        'driver1': 'home_team',
        'driver2': 'away_team',
    })
    pvp_results = pvp_results.drop(columns=['position1', 'position2', 'raceId'])
    pvp_results['home_field'] = 'no'
    pvp_results['country'] = 'world'
    pvp_results['tournament'] = 'F1'

    pvp_results['result'] = pvp_results['score'].map(_calc_result)

    return pvp_results


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
