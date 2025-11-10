"""
Logic to calculate team, league and season combinations.
"""
from pandas import concat, read_csv

from f1_elo.constants import SEASON_DRIVERS_PATH
from f1_elo.fetch_pvp import read_pvp_results


def calc_season_drivers(min_num_games=1, start_rating = 2000):
    """
    Calculate drivers per each season and save it to specified location.

    Arguments:
        min_num_games (int, optional): Minimum games required for a team-season combo. Defaults to 1.
        start_rating (int, optional): Initial rating assigned to all teams. Defaults to 1400.
    """
    listing = read_pvp_results()

    games_stats = _get_num_games_stats(listing=listing)
    season_drivers = _calc_season_drivers_combos(games_stats=games_stats, min_num_games=min_num_games)
    season_drivers.to_csv(SEASON_DRIVERS_PATH, index=False)


def _calc_min_allowed_games_per_tournament(games_stats):
    """
    Calculates the minimum allowed games per tournament and season to consider a team as having participated in the tournament.

    Arguments:
        games_stats (pandas.DataFrame): dataset containing game statistics perteam, season and tournament.

    Returns:
        pandas.DataFrame: A dataset with minimum number of games required per tournament and season to consider
                          team as a one participated in tournament.
    """
    tournament_stats = games_stats.groupby(['season', 'tournament']).agg(max_games=('num_rounds', 'max')).reset_index()
    tournament_stats['min_allowed_num_games'] = tournament_stats['max_games'].map(_calc_min_games_to_include_team)
    tournament_stats = tournament_stats.drop(columns=['max_games'])

    return tournament_stats


def _calc_min_games_to_include_team(num_games):
    """
    Calculates the minimum number of games required to include a team based on the total games played.

    Arguments:
        num_games (int): Total number of games in a tournament.

    Returns:
        int: The calculated minimum number of games needed to include a team.
    """
    if num_games <= 2:
        return 1

    return 2


def _calc_season_drivers_combos(games_stats, min_num_games=1):
    """
    Calculates the valid team-season-tournament combinations based on game statistics.

    Arguments:
        games_stats (pandas.DataFrame): dataset containing game statistics per team, season and tournament.
        min_num_games (int, optional): Minimum number of games a team must have played to be included. Defaults to 1.

    Returns:
        pandas.DataFrame: A dataset containing valid combinations of tournament, season, and team.
    """
    games_stats = games_stats.loc[games_stats['num_rounds'] >= min_num_games]
    
    min_allowed_games_stats = _calc_min_allowed_games_per_tournament(games_stats=games_stats)
    games_stats = games_stats.merge(min_allowed_games_stats, how='left', on=['season', 'tournament'])
    games_stats = games_stats.loc[games_stats['num_rounds'] >= games_stats['min_allowed_num_games']]
    
    games_stats = games_stats.sort_values(['season', 'tournament', 'team'])
    games_stats = games_stats[['tournament', 'season', 'team']]

    return games_stats


def _get_num_games_stats(listing):
    """
    Calculates the number of games for each team in a tournament and season.

    Arguments:
        listing (pandas.DataFrame): dataset containing game data.

    Returns:
        pandas.DataFrame: A dataset containing the number of  total games for each team per tournament and season.
    """
    home_team_df = listing[['home_team', 'tournament', 'season', 'round']].rename(columns={'home_team': 'team'})
    away_team_df = listing[['away_team', 'tournament', 'season', 'round']].rename(columns={'away_team': 'team'})
    team_df = concat([home_team_df, away_team_df], ignore_index=True)

    round_stats = team_df.groupby(['team', 'tournament', 'season']).agg(num_rounds=('round', 'nunique')).reset_index()

    return round_stats
