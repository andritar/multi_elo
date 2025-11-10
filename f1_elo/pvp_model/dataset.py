from pandas import concat

from f1_elo.fetch_pvp import (
    read_pvp_results,
    read_season_drivers,
)


def get_model_ready_pvp_results():
    """
    Prepare pairwise driver comparison data for model training.

    Returns:
        Cleaned and enriched PVP results dataset as a pandas.DataFrame.
    """
    results = read_pvp_results()
    results = _filter_not_handled_drivers(results=results)
    results = _calc_race_season_stats(results=results)

    return results


def _filter_not_handled_drivers(results):
    """
    Keep only PVP results involving drivers present in the season dataset.

    Arguments:
        results (DataFrame): PVP results dataset.

    Returns:
        Filtered PVP results dataset as a pandas.DataFrame.
    """
    season_drivers = read_season_drivers()
    
    first_season_drivers = season_drivers.rename(columns={'team': 'home_team'})
    results = results.merge(first_season_drivers, how='inner', on=['home_team', 'season', 'tournament'])

    second_season_drivers = season_drivers.rename(columns={'team': 'away_team'})
    results = results.merge(second_season_drivers, how='inner', on=['away_team', 'season', 'tournament'])

    return results


def _calc_race_season_stats(results):
    """
    Calculate race-level and season-level statistics for the filtered PVP dataset.

    Arguments:
        results (DataFrame): Filtered PVP results dataset.

    Returns:
        PVP results dataset with calculated stats as a pandas.DataFrame.
    """
    home_driver_info = results[['home_team', 'season', 'round', 'tournament']].rename(columns={'home_team': 'team'})
    away_driver_info = results[['away_team', 'season', 'round', 'tournament']].rename(columns={'away_team': 'team'})

    driver_info = concat([home_driver_info, away_driver_info], ignore_index=True).drop_duplicates()

    round_stats = driver_info.groupby(['tournament', 'season', 'round']).agg(num_drivers=('team', 'nunique')).reset_index()
    results = results.merge(round_stats, how='left', on=['tournament', 'season', 'round'])

    season_stats = driver_info.groupby(['tournament', 'season']).agg(num_races=('round', 'nunique')).reset_index()
    results = results.merge(season_stats, how='left', on=['tournament', 'season'])

    return results
