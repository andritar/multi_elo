from pandas import (
    DataFrame,
    read_csv,
    to_datetime,
)


def read_game_listing(path):
    """
    Reads and processes a game listing from a CSV file.

    Arguments:
        path (str): Path to the CSV file containing game listings.

    Returns:
        pandas.DataFrame: A cleaned DataFrame with game listings.

    Raises:
        Exception: If any game listing contains missing scores (indicated by '- : -').
    """
    listing = read_csv(path, sep=';')
    listing['game_date'] = to_datetime(listing['game_date'], format='%Y-%m-%d').map(lambda x: x.date())
    listing = listing.sort_values(by='game_date')
    listing['season'] = listing['season'].astype(str)

    listing['result'] = listing['score'].map(_calc_result)
                        
    return listing


def load_elo_ratings(path):
    """
    Retrieves initial Elo ratings for teams from a CSV file.

    Arguments:
        path (str): Path to the CSV file containing Elo ratings.

    Returns:
        dict: A dictionary mapping teams to their corresponding Elo ratings.
    """
    elo_ratings = read_csv(path)
    elo_ratings_dict = elo_ratings.set_index('team')['rating'].to_dict()

    return elo_ratings_dict


def save_prepare_season_initial_ratings(elo_ratings_dict, team_season_path, output_elo_ratings_path):
    """
    Prepares and saves initial Elo ratings for teams in the earliest season of a dataset.

    Arguments:
        elo_ratings_dict (dict): A dictionary mapping teams to their Elo ratings.
        team_season_path (str): Path to the CSV file with team-season data.
        output_elo_ratings_path (str): Path to save the initial team Elo ratings with league averages.
    """
    team_seasons = read_csv(team_season_path)
    team_seasons = team_seasons.loc[team_seasons['season'] == team_seasons['season'].min()]

    final_rating = DataFrame.from_dict(elo_ratings_dict, orient='index').reset_index()
    final_rating = final_rating.rename(columns={'index': 'team', 0: 'rating'})
    team_seasons = team_seasons.merge(final_rating, how='left', on='team')
    
    team_seasons['avg_league_rating'] = team_seasons.groupby(['tournament']).transform('mean', 'rating').round(2)
    team_seasons = team_seasons[['team', 'avg_league_rating']].rename(columns={'avg_league_rating': 'rating'})
    team_seasons.to_csv(output_elo_ratings_path, index=False)


def save_ratings(elo_ratings_dict, output_elo_ratings_path):
    """
    Saves Elo ratings.

    Arguments:
        elo_ratings_dict (dict): A dictionary mapping teams to their Elo ratings.
        output_elo_ratings_path (str): Path to save the Elo ratings.
    """
    rating_df = DataFrame.from_dict(elo_ratings_dict, orient='index').reset_index()
    rating_df = rating_df.rename(columns={'index': 'team', 0: 'rating'})
    rating_df['rating'] = rating_df['rating'].round(2)

    rating_df.to_csv(output_elo_ratings_path, index=False)


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
