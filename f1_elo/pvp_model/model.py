from copy import deepcopy
from datetime import date

from numpy import log as np_log
from pandas import (
    DataFrame,
    concat,
)

from f1_elo.constants import RESULT_RATIOS
from f1_elo.fetch_pvp import read_season_drivers


class EloCalculation:
    """
    Class represents elo rating calculation logic.
    """
    def __init__(
        self,
        elo_game_value,
        num_rounds_degree=0,
        num_drivers_degree=0,
        new_agent_alpha=2,
        calibrated_rating=2000,
        num_calibrated_drivers=20,
        default_rating=1800,        
        saturation_rounds=10,
    ):
        """
        Initializes an instance with paths to data files and Elo configuration parameters.

        Arguments:
            elo_game_value (float): default Elo adjustment factor.
            num_rounds_degree (float): degree parameter for number of rounds scaling.
            num_drivers_degree (float): degree parameter for number of drivers scaling.
            new_agent_alpha (float): coefficient controlling the new agent adjustment.
            calibrated_rating (float): target mean rating for top drivers.
            num_calibrated_drivers (int): number of top drivers to use for calibration.
            default_rating (float): default Elo rating for new drivers.
            saturation_rounds (int): rounds required to reach rating stability.
        """
        self.elo_game_value = elo_game_value
        self.num_rounds_degree = num_rounds_degree
        self.num_drivers_degree = num_drivers_degree
        self.calibrated_rating = calibrated_rating
        self.num_calibrated_drivers = num_calibrated_drivers
        self.default_rating = default_rating
        self.saturation_rounds = saturation_rounds
        self.new_agent_alpha = new_agent_alpha

        self.season_drivers = read_season_drivers()
        self.drivers_years_active = self.season_drivers.groupby(['team']).agg(
            first_season=('season', 'min'),
            last_season=('season', 'max'),
        ).reset_index()

        self.elo_ratings_dict = {}
        self.num_games_per_team = {}

        self.temp_ratings = []
        self.total_rating = None
        self.output = []

    @property
    def log_loss(self):
        """
        Compute the mean log loss across all processed games.

        Returns:
            mean log loss if available as a float, otherwise None.
        """
        if len(self.output) == 0:
            return None
            
        output_df = DataFrame(self.output)
        log_loss = output_df['log_loss'].mean()

        return log_loss

    def reset_log_loss(self):
        """
        Reset stored log loss output.
        """
        self.output = []

    def run_pipeline(self, results):
        """
        Run ratings for every season bases on race results.

        Arguments:
            pvp_results (pandas.DataFrame): dataset with PVP interactions history.
        """
        seasons = sorted(results['season'].unique())

        for season in seasons:
            include_ix = self.drivers_years_active['first_season'] == season
            teams_to_include = self.drivers_years_active['team'].loc[include_ix].to_list()
            for team in teams_to_include:
                self.elo_ratings_dict[team] = self.elo_ratings_dict.get(team, self.default_rating)

            self._calibrate_ratings(season=season)

            self.run_season(pvp_results=results, season=season)

            exclude_ix = self.drivers_years_active['last_season'] == season
            teams_to_exclude = self.drivers_years_active['team'].loc[exclude_ix].to_list()
            for team in teams_to_exclude:
                _ = self.elo_ratings_dict.pop(team)

        self.total_rating = concat(self.temp_ratings)

    def run_round(self, pvp_results):
        """
        Run Elo updates for a single race.

        Arguments:
            pvp_results (pandas.DataFrame): Race results in PVP format.
        """
        # need a copy as expected result should be calculated on a pre race ratings for all race interactions.
        round_elo_rating_dict = deepcopy(self.elo_ratings_dict)

        for _, row in pvp_results.iterrows():
            if (row['home_team'] not in self.elo_ratings_dict) or (row['away_team'] not in self.elo_ratings_dict):
                continue

            round_elo_rating_dict = self.update_elo(interaction=row.to_dict(), elo_dict=round_elo_rating_dict)

        self.elo_ratings_dict = round_elo_rating_dict

        round_drivers = set(pvp_results['home_team']).union(set(pvp_results['away_team']))
        for driver in round_drivers:
            self.num_games_per_team[driver] = self.num_games_per_team.get(driver, 0) + 1

    def run_season(self, pvp_results, season):
        """
        Execute Elo updates for all rounds within a season.

        Arguments:
            pvp_results (pandas.DataFrame): Dataset with race history.
            season (str): Season identifier.
        """
        self._log_current_rating(rating_date=date(int(season), 1, 1))
        season_rounds = sorted(pvp_results['round'].loc[pvp_results['season'] == season].unique())
        for season_round in season_rounds:
            round_listing = pvp_results.loc[(pvp_results['season'] == season) & (pvp_results['round'] == season_round)]
            self.run_round(pvp_results=round_listing)
            self._log_current_rating(rating_date=round_listing['game_date'].iloc[0])

        self._log_current_rating(rating_date=date(int(season), 12, 31))

    def update_elo(self, interaction, elo_dict):
        """
        Update Elo ratings based on race outcome.

        Arguments:
            interaction (dict): PVP race reult information.
            elo_dict (dict): Current Elo ratings.

        Returns:
            Updated Elo ratings after race interactions as a dictionary.
        """    
        home_team = interaction['home_team']
        away_team = interaction['away_team']
        interaction['result_ratio'] = RESULT_RATIOS.get(interaction['result'])
        
        diff = self.elo_ratings_dict[home_team] - self.elo_ratings_dict[away_team]
        interaction['exp_result'] = self._calc_expected_result(diff)
        interaction['log_loss'] = self._calc_log_loss(interaction=interaction)

        interaction['home_team_rating'] = self.elo_ratings_dict[home_team]
        interaction['away_team_rating'] = self.elo_ratings_dict[away_team]
        self.output.append(interaction)

        home_team_rating_change = self._calc_rating_change(game=interaction, team=home_team)
        elo_dict[home_team] += home_team_rating_change

        away_team_rating_change = self._calc_rating_change(game=interaction, team=away_team)
        elo_dict[away_team] -= away_team_rating_change

        return elo_dict

    @staticmethod
    def _calc_expected_result(difference):
        """
        Calculates the expected result of a match based on the Elo rating difference between two teams.

        Arguments:
            difference (float): The Elo rating difference between the home team and the away team.

        Returns:
            The expected probability of the home team winning the match as a float.
        """
        return 1 / (10 ** (-difference / 400) + 1)

    @staticmethod
    def _calc_log_loss(interaction):
        """
        Calculates the log loss value for PVP interaction between two drivers.

        Arguments:
            interaction (dict): PVP interaction between two drivers.

        Returns:
            The elog loss value as a float.
        """
        res_ratio = interaction['result_ratio']
        exp_result = interaction['exp_result']
        log_loss = -res_ratio * np_log(exp_result) - (1 - res_ratio) * np_log(1 - exp_result)

        return log_loss

    def _calc_new_agent_multiplier(self, num_rounds):
        """
        Compute adjustment multiplier for newly introduced drivers.

        Arguments:
            num_rounds (int): Number of rounds a driver has participated in.

        Returns:
            Adjustment multiplier for rating change as a float.
        """
        multiplier = 1 + self.new_agent_alpha * (1 - min(self.saturation_rounds, num_rounds)/self.saturation_rounds)**2

        return multiplier

    def _calc_rating_change(self, game, team):
        """
        Compute rating change for a team based on PVP result and scaling parameters.

        Arguments:
            game (dict): PVP interaction information including expected and actual results.
            team (str): Team identifier.

        Returns:
            Rating adjustment for the team as a result of PVP interaction as a float.
        """
        num_races_coef = game['num_races'] ** self.num_rounds_degree 
        num_teams_coef = game['num_drivers'] ** self.num_drivers_degree
        elo_game_ratio = self.elo_game_value/(num_races_coef*num_teams_coef)

        num_rounds = self.num_games_per_team.get(team, 0)
        multiplier = self._calc_new_agent_multiplier(num_rounds=num_rounds)
        rating_change = (game['result_ratio'] - game['exp_result']) * elo_game_ratio * multiplier

        return rating_change

    def _calibrate_ratings(self, season):
        """
        Adjust all ratings so that the top drivers’ mean matches the calibrated rating.

        Arguments:
            season (str): Season identifier.
        """
        season_teams = self.season_drivers[['team']].loc[self.season_drivers['season'] == season]
        season_teams['rating'] = season_teams['team'].map(self.elo_ratings_dict)
        season_teams = season_teams.sort_values('rating', ascending=False).reset_index(drop=True)
        if len(season_teams) > self.num_calibrated_drivers:
            season_teams = season_teams.iloc[:self.num_calibrated_drivers]

        calibration_delta = self.calibrated_rating - season_teams['rating'].mean()
        for team in self.elo_ratings_dict:
            self.elo_ratings_dict[team] += calibration_delta

    def _log_current_rating(self, rating_date):
        """
        Log current Elo ratings with a specified date.

        Arguments:
            rating_date (datetime.date): Date of logged ratings.
        """
        temp_rating = DataFrame.from_dict(self.elo_ratings_dict, orient='index').reset_index()
        temp_rating = temp_rating.rename(columns={'index': 'team', 0: 'rating'})
        temp_rating['date'] = rating_date
        self.temp_ratings.append(temp_rating)
