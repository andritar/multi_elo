from f1_elo.constants import (
    INIT_DATASET_END_DATE,
    TRAIN_DATASET_END_DATE,
    VALIDATION_DATASET_END_DATE,
)
from f1_elo.gradient import AbstractGradientOptimizer
from f1_elo.pvp_model.model import EloCalculation

from numpy.random import uniform


class EloGradientOptimizer(AbstractGradientOptimizer):
    """Gradient descent optimizer for tuning Elo model parameters."""

    def __init__(self, gradient_delta=0.0001):
        """
        Construct the object.

        Arguments:
            gradient_delta (float): Step size used for gradient perturbation.
        """
        super().__init__(gradient_delta=gradient_delta)
        self.init_settings = None

    def set_model(self):
        """
        Set the Elo model class to be optimized.
        """
        self.model = EloCalculation

    def initialize_model_settings(self):
        """
        Initialize Elo model parameters with random starting values.

        Returns:
            Dictionary with initial Elo parameter settings.
        """
        settings = {
            'elo_game_value': uniform(77, 78),
            'num_rounds_degree': uniform(0.33, 0.34),
            'num_drivers_degree': uniform(0.39, 0.4),
            'default_rating': uniform(1830, 1831),
            'new_agent_alpha': uniform(1.65, 1.7),
        }

        self.init_settings = settings
        print(settings)

        return settings


def split_dataset(df):
    """
    Split a dataset into initialization, training, and validation subsets based on dates.

    Arguments:
        df (pandas.DataFrame): PVP results dataset.

    Returns:
        dict: Dictionary with keys 'init', 'train', and 'validation' containing the respective subsets.
    """
    init_ix = df['game_date'] <= INIT_DATASET_END_DATE
    init_df = df.loc[init_ix]

    train_ix = (df['game_date'] > INIT_DATASET_END_DATE) & (df['game_date'] <= TRAIN_DATASET_END_DATE)
    train_df = df.loc[train_ix]

    validation_ix = (df['game_date'] > TRAIN_DATASET_END_DATE) & (df['game_date'] <= VALIDATION_DATASET_END_DATE)
    validation_df = df.loc[validation_ix]

    datasets = {'init': init_df, 'train': train_df, 'validation': validation_df}

    return datasets
