from abc import ABC, abstractmethod

from numpy import (
    abs as np_abs,
    min as np_min,
    sign as np_sign,
)
from pandas import DataFrame


class AbstractGradientOptimizer(ABC):
    """Abstract base class for parameter optimization using gradient descent."""

    def __init__(self, gradient_delta=0.0001):
        """
        Construct the object.

        Arguments:
            gradient_delta (float): Step size used for gradient perturbation.
        """
        self.gradient_delta = gradient_delta
        self.model = None
        self.output = None
        self.init_parameters = None
        self.optimized_parameters = None
        self.set_model()

    @abstractmethod
    def set_model(self):
        """
        Set model class for which Gradient Descent Optimizer should be applied.
        """
        pass

    @abstractmethod
    def initialize_model_settings(self):
        """
        Initialize model parameters to be optimized.
        """
        pass

    def run(self, datasets, num_epochs=50):
        """
        Run full optimization pipeline using gradient descent.

        Arguments:
            datasets (dict): Dictionary with 'init', 'train', and 'validation' datasets.
            num_epochs (int): Number of optimization iterations.

        Returns:
            Gradient descent optimization output as a pandas.DataFrame.
        """
        model_settings = self.initialize_model_settings()
        outputs = []
        for iter_num in range(1, num_epochs+1):
            iter_dict = {}
            iter_dict['iteration'] = iter_num
            iter_dict.update(model_settings)
    
            iter_dict = self.run_baseline_simulation(settings=model_settings, iter_result=iter_dict, datasets=datasets, stage_suffix='pre')

            upd_settings = {}
            for parameter in model_settings.keys():
                iter_dict = self.run_gradient(settings=model_settings, iter_result=iter_dict, datasets=datasets, gradient_field=parameter)
                upd_settings[parameter] = iter_dict[f'new_{parameter}']

            iter_dict = self.run_baseline_simulation(settings=upd_settings, iter_result=iter_dict, datasets=datasets, stage_suffix='new')
    
            model_settings.update(upd_settings)
            outputs.append(iter_dict)
            print(f'Iteration: {iter_num}: {iter_dict["new_train_log_loss"]}')

        self.optimized_parameters = model_settings
        self.output = DataFrame(outputs)

        return self.output

    def run_baseline_simulation(self, settings, iter_result, datasets, stage_suffix='pre'):
        """
        Run baseline model evaluation before and after parameter updates.

        Arguments:
            settings (dict): Current model parameter values.
            iter_result (dict): Dictionary with iteration statistics.
            datasets (dict): Dictionary with 'init', 'train', and 'validation' datasets.
            stage_suffix (str): Label for current stage ('pre' or 'new').

        Returns:
            Updated iteration statistics as a dictionary.
        """
        current_settings = self.build_settings(settings, gradient_field=None)
        elo_calc_current = self.model(**current_settings)
        elo_calc_current.run_pipeline(pvp_results=datasets.get('init'))
        elo_calc_current.reset_log_loss()
    
        elo_calc_current.run_pipeline(pvp_results=datasets.get('train'))
        iter_result[f'{stage_suffix}_train_log_loss'] = elo_calc_current.log_loss
        elo_calc_current.reset_log_loss()
    
        elo_calc_current.run_pipeline(pvp_results=datasets.get('validation'))
        current_validation_log_loss = elo_calc_current.log_loss
        iter_result[f'{stage_suffix}_validation_log_loss'] = current_validation_log_loss

        return iter_result

    def run_gradient(self, settings, iter_result, datasets, gradient_field):
        """
        Perform gradient-based update for a single parameter.

        Arguments:
            settings (dict): Current model parameter values.
            iter_result (dict): Dictionary with iteration statistics.
            datasets (dict): Dictionary with 'init', 'train', and 'validation' datasets.
            gradient_field (str): Parameter name to update.

        Returns:
            Updated iteration statistics as a dictionary.
        """
        gradient_settings = self.build_settings(settings, gradient_field=gradient_field)  
        elo_calc = self.model(**gradient_settings)
        elo_calc.run_pipeline(pvp_results=datasets.get('init'))
        elo_calc.reset_log_loss()
    
        elo_calc.run_pipeline(pvp_results=datasets.get('train'))
        gradient_coeff = min(iter_result['iteration'], 5)
        gradient_value = -(elo_calc.log_loss - iter_result['pre_train_log_loss'])/self.gradient_delta
        iter_delta = (iter_result[gradient_field]**0.6) * gradient_coeff * gradient_value
        iter_delta = np_sign(iter_delta) * np_min([np_abs(iter_delta), np_abs(iter_result[gradient_field])*0.1])
        iter_result[f'{gradient_field}_delta'] = iter_delta
        iter_result[f'new_{gradient_field}'] = iter_result[gradient_field] + iter_delta

        return iter_result

    def build_settings(self, model_settings, gradient_field=None):
        """
        Construct parameter set for model initialization.

        Arguments:
            model_settings (dict): Current model parameter values.
            gradient_field (str): Parameter name to perturb for gradient estimation.

        Returns:
            parameter settings for simulation as a dictionary.
        """
        settings = {}
        for model_setting in model_settings:
            gradient_delta = self.gradient_delta if model_setting == gradient_field else 0
            settings[model_setting] = model_settings[model_setting] * (1 + gradient_delta)

        return settings
