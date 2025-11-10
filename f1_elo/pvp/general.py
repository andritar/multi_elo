from f1_elo.constants import PVP_DATASET_PATH
from f1_elo.pvp.calc import (
    calc_pvp_results,
    filter_results,
)
from f1_elo.pvp.fetch import get_results_data


def build_pvp_results():
    """
    Generate and save the pairwise driver comparison dataset.
    """
    results = get_results_data()
    
    finished_results = filter_results(results=results)
    
    pvp_results = calc_pvp_results(results=finished_results)
    pvp_results.to_csv(PVP_DATASET_PATH, index=False, sep=';')
