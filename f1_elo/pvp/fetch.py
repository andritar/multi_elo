from pandas import (
    read_csv,
    to_datetime,
)

from f1_elo.constants import (
    CIRCUITS_DATASET_PATH,
    CONSTRUCTORS_DATASET_PATH,
    DRIVERS_DATASET_PATH,
    RACES_DATASET_PATH,
    RESULTS_DATASET_PATH,
)


def get_results_data():
    """
    Load raw results data and enrich it with driver, constructor and race info.

    Returns:
        DataFrame combining all relevant race result details.
    """
    raw_results = _fetch_raw_results(results_path=RESULTS_DATASET_PATH)
    drivers = _fetch_drivers(drivers_path=DRIVERS_DATASET_PATH)
    constructors = _fetch_constructors(constructors_path=CONSTRUCTORS_DATASET_PATH)
    races = _fetch_races(races_path=RACES_DATASET_PATH, circuits_path=CIRCUITS_DATASET_PATH)

    results = raw_results.merge(drivers, how='left', on='driverId')
    results = results.merge(constructors, how='left', on='constructorId')
    results = results.merge(races, how='left', on='raceId')

    results = results[
        [
            'resultId', 'year', 'round', 'date', 'circuit', 'circuit_country', 'raceId',
            'positionOrder', 'position', 'driver', 'constructor', 'points', 'grid'
        ]
    ]

    results = results.sort_values(['year', 'round', 'positionOrder'])

    return results


def _fetch_circuits(circuits_path):
    """
    Fetch circuit data from a CSV file.

    Arguments:
        circuits_path (str): Path to the CSV file containing circuit information.

    Returns:
        DataFrame with circuit IDs, names, and countries.
    """
    circuits = read_csv(circuits_path)
    circuits = circuits[['circuitId', 'name', 'country']]
    circuits = circuits.rename(columns={'name': 'circuit', 'country': 'circuit_country'})

    return circuits


def _fetch_constructors(constructors_path):
    """
    Fetch constructor data from a CSV file.

    Arguments:
        constructors_path (str): Path to the CSV file containing constructor information.

    Returns:
        DataFrame with constructor IDs and their names.
    """
    constructors = read_csv(constructors_path)
    constructors = constructors.rename(columns={'name': 'constructor'})
    constructors = constructors[['constructorId', 'constructor']]

    return constructors


def _fetch_drivers(drivers_path):
    """
    Fetch driver data from a CSV file.

    Arguments:
        drivers_path (str): Path to the CSV file containing driver information.

    Returns:
        DataFrame with driver IDs and their full names.
    """
    drivers = read_csv(drivers_path)
    drivers['driver'] = drivers['forename'] + ' ' + drivers['surname']
    drivers = drivers[['driverId', 'driver']]

    return drivers


def _fetch_races(races_path, circuits_path):
    """
    Fetch race data from a CSV file and merge it with circuit information.

    Arguments:
        races_path (str): Path to the CSV file containing race information.
        circuits_path (str): Path to the CSV file containing circuit information.

    Returns:
        DataFrame with race details including circuit names and countries.
    """
    races = read_csv(races_path)
    circuits = _fetch_circuits(circuits_path=circuits_path)

    races = races.merge(circuits, how='left', on='circuitId')
    races = races[['raceId', 'year', 'round', 'date', 'circuit', 'circuit_country']]
    races['date'] = to_datetime(races['date'], format='%Y-%m-%d').map(lambda x: x.date())

    return races


def _fetch_raw_results(results_path):
    """
    Fetch race result data from a CSV file.

    Arguments:
        results_path (str): Path to the CSV file containing race results.

    Returns:
        DataFrame with raw race result information.
    """
    results = read_csv(results_path)
    
    return results
