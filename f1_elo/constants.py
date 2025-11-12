from datetime import date

ROOT_DATA_FOLDER = '/Users/taras/phd/f1_data'
CIRCUITS_DATASET_PATH = f'{ROOT_DATA_FOLDER}/input/circuits.csv'
CONSTRUCTORS_DATASET_PATH = f'{ROOT_DATA_FOLDER}/input/constructors.csv'
DRIVERS_DATASET_PATH = f'{ROOT_DATA_FOLDER}/input/drivers.csv'
PVP_DATASET_PATH = f'{ROOT_DATA_FOLDER}/processed/listing.csv'
RACES_DATASET_PATH = f'{ROOT_DATA_FOLDER}/input/races.csv'
RESULTS_DATASET_PATH = f'{ROOT_DATA_FOLDER}/input/results.csv'
SEASON_DRIVERS_PATH = f'{ROOT_DATA_FOLDER}/processed/season_drivers.csv'

RESULT_RATIOS = {'win1': 1, 'draw': 0.5, 'win2': 0}

INIT_DATASET_END_DATE = date(1954, 12, 31)
TRAIN_DATASET_END_DATE = date(1999, 12, 31)
VALIDATION_DATASET_END_DATE = date(2024, 12, 31)
