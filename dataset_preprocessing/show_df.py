import pandas as pd

import config

if __name__ == '__main__':
    df_path = f"{config.HOMES_PREPROCESSED_HEATING_CIRCUIT_DATASETS_DIR}\\engelsa_37.csv.pickle"
    df = pd.read_pickle(df_path)
    print()
