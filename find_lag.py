import datetime
import os

import pandas as pd
from dateutil.tz import gettz

from boiler_constants import column_names
from boiler_time_delta.corr_time_delta_calculator import CorrTimeDeltaCalculator
from boiler_parsing_utils.utils import filter_by_timestamp_closed
import config


def main():
    start_datetime = datetime.datetime(2019, 1, 5, 0, 0, 0, tzinfo=gettz(config.TIMEZONE))
    end_datetime = datetime.datetime(2019, 1, 15, 0, 0, 0, tzinfo=gettz(config.TIMEZONE))

    allowed_homes = [
        "engelsa_35.csv.pickle",
        "engelsa_37.csv.pickle",
        "gaydara_1.csv.pickle",
        "gaydara_22.csv.pickle",
        "gaydara_26.csv.pickle",
        "gaydara_28.csv.pickle",
        "gaydara_30.csv.pickle",
        "gaydara_32.csv.pickle",
        "kuibysheva_10.csv.pickle",
        "kuibysheva_14.csv.pickle",
        "kuibysheva_16.csv.pickle",
        "kuibysheva_8.csv.pickle",
    ]

    lag_calculator = CorrTimeDeltaCalculator()

    boiler_df = pd.read_pickle(config.BOILER_PREPROCESSED_HEATING_CIRCUIT_DATASET_PATH)
    boiler_df: pd.DataFrame = filter_by_timestamp_closed(boiler_df, start_datetime, end_datetime)
    boiler_out_temp = boiler_df[column_names.FORWARD_PIPE_COOLANT_TEMP].to_numpy()
    # boiler_out_temp = average_values(boiler_out_temp, average_size)

    for home_dataset_name in os.listdir(config.HOMES_PREPROCESSED_HEATING_CIRCUIT_DATASETS_DIR):
        if home_dataset_name in allowed_homes:
            home_df: pd.DataFrame = pd.read_pickle(
                f"{config.HOMES_PREPROCESSED_HEATING_CIRCUIT_DATASETS_DIR}\\{home_dataset_name}"
            )
            home_df = filter_by_timestamp_closed(home_df, start_datetime, end_datetime)
            home_in_temp = home_df[column_names.FORWARD_PIPE_COOLANT_TEMP].to_numpy()
            # average_values(home_in_temp, average_size)

            lag = lag_calculator.find_lag(boiler_out_temp, home_in_temp)
            print(f"{home_dataset_name:20}: {lag}")


if __name__ == '__main__':
    main()
