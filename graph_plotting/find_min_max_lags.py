import datetime
import os

import pandas as pd
import numpy as np
from dateutil.tz import gettz
from matplotlib import pyplot as plt

from boiler.constants import column_names
from boiler.time_delta.corr_time_delta_calculator import CorrTimeDeltaCalculator
from boiler.parsing_utils.utils import filter_by_timestamp_closed, average_values
import config


def main():
    start_datetime = datetime.datetime(2019, 1, 1, 0, 0, 0, tzinfo=gettz(config.DEFAULT_TIMEZONE))
    end_datetime = datetime.datetime(2019, 5, 1, 0, 0, 0, tzinfo=gettz(config.DEFAULT_TIMEZONE))
    calc_step = pd.Timedelta(hours=360)
    average_size = 100

    allowed_homes = [
        # "engelsa_35.csv.pickle",
        # "engelsa_37.csv.pickle",
        # "gaydara_1.csv.pickle",
        # "gaydara_22.csv.pickle",
        # "gaydara_26.csv.pickle",
        "gaydara_28.csv.pickle",
        "gaydara_30.csv.pickle",
        "gaydara_32.csv.pickle",
        # "kuibysheva_10.csv.pickle",
        # "kuibysheva_14.csv.pickle",
        # "kuibysheva_16.csv.pickle",
        # "kuibysheva_8.csv.pickle",
    ]

    start_datetime = pd.Timestamp(start_datetime)
    end_datetime = pd.Timestamp(end_datetime)

    calulations_count = (end_datetime-start_datetime) // calc_step

    lag_calculator = CorrTimeDeltaCalculator()

    boiler_df = pd.read_pickle(config.BOILER_PREPROCESSED_HEATING_CIRCUIT_DATASET_PATH)
    boiler_df: pd.DataFrame = filter_by_timestamp_closed(boiler_df, start_datetime, end_datetime)

    for home_dataset_name in os.listdir(config.HOMES_PREPROCESSED_HEATING_CIRCUIT_DATASETS_DIR):
        if home_dataset_name in allowed_homes:
            home_df: pd.DataFrame = pd.read_pickle(
                f"{config.HOMES_PREPROCESSED_HEATING_CIRCUIT_DATASETS_DIR}\\{home_dataset_name}"
            )
            home_df = filter_by_timestamp_closed(home_df, start_datetime, end_datetime)

            # noinspection PyUnresolvedReferences
            lag_array = np.empty(calulations_count, dtype=np.float)
            lag_dates = []
            for calc_number in range(calulations_count):
                sub_start_datetime = start_datetime + (calc_step * calc_number)
                sub_end_datetime = sub_start_datetime + calc_step

                mean_date = sub_start_datetime + (calc_step // 2)
                lag_dates.append(mean_date)

                boiler_sub_df = filter_by_timestamp_closed(boiler_df, sub_start_datetime, sub_end_datetime)
                boiler_out_temp = boiler_sub_df[column_names.FORWARD_PIPE_COOLANT_TEMP].to_numpy()
                boiler_out_temp = average_values(boiler_out_temp, average_size)

                home_sub_df = filter_by_timestamp_closed(home_df, sub_start_datetime, sub_end_datetime)
                home_in_temp = home_sub_df[column_names.FORWARD_PIPE_COOLANT_TEMP].to_numpy()
                average_values(home_in_temp, average_size)

                lag = lag_calculator.find_lag(boiler_out_temp, home_in_temp)
                lag_array[calc_number] = lag

            # noinspection PyUnresolvedReferences
            print(f"{home_dataset_name:25} "
                  f"min: {np.min(lag_array):5.3} "
                  f"max: {np.max(lag_array):5.3} "
                  f"mean: {np.mean(lag_array):5.3}")

            plt.plot(lag_dates, lag_array, "-.", label=home_dataset_name)

    plt.legend()
    plt.grid(True)
    plt.show()


if __name__ == '__main__':
    main()
