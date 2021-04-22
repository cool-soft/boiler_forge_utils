import datetime
import os

import numpy as np
import pandas as pd
from dateutil.tz import gettz
from matplotlib import pyplot as plt

import config
from boiler.constants import column_names
from boiler.parsing_utils.utils import filter_by_timestamp_closed
from boiler.time_delta.std_var_time_delta_calculator import StdVarTimeDeltaCalculator


def main():
    start_datetime = datetime.datetime(2019, 1, 15, 0, 0, 0, tzinfo=gettz(config.TIMEZONE))
    end_datetime = datetime.datetime(2019, 4, 1, 0, 0, 0, tzinfo=gettz(config.TIMEZONE))
    calc_step = pd.Timedelta(hours=180)
    boiler_temp_round_step = 0.1
    min_lag = 1
    max_lag = 20

    allowed_homes = [
        "engelsa_35.pickle",
        "engelsa_37.pickle",
        "gaydara_1.pickle",
        "gaydara_22.pickle",
        "gaydara_26.pickle",
        "gaydara_28.pickle",
        "gaydara_30.pickle",
        "gaydara_32.pickle",
        "kuibysheva_10.pickle",
        "kuibysheva_14.pickle",
        "kuibysheva_16.pickle",
        "kuibysheva_8.pickle",
    ]

    start_datetime = pd.Timestamp(start_datetime)
    end_datetime = pd.Timestamp(end_datetime)

    calulations_count = (end_datetime - start_datetime) // calc_step

    lag_caluclator = StdVarTimeDeltaCalculator()
    lag_caluclator.set_x_round_step(boiler_temp_round_step)
    lag_caluclator.set_min_lag(min_lag)
    lag_caluclator.set_max_lag(max_lag)

    boiler_df = pd.read_pickle(config.BOILER_PREPROCESSED_HEATING_CIRCUIT_DATASET_PATH)
    boiler_df: pd.DataFrame = filter_by_timestamp_closed(boiler_df, start_datetime, end_datetime)

    for home_dataset_name in os.listdir(config.HOMES_PREPROCESSED_HEATING_CIRCUIT_DATASETS_DIR):
        if home_dataset_name in allowed_homes:
            home_df: pd.DataFrame = pd.read_pickle(
                f"{config.HOMES_PREPROCESSED_HEATING_CIRCUIT_DATASETS_DIR}\\{home_dataset_name}"
            )
            home_df = filter_by_timestamp_closed(home_df, start_datetime, end_datetime)

            lags = np.empty(calulations_count, dtype=np.float32)
            lag_dates = []
            for calc_number in range(calulations_count):
                sub_start_datetime = start_datetime + (calc_step * calc_number)
                sub_end_datetime = sub_start_datetime + calc_step

                mean_date = sub_start_datetime + (calc_step // 2)
                lag_dates.append(mean_date)

                boiler_sub_df = filter_by_timestamp_closed(boiler_df, sub_start_datetime, sub_end_datetime)
                boiler_forward_temp = boiler_sub_df[column_names.FORWARD_PIPE_COOLANT_TEMP].to_numpy()

                home_sub_df = filter_by_timestamp_closed(home_df, sub_start_datetime, sub_end_datetime)
                home_forward_temp = home_sub_df[column_names.FORWARD_PIPE_COOLANT_TEMP].to_numpy()

                lag = lag_caluclator.find_lag(boiler_forward_temp, home_forward_temp)

                lags[calc_number] = lag

            # noinspection PyUnresolvedReferences
            print(f"{home_dataset_name:25} "
                  f"min: {np.min(lags):4.2} "
                  f"max: {np.max(lags):4.2} "
                  f"mean: {np.mean(lags):4.2}")

    #         plt.plot(lag_dates, lags, label=home_dataset_name)
    #
    # plt.legend()
    # plt.grid(True)
    # plt.show()


if __name__ == '__main__':
    main()
