import datetime
import os

import matplotlib.pyplot as plt
import pandas as pd
from dateutil.tz import gettz
from pandas.plotting import register_matplotlib_converters

from boiler.parsing_utils.utils import average_values, filter_by_timestamp_closed
from boiler.constants import column_names
import config


def main():
    start_datetime = datetime.datetime(2018, 12, 1, 23, 0, 0, tzinfo=gettz(config.DEFAULT_TIMEZONE))
    end_datetime = datetime.datetime(2019, 3, 1, 0, 0, 0, tzinfo=gettz(config.DEFAULT_TIMEZONE))
    homes_in_temp_smooth_size = 100
    boiler_temp_smooth_size = 100
    # noinspection SpellCheckingInspection
    allowed_homes = [
        # "engelsa_35.csv.pickle",
        # "engelsa_37.csv.pickle",
        # "gaydara_1.csv.pickle",
        # "gaydara_22.csv.pickle",
        # "gaydara_26.csv.pickle",
        # "gaydara_28.csv.pickle",
        # "gaydara_30.csv.pickle",
        # "gaydara_32.csv.pickle",
        # "kuibysheva_10.csv.pickle",
        "kuibysheva_14.csv.pickle",
        "kuibysheva_16.csv.pickle",
        "kuibysheva_8.csv.pickle",
    ]

    register_matplotlib_converters()
    ax = plt.axes()
    boiler_df = pd.read_pickle(config.BOILER_PREPROCESSED_HEATING_CIRCUIT_DATASET_PATH)
    boiler_df = filter_by_timestamp_closed(boiler_df, start_datetime, end_datetime)
    boiler_temp = boiler_df[column_names.FORWARD_PIPE_COOLANT_TEMP]
    boiler_temp = average_values(boiler_temp, boiler_temp_smooth_size)
    ax.plot(boiler_df[column_names.TIMESTAMP], boiler_temp, label="real boiler temp")

    for home_dataset_name in os.listdir(config.HOMES_PREPROCESSED_HEATING_CIRCUIT_DATASETS_DIR):
        if home_dataset_name in allowed_homes:
            home_df = pd.read_pickle(
                f"{config.HOMES_PREPROCESSED_HEATING_CIRCUIT_DATASETS_DIR}/{home_dataset_name}"
            )
            home_df = filter_by_timestamp_closed(home_df, start_datetime, end_datetime)
            home_in_temp = home_df[column_names.FORWARD_PIPE_COOLANT_TEMP]
            home_in_temp = average_values(home_in_temp, homes_in_temp_smooth_size)
            ax.plot(home_df[column_names.TIMESTAMP], home_in_temp, label=home_dataset_name)

    ax.grid(True)
    ax.legend()
    plt.show()


if __name__ == '__main__':
    main()
