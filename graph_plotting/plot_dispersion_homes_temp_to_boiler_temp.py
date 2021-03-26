import datetime
import os

# import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from dateutil.tz import gettz

from boiler_parsing_utils.utils import filter_by_timestamp_closed
from boiler_constants import column_names
import config


def main():
    start_datetime = datetime.datetime(2019, 1, 1, 0, 0, 0, tzinfo=gettz(config.TIMEZONE))
    end_datetime = datetime.datetime(2019, 4, 1, 0, 0, 0, tzinfo=gettz(config.TIMEZONE))
    boiler_round_step = 0.1

    home_column = "HOME"
    boiler_column = "BOILER"
    boiler_rounded_temp_column = "BOILER_ROUNDED"
    mean_group_value_column = "MEAN_GROUP_VALUE"
    home_value_delta_column = "HOME_DELTA"
    home_value_abs_delta_column = "ABS_HOME_DELTA"

    with open(config.HOMES_TIME_DELTAS_PATH, "r") as f:
        time_deltas_df = pd.read_csv(f)

    boiler_df = pd.read_pickle(config.BOILER_PREPROCESSED_HEATING_CIRCUIT_DATASET_PATH)
    boiler_df = filter_by_timestamp_closed(boiler_df, start_datetime, end_datetime)
    boiler_forward_temp = boiler_df[column_names.FORWARD_PIPE_COOLANT_TEMP].to_numpy()

    for home_dataset_name in os.listdir(config.HOMES_PREPROCESSED_HEATING_CIRCUIT_DATASETS_DIR):
        home_dataset_path = f"{config.HOMES_PREPROCESSED_HEATING_CIRCUIT_DATASETS_DIR}\\{home_dataset_name}"
        home_df = pd.read_pickle(home_dataset_path)
        home_df = filter_by_timestamp_closed(home_df, start_datetime, end_datetime)
        home_forward_temp = home_df[column_names.FORWARD_PIPE_COOLANT_TEMP].to_numpy()

        home_name = home_dataset_name[:-len(config.PREPROCESSED_DATASET_FILENAME_EXT)]
        home_lag = time_deltas_df[time_deltas_df[column_names.HOME_NAME] == home_name][column_names.TIME_DELTA]
        home_lag = int(home_lag.iat[0])

        correlation_df = pd.DataFrame({
            boiler_column: boiler_forward_temp[:-home_lag],
            home_column: home_forward_temp[home_lag:]
        })

        correlation_df = correlation_df[correlation_df[boiler_column] >= correlation_df[home_column]]

        correlation_df[boiler_rounded_temp_column] = correlation_df[boiler_column].apply(
            lambda x: x//boiler_round_step*boiler_round_step
        )
        mean_series = correlation_df.groupby(boiler_rounded_temp_column)[home_column].mean()
        correlation_df[mean_group_value_column] = correlation_df[boiler_rounded_temp_column].apply(
            lambda x: mean_series[x]
        )

        home_temp = correlation_df[home_column].to_numpy()
        mean_group_temp = correlation_df[mean_group_value_column].to_numpy()
        delta = home_temp-mean_group_temp
        correlation_df[home_value_delta_column] = delta
        correlation_df[home_value_abs_delta_column] = np.abs(delta)
        std_var = np.std(delta)
        # print(f"{home_name}: {std_var}")
        correlation_df = correlation_df[correlation_df[home_value_abs_delta_column] <= 3 * std_var]

        print(f"{home_name:20}: "
              f"[{correlation_df[home_value_delta_column].quantile(0.025):5.3}, "
              f"{correlation_df[home_value_delta_column].quantile(0.975):5.3}]")

        # ax = correlation_df[home_value_delta_column].hist(bins=20)
        # ax.set_title(home_name)
        # plt.show()

        print(end="")


if __name__ == '__main__':
    main()
