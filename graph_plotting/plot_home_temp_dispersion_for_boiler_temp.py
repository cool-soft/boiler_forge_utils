import datetime

import matplotlib.pyplot as plt
import pandas as pd
from dateutil.tz import gettz

import config
from boiler.constants import column_names
from boiler.parsing_utils.utils import filter_by_timestamp_closed


def main():
    start_datetime = datetime.datetime(2019, 1, 1, 0, 0, 0, tzinfo=gettz(config.TIMEZONE))
    end_datetime = datetime.datetime(2019, 4, 1, 0, 0, 0, tzinfo=gettz(config.TIMEZONE))
    boiler_round_step = 0.1

    left_quantile = 0.025
    right_quantile = 0.975

    bins_count = 20

    datasets_and_lags = {
        "engelsa_35.pickle": [6],
        "engelsa_37.pickle": [6],
        "gaydara_1.pickle": [3],
        "gaydara_22.pickle": [2],
        "gaydara_26.pickle": [4],
        "gaydara_28.pickle": [5],
        "gaydara_30.pickle": [5],
        "gaydara_32.pickle": [5],
        "kuibysheva_10.pickle": [3],
        "kuibysheva_14.pickle": [3],
        "kuibysheva_16.pickle": [3],
        "kuibysheva_8.pickle": [3]
    }

    home_column = "HOME"
    boiler_column = "BOILER"
    boiler_rounded_column = "BOILER_ROUNDED"
    mean_group_value_column = "MEAN_GROUP_TEMP"
    home_delta_column = "HOME_DELTA"
    home_abs_delta_column = "HOME_ABS_DELTA"

    boiler_df = pd.read_pickle(config.BOILER_PREPROCESSED_HEATING_CIRCUIT_DATASET_PATH)
    boiler_df = filter_by_timestamp_closed(boiler_df, start_datetime, end_datetime)
    boiler_forward_temp = boiler_df[column_names.FORWARD_PIPE_COOLANT_TEMP].to_numpy()

    for dataset_name, lags in datasets_and_lags.items():
        dataset_path = f"{config.HOMES_PREPROCESSED_HEATING_CIRCUIT_DATASETS_DIR}\\{dataset_name}"
        home_df = pd.read_pickle(dataset_path)
        home_df = filter_by_timestamp_closed(home_df, start_datetime, end_datetime)
        home_forward_temp = home_df[column_names.FORWARD_PIPE_COOLANT_TEMP].to_numpy()

        for lag in lags:
            correlation_df = pd.DataFrame({
                boiler_column: boiler_forward_temp[:-lag],
                home_column: home_forward_temp[lag:]
            })

            correlation_df[boiler_rounded_column] = correlation_df[boiler_column].apply(
                lambda x: x // boiler_round_step * boiler_round_step
            )
            mean_series = correlation_df.groupby(boiler_rounded_column)[home_column].mean()
            correlation_df[mean_group_value_column] = correlation_df[boiler_rounded_column].apply(
                lambda x: mean_series[x]
            )

            home_temp = correlation_df[home_column]
            mean_group_temp = correlation_df[mean_group_value_column]
            correlation_df[home_delta_column] = home_temp - mean_group_temp

            std_var = correlation_df[home_delta_column].std()

            home_forward_temp_delta = correlation_df[home_delta_column]
            print(f"{dataset_name:20} "
                  f"lag: {lag}; "
                  f"std: {std_var:5.3}; "
                  f"{(right_quantile-left_quantile) * 100 :6.3}%: "
                  f"[{home_forward_temp_delta.quantile(left_quantile):6.3}, "
                  f"{home_forward_temp_delta.quantile(right_quantile):6.3}];")

            correlation_df[home_abs_delta_column] = \
                correlation_df[home_delta_column].abs()
            # correlation_df = correlation_df[correlation_df[home_abs_delta_column] <= 3 * std_var]
            # ax = correlation_df[home_delta_column].hist(bins=bins_count)
            # ax.set_title(f"{dataset_name} LAG: {lag}")
            # plt.show()

            print(end="")


if __name__ == '__main__':
    main()
