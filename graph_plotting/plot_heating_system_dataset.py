import datetime

import matplotlib.pyplot as plt
import pandas as pd
from dateutil.tz import gettz
from pandas.plotting import register_matplotlib_converters

from boiler.parsing_utils.utils import filter_by_timestamp_closed
from boiler.constants import column_names
import config

if __name__ == '__main__':
    start_datetime = datetime.datetime(2018, 12, 1, 0, 0, 0, tzinfo=gettz(config.DEFAULT_TIMEZONE))
    end_datetime = datetime.datetime(2019, 3, 1, 0, 0, 0, tzinfo=gettz(config.DEFAULT_TIMEZONE))
    dataset_path = config.BOILER_PREPROCESSED_HEATING_CIRCUIT_DATASET_PATH
    # dataset_path = f"{config.HOMES_PREPROCESSED_HEATING_CIRCUIT_DATASETS_DIR}\\engelsa_37.csv.pickle"

    register_matplotlib_converters()
    ax = plt.axes()

    columns_to_plot = (
        column_names.FORWARD_PIPE_COOLANT_TEMP,
        column_names.BACKWARD_PIPE_COOLANT_TEMP,
        # column_names.FORWARD_PIPE_COOLANT_VOLUME,
        # column_names.BACKWARD_PIPE_COOLANT_VOLUME,
        # column_names.FORWARD_PIPE_COOLANT_PRESSURE,
        # column_names.BACKWARD_PIPE_COOLANT_PRESSURE
    )

    loaded_dataset = pd.read_pickle(dataset_path)
    loaded_dataset = filter_by_timestamp_closed(loaded_dataset, start_datetime, end_datetime)

    for column_name in columns_to_plot:
        data_to_plot = loaded_dataset[column_name]
        # data_to_plot = average_values(data_to_plot, 100)
        ax.plot(loaded_dataset[column_names.TIMESTAMP], data_to_plot, label=column_name)

    ax.grid(True)
    ax.legend()
    plt.show()
