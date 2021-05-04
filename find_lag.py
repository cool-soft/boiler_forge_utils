import datetime
import os

import pandas as pd
from boiler.time_delta.io.sync_timedelta_csv_writer import SyncTimedeltaCSVWriter
from boiler.time_delta.io.sync_timedelta_file_dumper import SyncTimedeltaFileDumper
from dateutil.tz import gettz
from boiler.constants import column_names, dataset_prototypes
from boiler.heating_obj.io.sync_heating_obj_file_loader import SyncHeatingObjFileLoader
from boiler.heating_obj.io.sync_heating_obj_pickle_reader import SyncHeatingObjPickleReader
from boiler.time_delta.calculators.algo.std_var_time_delta_calculation_algorithm \
    import StdVarTimeDeltaCalculationAlgorithm
from boiler_softm.constants import time_tick as soft_m_time_tick

import config


def main():
    start_datetime = datetime.datetime(2018, 12, 1, 0, 0, 0, tzinfo=gettz(config.DEFAULT_TIMEZONE))
    end_datetime = datetime.datetime(2019, 6, 1, 0, 0, 0, tzinfo=gettz(config.DEFAULT_TIMEZONE))
    time_tick = soft_m_time_tick.TIME_TICK

    allowed_houses = [
        "engelsa_35",
        "engelsa_37",
        "gaydara_1",
        "gaydara_22",
        "gaydara_26",
        "gaydara_28",
        "gaydara_30",
        "gaydara_32",
        "kuibysheva_10",
        "kuibysheva_14",
        "kuibysheva_16",
        "kuibysheva_8",
    ]

    pickle_reader = SyncHeatingObjPickleReader()

    boiler_df_loader = SyncHeatingObjFileLoader(
        reader=pickle_reader,
        filepath=config.BOILER_PREPROCESSED_HEATING_CIRCUIT_DATASET_PATH
    )
    boiler_df = boiler_df_loader.load_heating_obj(start_datetime, end_datetime)
    boiler_out_temp = boiler_df[column_names.FORWARD_PIPE_COOLANT_TEMP].to_numpy()

    lag_calculator = StdVarTimeDeltaCalculationAlgorithm()
    timedelta_df = dataset_prototypes.TIMEDELTA.copy()

    for dataset_filename in os.listdir(config.APARTMENT_HOUSE_PREPROCESSED_DATASETS_HEATING_CIRCUIT_DIR):
        dataset_name, filename_ext = os.path.splitext(dataset_filename)
        if dataset_name in allowed_houses:
            path = f"{config.APARTMENT_HOUSE_PREPROCESSED_DATASETS_HEATING_CIRCUIT_DIR}\\" \
                   f"{dataset_name}{filename_ext}"
            loader = SyncHeatingObjFileLoader(
                reader=pickle_reader,
                filepath=path
            )
            heating_obj_df = loader.load_heating_obj(start_datetime, end_datetime)
            heating_obj_in_temp = heating_obj_df[column_names.FORWARD_PIPE_COOLANT_TEMP].to_numpy()

            timedelta_in_tick = lag_calculator.find_lag(boiler_out_temp, heating_obj_in_temp)
            timedelta = pd.Timedelta(seconds=timedelta_in_tick*(time_tick.total_seconds()))
            timedelta_df = timedelta_df.append(
                {column_names.HEATING_OBJ_ID: dataset_name, column_names.AVG_TIMEDELTA: timedelta},
                ignore_index=True
            )

            print(f"{dataset_name:15}; {timedelta_in_tick} tick; {timedelta};")

    timedelta_writer = SyncTimedeltaCSVWriter()
    timedelta_dumper = SyncTimedeltaFileDumper(
        writer=timedelta_writer,
        filepath=config.HEATING_OBJ_TIME_DELTAS_PATH
    )
    timedelta_dumper.dump_timedelta(timedelta_df)


if __name__ == '__main__':
    main()
