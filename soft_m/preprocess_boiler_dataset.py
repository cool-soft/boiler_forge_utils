import logging

from boiler.constants import circuit_ids
from boiler.constants import column_names
from boiler.data_processing.timestamp_interpolator_algorithm import TimestampInterpolationAlgorithm
from boiler.data_processing.timestamp_round_algorithm import CeilTimestampRoundAlgorithm
from boiler.data_processing.value_interpolation_algorithm import LinearOutsideValueInterpolationAlgorithm, \
    LinearInsideValueInterpolationAlgorithm
from boiler.heating_obj.io.sync_heating_obj_file_dumper import SyncHeatingObjFileDumper
from boiler.heating_obj.io.sync_heating_obj_file_loader import SyncHeatingObjFileLoader
from boiler.heating_obj.io.sync_heating_obj_pickle_writer import SyncHeatingObjPickleWriter
from boiler_softm.constants import processing_parameters as soft_m_parsing_parameters
from boiler_softm.constants import time_tick as soft_m_time_tick
from boiler_softm.heating_obj.io.soft_m_sync_heating_obj_csv_reader import SoftMSyncHeatingObjCSVReader
from boiler_softm.heating_obj.processing import SoftMHeatingObjProcessor
from dateutil.tz import gettz

import config


def main():
    logging.basicConfig(level="DEBUG")

    boiler_df = load_boiler_dataset()
    boiler_df = process_boiler_dataset(boiler_df)
    save_boiler_dataset(boiler_df)


def load_boiler_dataset():
    boiler_data_reader = SoftMSyncHeatingObjCSVReader(
        timestamp_parse_patterns=soft_m_parsing_parameters.HEATING_OBJ_TIMESTAMP_PARSING_PATTERNS,
        timestamp_timezone=gettz(config.BOILER_DATA_TIMEZONE),
        need_columns=soft_m_parsing_parameters.BOILER_AVAILABLE_COLUMNS,
        float_columns=soft_m_parsing_parameters.BOILER_FLOAT_COLUMNS,
        water_temp_columns=[column_names.FORWARD_PIPE_COOLANT_TEMP,
                            column_names.BACKWARD_PIPE_COOLANT_TEMP],
        need_circuit=circuit_ids.HEATING_CIRCUIT
    )
    loader = SyncHeatingObjFileLoader(
        filepath=config.BOILER_SRC_DATASET_PATH,
        reader=boiler_data_reader
    )
    boiler_df = loader.load_heating_obj()
    return boiler_df


def process_boiler_dataset(boiler_df):
    timestamp_round_algorithm = CeilTimestampRoundAlgorithm(round_step=soft_m_time_tick.TIME_TICK)
    timestamp_interpolation_algorithm = TimestampInterpolationAlgorithm(
        timestamp_round_algo=timestamp_round_algorithm,
        interpolation_step=soft_m_time_tick.TIME_TICK
    )
    boiler_data_processor = SoftMHeatingObjProcessor(
        columns_to_interpolate=soft_m_parsing_parameters.BOILER_NEED_INTERPOLATE_COLUMNS,
        timestamp_round_algorithm=timestamp_round_algorithm,
        timestamp_interpolation_algorithm=timestamp_interpolation_algorithm,
        border_values_interpolation_algorithm=LinearOutsideValueInterpolationAlgorithm(),
        internal_values_interpolation_algorithm=LinearInsideValueInterpolationAlgorithm(),
    )
    boiler_df = boiler_data_processor.process_heating_obj(
        boiler_df,
        config.SHARED_START_TIMESTAMP,
        config.SHARED_END_TIMESTAMP
    )
    return boiler_df


def save_boiler_dataset(boiler_df):
    writer = SyncHeatingObjPickleWriter()
    dumper = SyncHeatingObjFileDumper(
        filepath=config.BOILER_PREPROCESSED_HEATING_CIRCUIT_DATASET_PATH,
        writer=writer
    )
    dumper.dump_heating_obj(boiler_df)


if __name__ == '__main__':
    main()
