import logging
import multiprocessing as mp
import os

from boiler.constants import circuit_types
from boiler.constants import column_names
from boiler.data_processing.timestamp_interpolator_algorithm import TimestampInterpolationAlgorithm
from boiler.data_processing.timestamp_round_algorithm import CeilTimestampRoundAlgorithm
from boiler.data_processing.value_interpolation_algorithm import LinearOutsideValueInterpolationAlgorithm, \
    LinearInsideValueInterpolationAlgorithm
from boiler.heating_obj.io.abstract_sync_heating_obj_dumper import AbstractSyncHeatingObjDumper
from boiler.heating_obj.io.abstract_sync_heating_obj_loader import AbstractSyncHeatingObjLoader
from boiler.heating_obj.io.sync_heating_obj_file_dumper import SyncHeatingObjFileDumper
from boiler.heating_obj.io.sync_heating_obj_file_loader import SyncHeatingObjFileLoader
from boiler.heating_obj.io.sync_heating_obj_pickle_writer import SyncHeatingObjPickleWriter
from boiler.heating_obj.processing import AbstractHeatingObjProcessor
from boiler_softm.constants import processing_parameters as soft_m_parsing_parameters
from boiler_softm.constants import time_tick as soft_m_time_tick
from boiler_softm.heating_obj.io.soft_m_sync_heating_obj_csv_reader import SoftMSyncHeatingObjCSVReader
from boiler_softm.heating_obj.processing import SoftMHeatingObjProcessor
from dateutil.tz import gettz

import config


def create_loader_for_dataset(filepath):
    boiler_data_reader = SoftMSyncHeatingObjCSVReader(
        timestamp_parse_patterns=soft_m_parsing_parameters.HEATING_OBJ_TIMESTAMP_PARSING_PATTERNS,
        timestamp_timezone=gettz(config.APARTMENT_HOUSE_DATA_TIMEZONE),
        need_columns=soft_m_parsing_parameters.APARTMENT_HOUSE_AVAILABLE_COLUMNS,
        float_columns=soft_m_parsing_parameters.APARTMENT_HOUSE_FLOAT_COLUMNS,
        water_temp_columns=[column_names.FORWARD_PIPE_COOLANT_TEMP,
                            column_names.BACKWARD_PIPE_COOLANT_TEMP],
        need_circuit=circuit_types.HEATING
    )
    loader = SyncHeatingObjFileLoader(
        filepath=filepath,
        reader=boiler_data_reader
    )
    return loader


def create_processor():
    timestamp_round_algorithm = CeilTimestampRoundAlgorithm(round_step=soft_m_time_tick.TIME_TICK)
    timestamp_interpolation_algorithm = TimestampInterpolationAlgorithm(
        timestamp_round_algo=timestamp_round_algorithm,
        interpolation_step=soft_m_time_tick.TIME_TICK
    )
    data_processor = SoftMHeatingObjProcessor(
        columns_to_interpolate=soft_m_parsing_parameters.APARTMENT_HOUSE_NEED_INTERPOLATE_COLUMNS,
        timestamp_round_algorithm=timestamp_round_algorithm,
        timestamp_interpolation_algorithm=timestamp_interpolation_algorithm,
        border_values_interpolation_algorithm=LinearOutsideValueInterpolationAlgorithm(),
        internal_values_interpolation_algorithm=LinearInsideValueInterpolationAlgorithm(),
    )
    return data_processor


def create_saver_for_dataset(filepath):
    writer = SyncHeatingObjPickleWriter()
    dumper = SyncHeatingObjFileDumper(
        filepath=filepath,
        writer=writer
    )
    return dumper


def preprocess_dataset(loader: AbstractSyncHeatingObjLoader,
                       saver: AbstractSyncHeatingObjDumper,
                       processor: AbstractHeatingObjProcessor,
                       start_datetime,
                       end_datetime):
    logging.basicConfig(level="INFO")
    df = loader.load_heating_obj()
    df = processor.process_heating_obj(df, start_datetime, end_datetime)
    saver.dump_heating_obj(df)


def main():
    logging.basicConfig(level="DEBUG")

    processes = []
    logging.debug(f"Searching homes datasets in {config.APARTMENT_HOUSE_SRC_DATASETS_DIR}")
    for filename_with_ext in os.listdir(config.APARTMENT_HOUSE_SRC_DATASETS_DIR):
        input_filepath = os.path.join(config.APARTMENT_HOUSE_SRC_DATASETS_DIR, filename_with_ext)
        filename, ext = os.path.splitext(filename_with_ext)
        output_filepath = os.path.join(
            config.APARTMENT_HOUSE_PREPROCESSED_DATASETS_HEATING_CIRCUIT_DIR,
            f"{filename}.pickle"
        )

        loader = create_loader_for_dataset(input_filepath)
        saver = create_saver_for_dataset(output_filepath)
        processor = create_processor()

        # preprocess_dataset(
        #     loader,
        #     saver,
        #     processor,
        #     config.SHARED_START_TIMESTAMP,
        #     config.SHARED_END_TIMESTAMP
        # )

        process = mp.Process(
            target=preprocess_dataset,
            args=(
                loader,
                saver,
                processor,
                config.SHARED_START_TIMESTAMP,
                config.SHARED_END_TIMESTAMP
            )
        )
        process.start()
        processes.append(process)

    for process in processes:
        process.join()


if __name__ == '__main__':
    main()
