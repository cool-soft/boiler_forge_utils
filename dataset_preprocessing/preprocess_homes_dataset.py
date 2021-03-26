import logging
import multiprocessing as mp

from boiler.constants import circuits_id
from boiler.heating_system.interpolators.heating_system_data_linear_interpolator \
    import HeatingSystemDataLinearInterpolator
from boiler.heating_system.parsers.soft_m_csv_heating_system_data_parser import SoftMCSVHeatingSystemDataParser
from boiler.heating_system.repository.heating_system_csv_repository import HeatingSystemCSVRepository
from boiler.heating_system.repository.heating_system_pickle_repository import HeatingSystemPickleRepository
from boiler.heating_system.repository.heating_system_repository import HeatingSystemRepository
import config


def preprocess_dataset(input_repository: HeatingSystemRepository,
                       output_repository: HeatingSystemRepository,
                       dataset_id,
                       start_datetime,
                       end_datetime):
    logging.basicConfig(level="INFO")
    logging.info(f"Processing {dataset_id}")

    df = input_repository.get_dataset(dataset_id, start_datetime, end_datetime)
    output_repository.set_dataset(dataset_id, df)


def main():
    logging.basicConfig(level="DEBUG")

    home_data_parser = SoftMCSVHeatingSystemDataParser()
    home_data_parser.set_timestamp_parse_patterns(config.HOME_TIMESTAMP_PATTERNS)
    home_data_parser.set_timestamp_timezone_name(config.HOME_DATA_TIMEZONE)
    home_data_parser.set_need_circuits(config.HOME_REQUIRED_CIRCUITS)
    home_data_parser.set_need_columns(config.HOME_REQUIRED_COLUMNS)
    home_data_parser.set_need_to_float_convert_columns(config.HOME_NEED_TO_FLOAT_CONVERT_COLUMNS)

    home_data_interpolator = HeatingSystemDataLinearInterpolator()
    home_data_interpolator.set_interpolation_step(config.TIME_TICK)
    home_data_interpolator.set_columns_to_interpolate(config.HOME_COLUMNS_TO_INTERPOLATE)

    input_repository = HeatingSystemCSVRepository()
    input_repository.set_parser(home_data_parser)
    input_repository.set_interpolator(home_data_interpolator)
    input_repository.set_storage_path(config.HOMES_SRC_DATASETS_DIR)
    input_repository.set_circuit_id(circuits_id.HEATING_CIRCUIT)

    output_repository = HeatingSystemPickleRepository()
    output_repository.set_storage_path(config.HOMES_PREPROCESSED_HEATING_CIRCUIT_DATASETS_DIR)

    logging.debug(f"Searching homes datasets in {config.HOMES_SRC_DATASETS_DIR}")

    processes = []
    for dataset_id in input_repository.list():
        process = mp.Process(
            target=preprocess_dataset,
            args=(
                input_repository,
                output_repository,
                dataset_id,
                config.START_DATETIME,
                config.END_DATETIME
            )
        )
        process.start()
        processes.append(process)

    for process in processes:
        process.join()


if __name__ == '__main__':
    main()
