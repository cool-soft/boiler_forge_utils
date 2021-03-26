import logging

import config
from boiler_constants import circuits_id
from boiler_heating_system.interpolators.heating_system_data_linear_interpolator import \
    HeatingSystemDataLinearInterpolator
from boiler_heating_system.parsers.soft_m_csv_heating_system_data_parser import SoftMCSVHeatingSystemDataParser
from boiler_heating_system.repository.heating_system_csv_repository import HeatingSystemCSVRepository
from boiler_heating_system.repository.heating_system_pickle_repository import HeatingSystemPickleRepository
from boiler_forge_utils.dataset_preprocessing.preprocess_homes_dataset import preprocess_dataset


def main():
    logging.basicConfig(level="DEBUG")
    dataset_name = "gaydara_32"

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

    preprocess_dataset(
        input_repository,
        output_repository,
        dataset_name,
        config.START_DATETIME,
        config.END_DATETIME
    )


if __name__ == '__main__':
    main()
