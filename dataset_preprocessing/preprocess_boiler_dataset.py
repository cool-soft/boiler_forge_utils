import logging

import config
from boiler_heating_system.interpolators.heating_system_data_linear_interpolator \
    import HeatingSystemDataLinearInterpolator
from boiler_heating_system.parsers.soft_m_csv_heating_system_data_parser import SoftMCSVHeatingSystemDataParser
from boiler_parsing_utils.utils import filter_by_timestamp_closed
from boiler_constants import column_names, circuits_id


def main():
    logging.basicConfig(level="DEBUG")

    boiler_data_parser = SoftMCSVHeatingSystemDataParser()
    boiler_data_parser.set_timestamp_parse_patterns(config.BOILER_TIMESTAMP_PATTERNS)
    boiler_data_parser.set_timestamp_timezone_name(config.BOILER_DATA_TIMEZONE)
    boiler_data_parser.set_need_circuits(config.BOILER_REQUIRED_CIRCUITS)
    boiler_data_parser.set_need_columns(config.BOILER_REQUIRED_COLUMNS)
    boiler_data_parser.set_need_to_float_convert_columns(config.BOILER_NEED_TO_FLOAT_CONVERT_COLUMNS)

    boiler_data_interpolator = HeatingSystemDataLinearInterpolator()
    boiler_data_interpolator.set_interpolation_step(config.TIME_TICK)
    boiler_data_interpolator.set_columns_to_interpolate(config.BOILER_COLUMNS_TO_INTERPOLATE)

    with open(config.BOILER_SRC_DATASET_PATH, encoding="UTF-8") as f:
        boiler_df = boiler_data_parser.parse(f)

    boiler_heating_circuit_df = boiler_df[boiler_df[column_names.CIRCUIT_ID] == circuits_id.HEATING_CIRCUIT].copy()
    del boiler_heating_circuit_df[column_names.CIRCUIT_ID]
    boiler_heating_circuit_df = boiler_data_interpolator.interpolate_data(
        boiler_heating_circuit_df,
        start_datetime=config.START_DATETIME,
        end_datetime=config.END_DATETIME,
        inplace=True
    )
    boiler_heating_circuit_df = filter_by_timestamp_closed(
        boiler_heating_circuit_df,
        config.START_DATETIME,
        config.END_DATETIME
    )
    logging.debug("Saving heating circuit df to {}".format(config.BOILER_PREPROCESSED_HEATING_CIRCUIT_DATASET_PATH))
    boiler_heating_circuit_df.to_pickle(config.BOILER_PREPROCESSED_HEATING_CIRCUIT_DATASET_PATH)


if __name__ == '__main__':
    main()
