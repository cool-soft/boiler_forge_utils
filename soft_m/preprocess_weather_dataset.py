
import os
import logging

import config
from boiler.weater_info.interpolators.weather_data_linear_interpolator import WeatherDataLinearInterpolator
from boiler.weater_info.parsers.soft_m_json_weather_data_parser import SoftMJSONWeatherDataParser


def preprocess_weather_dataset():
    parser = SoftMJSONWeatherDataParser()
    parser.set_weather_data_timezone_name(config.WEATHER_DATA_TIMEZONE)
    with open(os.path.abspath(config.WEATHER_SRC_DATASET_PATH), "r") as f:
        weather_df = parser.parse_weather_data(f)

    interpolator = WeatherDataLinearInterpolator()
    interpolator.interpolate_weather_data(weather_df, config.SHARED_START_TIMESTAMP, config.SHARED_END_TIMESTAMP, inplace=True)

    weather_df.to_pickle(config.WEATHER_PREPROCESSED_DATASET_PATH)


if __name__ == '__main__':
    logging.basicConfig(level="DEBUG")
    preprocess_weather_dataset()
