"""
ETL Pipeline Entry Point

Loads configuration, sets up logging, ensures folder structure, 
and runs the ETL pipeline.

Usage:
    python main.py
"""


from src.extract import extract
from src.transform import transform
import logging
from src.utils.logger import setup_logging
from src.utils.etl_config import setup_extraction_config

setup_logging()

logger = logging.getLogger(__name__)


def main():
    logger.info('Setting up extraction configuration')
    config = setup_extraction_config()

    logger.info('Starting extraction of data from API')
    saved_files = extract(config.Extract)
    logger.info('Extraction of data from API finished')

    logger.info('Starting transformation of extracted data')
    air_pollution_df, current_weather_df, forecast_weather_df = transform(
        saved_files)
    logger.info('Transformation of extracted data finished')

    logger.info('Starting loading of transformed data into database')
    air_pollution_df.name = "air_pollution"
    current_weather_df.name = "current_weather"
    forecast_weather_df.name = "forecast_weather"
    load(air_pollution_df, config.Load)
    load(current_weather_df, config.Load)
    load(forecast_weather_df, config.Load)
    logger.info('Loading of transformed data into database finished')


if __name__ == '__main__':
    main()
