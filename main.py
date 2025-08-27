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
    extract_config = setup_extraction_config()

    logger.info('Starting extraction of data from API')
    saved_files = extract(extract_config)
    logger.info('Extraction of data from API finished')

    logger.info('Starting transformation of extracted data')
    air_pollution_df, current_weather_df, forecast_weather_df = transform(
        saved_files)
    logger.info('Transformation of extracted data finished')
    print(air_pollution_df, current_weather_df, forecast_weather_df)


if __name__ == '__main__':
    main()
