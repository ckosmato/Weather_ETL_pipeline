"""
Extraction Configuration Module

Handles loading and validation of configuration for the weather data extraction pipeline.

Provides the ExtractionConfig dataclass, which contains:
- cities: List of city names to extract weather data for
- api_key: OpenWeatherMap API key
- raw_path: Directory path to store raw JSON data
- units: Units of measurement for API responses (metric, imperial, etc.)

Usage:
    from etl_config import setup_extraction_config

    config = setup_extraction_config()
    print(config.cities, config.api_key, config.raw_path, config.units)
"""
import logging
from dataclasses import dataclass
import os
from dotenv import load_dotenv


@dataclass
class ExtractionConfig:
    cities: list
    api_key: str
    raw_path: str
    units: str


logger = logging.getLogger(__name__)

# Setup extraction configuration


def setup_extraction_config():
    load_dotenv('config.env')

    logger.info('Getting API key from enviroment')
    api_key = os.environ.get('OWM_API_KEY')
    if not api_key:
        logger.error(
            "OpenWeatherMap API key not found. Set OWM_API_KEY in .env or pipeline.")
        raise ValueError(
            "OpenWeatherMap API key not found. Set OWM_API_KEY in .env or pipeline.")

    logger.info('Getting data storage path for raw data from enviroment')
    raw_path = os.environ.get('RAW_DIR', 'data/raw')
    os.makedirs(raw_path, exist_ok=True)

    logger.info('Getting type of measurement API responses from enviroment')
    units = os.environ.get('UNITS', 'metric')
    if not units:
        logger.error('Units measure not found. Set UNITS in .env or pipeline.')
        raise ValueError(
            'Units measure not found. Set UNITS in .env or pipeline.')

    logger.info('Getting cities to extract from environment')
    cities_str = os.environ.get('CITIES', '')
    cities = [c.strip() for c in cities_str.split(",") if c.strip()]
    if not cities:
        logger.error('No cities found in environment variables')
        raise ValueError('No cities found in environment variables')

    config = ExtractionConfig(
        api_key=api_key,
        raw_path=raw_path,
        units=units,
        cities=cities
    )

    return config
