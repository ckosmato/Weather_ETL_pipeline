"""
ETL Pipeline Entry Point

Loads configuration, sets up logging, ensures folder structure, 
and runs the ETL pipeline.

Usage:
    python main.py
"""

import os
from dotenv import load_dotenv
from extract import extract
import logging
from config import setup_logging

setup_logging()

logger = logging.getLogger(__name__)


def main():

    load_dotenv('./config.env')

    logger.info('Getting API key from enviroment')
    api_key = os.environ.get('OWM_API_KEY')
    if not api_key:
        logger.error(
            "OpenWeatherMap API key not found. Set OWM_API_KEY in .env or pipeline.")
        raise ValueError(
            "OpenWeatherMap API key not found. Set OWM_API_KEY in .env or pipeline.")

    # Ensure data folder integrity
    os.makedirs('data/raw', exist_ok=True)

    os.makedirs('../logs', exist_ok=True)

    # Define the cities for extraction
    logger.info('Getting cities to extract from environment')
    cities = {}
    cities_str = os.environ.get('CITIES', '')
    for i, city in enumerate(cities_str.split(","), start=1):
        city = city.strip()
        if city:
            cities[f'city_{i}'] = city

    if not any(cities.values()):
        logger.error('No cities found in environment variables')
        return

    # Extract the data from API
    logger.info('Starting extraction of data from API')
    extract(cities, api_key)
    logger.info('Extraction of data from API finished')


if __name__ == '__main__':
    main()
