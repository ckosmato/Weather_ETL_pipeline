"""
Weather Data Extraction Module

Provides functions to fetch and save weather, forecast, and air pollution data 
from the OpenWeatherMap API. Cities are resolved to coordinates, data is 
downloaded, and results are stored as JSON files in ./data/raw/.

Usage:
    from extract import extract
    extract(cities, api_key)
"""

import requests
import json
import time
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

# Main extraction function


def extract(config):
    saved_files = {}
    for city in config.cities:

        logger.info(f'Fetching coordinates for {city}')
        lat, lon = fetch_coordinates(city, config.api_key)

        if lat is None or lon is None:
            logger.info(f'{lat} or {lon} is empty, skipping')
            continue

        logger.info(f'Fetching weather data for {city}')
        saved_files[city] = fetch_weather(city, lat, lon, config)

    return saved_files

# Fetch coordinates for a city from OpenWeatherMap API


def fetch_coordinates(city, api_key):
    geo_url = f'http://api.openweathermap.org/geo/1.0/direct?q={city}&limit=5&appid={api_key}'

    try:
        response = requests.get(geo_url, timeout=10)
        response.raise_for_status()
        geo_data = response.json()
    except requests.RequestException as e:
        logger.exception(f"Error fetching coordinates for {city}: {e}")
        return None, None

    if not geo_data:
        logger.warning(f"No coordinates found for {city}")
        return None, None

    lat, lon = geo_data[0]['lat'], geo_data[0]['lon']
    logger.info(f"Coordinates for {city}: lat={lat}, lon={lon}")
    return lat, lon

# Fetch weather data for a city from OpenWeatherMap API


def fetch_weather(city, lat, lon, config):

    urls = {
        'current_weather': f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&units={config.units}&appid={config.api_key}',
        'forecast_weather': f'https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&units={config.units}&appid={config.api_key}',
        'air_pollution': f'https://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&units={config.units}&appid={config.api_key}'
    }

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    city_safe = city.replace(" ", "_")

    saved_files = []
    for data_type, api_url in urls.items():
        file_path = Path(config.raw_path) / \
            f'raw_{data_type}_{city_safe}_{timestamp}.json'

        try:
            res = requests.get(api_url, timeout=10)
            res.raise_for_status()
            weather_data = res.json()
            logger.info(f'Fetched weather data for {city}')
            save_file(file_path, weather_data)
            saved_files.append(file_path)
            logger.info(f"Saved {data_type} data for {city} at {file_path}")
        except requests.RequestException as e:
            logger.exception(
                f"Error fetching {data_type} data for {city}: {e}")

        logger.debug("Sleeping 1 second to avoid hitting API rate limit")
        time.sleep(1)
    return saved_files

# Save data to a JSON file


def save_file(file_path, data):
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
