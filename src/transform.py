"""
Transform Module

Functions to convert raw JSON data into structured pandas DataFrames.

Usage:
    from src import transform
"""
import pandas as pd
import json
import logging

logger = logging.getLogger(__name__)

aqi_map = {1: "Good", 2: "Fair", 3: "Moderate", 4: "Poor", 5: "Very Poor"}


def transform(raw_file):
    all_air_pollution = []
    all_current_weather = []
    all_forecast_weather = []
    for city, files in raw_file.items():
        for file in files:
            if 'air_pollution' in str(file):
                logger.info(
                    f"Processing air pollution data for {city} from {file}")
                data = json_open(file)
                if data is None:
                    continue
                try:
                    air_pollution_df = pd.json_normalize(
                        data, record_path="list", meta=["coord"], sep="_")
                except KeyError as e:
                    logger.error(
                        f"Missing expected field in air pollution JSON for {city}: {e}")
                    continue

                air_pollution_df['main_aqi_desc'] = air_pollution_df['main_aqi'].map(
                    aqi_map)

                coord_df = pd.json_normalize(air_pollution_df['coord'])
                coord_df.columns = ['coord_' + c for c in coord_df.columns]
                air_pollution_df = pd.concat(
                    [air_pollution_df.drop(columns='coord'), coord_df], axis=1)

                air_pollution_df['city'] = city
                air_pollution_df['dt'] = pd.to_datetime(
                    air_pollution_df['dt'], unit='s')
                drop_dupes_and_fill(air_pollution_df, ['city', 'dt'], {
                    'main_aqi': -1,
                    'dt': pd.Timestamp("1970-01-01")
                })
                mask = air_pollution_df['main_aqi'].between(1, 5)
                air_pollution_df.loc[mask == False,
                                     'main_aqi_desc'] = 'Unknown'
                logger.info(
                    f"Finished processing air pollution data for {city} from {file}")
                all_air_pollution.append(air_pollution_df)

            elif 'current_weather' in str(file):
                logger.info(
                    f"Processing current weather data for {city} from {file}")
                data = json_open(file)
                if data is None:
                    continue

                try:
                    df = pd.json_normalize(data, meta=["coord"], sep="_")
                except KeyError as e:
                    logger.error(
                        f"Missing expected field in current weather JSON for {city}: {e}")
                    continue

                columns_to_keep = [
                    "main_temp",
                    "main_feels_like",
                    "main_temp_max",
                    "main_temp_min",
                    "main_humidity",
                    "main_pressure",
                    "dt",
                    "coord_lat",
                    "coord_lon"
                ]

                current_weather_df = df[columns_to_keep].copy()
                current_weather_df['city'] = city

                current_weather_df['dt'] = pd.to_datetime(
                    current_weather_df['dt'], unit='s')
                drop_dupes_and_fill(current_weather_df, ['city', 'dt'], {
                    'dt': pd.Timestamp("1970-01-01")
                })
                logger.info(
                    f"Finished processing current weather data for {city} from {file}")
                all_current_weather.append(current_weather_df)

            elif 'forecast_weather' in str(file):
                logger.info(
                    f"Processing forecast weather data for {city} from {file}")
                data = json_open(file)
                if data is None:
                    continue

                try:
                    df = pd.json_normalize(
                        data, record_path='list', meta=['city'], sep='_')
                except KeyError as e:
                    logger.error(
                        f"Missing expected field in forecast weather JSON for {city}: {e}")
                    continue

                df['dt'] = pd.to_datetime(df['dt'], unit='s')

                coord_df = pd.json_normalize(df['city'], sep='_')

                columns_to_keep = [
                    "main_temp",
                    "main_feels_like",
                    "main_temp_max",
                    "main_temp_min",
                    "main_humidity",
                    "main_pressure",
                    "dt"
                ]

                forecast_weather_df = df[columns_to_keep].copy()
                forecast_weather_df['coord_lat'] = coord_df['coord_lat']
                forecast_weather_df['coord_lon'] = coord_df['coord_lon']
                forecast_weather_df['city'] = city

                forecast_weather_df['dt'] = pd.to_datetime(
                    forecast_weather_df['dt'], unit='s')
                drop_dupes_and_fill(forecast_weather_df, ['city', 'dt'], {
                    'dt': pd.Timestamp("1970-01-01")})
                logger.info(
                    f"Finished processing forecast weather data for {city} from {file}")
                all_forecast_weather.append(forecast_weather_df)

    air_pollution_df = pd.concat(
        all_air_pollution, ignore_index=True) if all_air_pollution else pd.DataFrame()
    current_weather_df = pd.concat(
        all_current_weather, ignore_index=True) if all_current_weather else pd.DataFrame()
    forecast_weather_df = pd.concat(
        all_forecast_weather, ignore_index=True) if all_forecast_weather else pd.DataFrame()

    return air_pollution_df, current_weather_df, forecast_weather_df


def drop_dupes_and_fill(df, subset, fill_values=None):
    df.drop_duplicates(subset=subset, inplace=True)
    if fill_values:
        df.fillna(fill_values, inplace=True)
    return df


def json_open(file):
    try:
        with open(file) as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Missing file: {file}")
        return None
    except json.JSONDecodeError:
        logger.error(f"Corrupted JSON in: {file}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error with {file}: {e}")
        return None
