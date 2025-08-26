import pandas as pd
import json


aqi_map = {1: "Good", 2: "Fair", 3: "Moderate", 4: "Poor", 5: "Very Poor"}


def transform(raw_file):
    all_air_pollution = []
    all_current_weather = []
    all_forecast_weather = []
    for city, files in raw_file.items():
        for file in files:
            if 'air_pollution' in str(file):
                with open(file) as f:
                    data = json.load(f)
                    air_pollution_df = pd.json_normalize(
                        data, record_path="list", meta=["coord"], sep="_")

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
                        'main_aqi': 1,
                        'dt': pd.Timestamp("1970-01-01")
                    })
                    mask = air_pollution_df['main_aqi'].between(1, 5)
                    air_pollution_df.loc[mask == False,
                                         'main_aqi_desc'] = 'Unknown'
                    all_air_pollution.append(air_pollution_df)

            elif 'current_weather' in str(file):
                with open(file) as f:
                    data = json.load(f)
                    df = pd.json_normalize(data, meta=["coord"], sep="_")
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
                    all_current_weather.append(current_weather_df)

            elif 'forecast_weather' in str(file):
                with open(file) as f:
                    data = json.load(f)
                    df = pd.json_normalize(
                        data, record_path='list', meta=['city'], sep='_')
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
                    all_forecast_weather.append(forecast_weather_df)

    air_pollution_df = pd.concat(all_air_pollution, ignore_index=True)
    current_weather_df = pd.concat(all_current_weather, ignore_index=True)
    forecast_weather_df = pd.concat(all_forecast_weather, ignore_index=True)

    return air_pollution_df, current_weather_df, forecast_weather_df


def drop_dupes_and_fill(df, subset, fill_values=None):
    df.drop_duplicates(subset=subset, inplace=True)
    if fill_values:
        df.fillna(fill_values, inplace=True)
    return df
