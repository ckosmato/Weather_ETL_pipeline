'''
Unit tests for the transform module

These tests cover the functions in the transform module.
- json_open: Tests for opening and reading JSON files.
- drop_dupes_and_fill: Tests for dropping duplicates and filling missing values.
- transform: Integration tests for the entire transformation process.

Usage:
Run all tests with pytest:
        pytest tests/

'''

from src.transform import json_open, drop_dupes_and_fill, transform
import json
import pandas as pd


def test_json_open_success(tmp_path):
    test_data = {"key": "value"}
    file_path = tmp_path / "test.json"
    with open(file_path, 'w') as f:
        json.dump(test_data, f)

    result = json_open(file_path)
    assert result == test_data
    assert isinstance(result, dict)


def test_json_open_missing(tmp_path):
    file_path = tmp_path / "missing.json"
    result = json_open(file_path)
    assert result is None


def test_json_open_corrupted(tmp_path):
    file_path = tmp_path / "bad.json"
    file_path.write_text("{ invalid json }")
    result = json_open(file_path)
    assert result is None


def test_drop_dupes_and_fill():
    df = pd.DataFrame({
        "city": ["Athens", "Athens", "Paris"],
        "dt": [1, 1, 2],
        "value": [10, None, None]
    })

    cleaned = drop_dupes_and_fill(
        df, subset=["city", "dt"], fill_values={"value": 0})

    assert len(cleaned) == 2

    assert int(cleaned.loc[cleaned["city"] == "Athens", "value"].iloc[0]) == 10
    assert int(cleaned.loc[cleaned["city"] == "Paris", "value"].iloc[0]) == 0


def test_transform_integration(tmp_path):

    air_pollution = {
        "list": [{"main_aqi": 3, "dt": 1690000000}], "coord": {"lat": 37.98, "lon": 23.72}
    }
    air_file = tmp_path / "air_pollution.json"
    air_file.write_text(json.dumps(air_pollution))

    current_weather = {
        "main_temp": 25, "main_feels_like": 26, "main_temp_max": 27,
        "main_temp_min": 24, "main_humidity": 50, "main_pressure": 1012,
        "dt": 1690000000, "coord": {"lat": 37.98, "lon": 23.72}
    }
    current_file = tmp_path / "current_weather.json"
    current_file.write_text(json.dumps(current_weather))

    forecast_weather = {
        "list": [{"main_temp": 25, "main_feels_like": 26, "main_temp_max": 27,
                  "main_temp_min": 24, "main_humidity": 50, "main_pressure": 1012,
                  "dt": 1690000000}],
        "city": {"coord": {"lat": 37.98, "lon": 23.72}}
    }
    forecast_file = tmp_path / "forecast_weather.json"
    forecast_file.write_text(json.dumps(forecast_weather))

    raw_file = {
        "Athens": [air_file, current_file, forecast_file]
    }

    air_df, current_df, forecast_df = transform(raw_file)

    assert not air_df.empty
    assert "main_aqi_desc" in air_df.columns
    assert air_df["main_aqi_desc"].iloc[0] == "Moderate"

    assert not current_df.empty
    assert "main_temp" in current_df.columns
    assert current_df["coord_lat"].iloc[0] == 37.98

    assert not forecast_df.empty
    assert "coord_lon" in forecast_df.columns
    assert forecast_df["coord_lon"].iloc[0] == 23.72
