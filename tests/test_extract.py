"""
Unit Tests for Weather Data Extraction Module

These tests cover the functions in src.extract:
- fetch_coordinates
- fetch_weather
- save_file

Tests include:
- Successful API calls
- Handling of missing or invalid data
- Network/API errors
- Partial failures for fetch_weather
- JSON file writing for save_file

Usage:
    Run all tests with pytest:
        pytest tests/
"""

from src.extract import extract, fetch_coordinates, fetch_weather, save_file
from src.utils.etl_config import ExtractionConfig
from requests.exceptions import ConnectionError
import json

# Helper function to create a test config


def make_test_config(tmp_path):
    return ExtractionConfig(
        api_key="fake_key",
        raw_path=str(tmp_path),
        units="metric",
        cities=["Athens"]
    )

# Test fetching coordinates successfully


def test_fetch_coordinates_success(mocker):
    mock_get = mocker.patch("src.extract.requests.get")

    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = [{'lat': 37.98, 'lon': 23.72}]

    result = fetch_coordinates("Athens", "fake_key")

    assert result == (37.98, 23.72)
    mock_get.assert_called_once_with(
        "http://api.openweathermap.org/geo/1.0/direct?q=Athens&limit=5&appid=fake_key", timeout=10)

# Test fetching coordinates not found


def test_fetch_coordinates_not_found(mocker):
    mock_get = mocker.patch("src.extract.requests.get")

    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = []
    result = fetch_coordinates("", "fake_key")

    assert result == (None, None)
    mock_get.assert_called_once_with(
        "http://api.openweathermap.org/geo/1.0/direct?q=&limit=5&appid=fake_key", timeout=10)

# Test fetching coordinates API error


def test_fetch_coordinates_api_error(mocker):
    mock_get = mocker.patch("src.extract.requests.get")

    mock_get.side_effect = ConnectionError("Network error")

    result = fetch_coordinates("Athens", "fake_key")

    assert result == (None, None)
    mock_get.assert_called_once_with(
        "http://api.openweathermap.org/geo/1.0/direct?q=Athens&limit=5&appid=fake_key", timeout=10)

# Test fetching weather data successfully


def test_fetch_weather_success(mocker, tmp_path):
    mock_get = mocker.patch("src.extract.requests.get")
    mock_save = mocker.patch("src.extract.save_file")
    mock_sleep = mocker.patch("time.sleep", return_value=None)

    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {
        "main": {"temp": 27.06, "feels_like": 28.31, }}

    config = make_test_config(tmp_path)
    result = fetch_weather("Athens", 37.98, 23.72, config)

    expected_urls = [
        f"https://api.openweathermap.org/data/2.5/weather?lat=37.98&lon=23.72&units=metric&appid=fake_key",
        f"https://api.openweathermap.org/data/2.5/forecast?lat=37.98&lon=23.72&units=metric&appid=fake_key",
        f"https://api.openweathermap.org/data/2.5/air_pollution?lat=37.98&lon=23.72&units=metric&appid=fake_key",
    ]
    called_urls = [call.args[0] for call in mock_get.call_args_list]
    assert called_urls == expected_urls

    assert mock_save.call_count == 3
    for call in mock_save.call_args_list:
        path, data = call.args
        assert "Athens" in str(path)
        assert isinstance(data, dict)

    assert all("Athens" in str(path) for path in result)
    assert len(result) == 3

# Test fetching weather data failure


def test_fetch_weather_failure(mocker, tmp_path):
    mock_get = mocker.patch("src.extract.requests.get")
    mock_save = mocker.patch("src.extract.save_file")
    mock_sleep = mocker.patch("time.sleep", return_value=None)

    mock_get.side_effect = ConnectionError("Network error")

    config = make_test_config(tmp_path)
    result = fetch_weather("Athens", 37.98, 23.72, config)

    assert mock_get.call_count == 3

    assert mock_save.call_count == 0

    assert result == []

# Test fetching weather data partial failure


def test_fetch_weather_partial_failure(mocker, tmp_path):
    mock_get = mocker.patch("src.extract.requests.get")
    mock_save = mocker.patch("src.extract.save_file")
    mock_sleep = mocker.patch("time.sleep", return_value=None)

    successful_response = mocker.Mock()
    successful_response.status_code = 200
    successful_response.json.return_value = {"main": {"temp": 27.06}}
    mock_get.side_effect = [
        successful_response,
        ConnectionError("Network error"),
        successful_response
    ]
    config = make_test_config(tmp_path)
    result = fetch_weather("Athens", 37.98, 23.72, config)

    assert mock_get.call_count == 3

    assert mock_save.call_count == 2

    assert all("Athens" in str(path) for path in result)
    assert len(result) == 2

# Test saving data to a JSON file


def test_save_file(tmp_path):

    file_path = tmp_path / "test.json"
    data = {"city": "Athens", "temp": 27.06}

    save_file(file_path, data)

    assert file_path.exists()
    with open(file_path, "r", encoding="utf-8") as f:
        saved = json.load(f)
    assert saved == data

# Test extracting weather data


def test_extract(mocker, tmp_path):
    mock_coords = mocker.patch(
        "src.extract.fetch_coordinates", return_value=(None, None))
    mock_weather = mocker.patch("src.extract.fetch_weather")

    config = make_test_config(tmp_path)
    config.cities = ["InvalidCity"]
    result = extract(config)

    assert result == {}
    mock_coords.assert_called_once()
    mock_weather.assert_not_called()
