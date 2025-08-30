/*
===============================================================================
DDL Script: Create Air Pollution , Current and Forecast Weather Tables
===============================================================================
Script Purpose:
    This script creates tables for the Air Pollution , Current and Forecast Weather
    data, dropping existing tables if they already exist.
	  Run this script to re-define the DDL structure of weather Tables
===============================================================================
*/

IF OBJECT_ID('air_pollution', 'U') IS NOT NULL
    DROP TABLE air_pollution;
GO

CREATE TABLE air_pollution (
	id INT IDENTITY(1,1) PRIMARY KEY,
	dt DATETIME,
	main_aqi INT,
	components_co FLOAT,
	components_no FLOAT,
	components_no2 FLOAT,
	components_o3 FLOAT,
	components_so2 FLOAT,
	components_pm2_5 FLOAT,
	components_pm10 FLOAT,
	components_nh3 FLOAT,
	main_aqi_desc NVARCHAR(50),
	coord_lon FLOAT,
	coord_lat FLOAT,
	city NVARCHAR(50)
	);
GO

IF OBJECT_ID('current_weather', 'U') IS NOT NULL
	DROP TABLE current_weather;
GO

CREATE TABLE current_weather (
	id INT IDENTITY(1,1) PRIMARY KEY,
	main_temp FLOAT,
	main_feels_like FLOAT,
	main_temp_max FLOAT,
	main_temp_min FLOAT,
	main_humidity INT,
	main_pressure INT,
	dt DATETIME,
	coord_lat FLOAT,
	coord_lon FLOAT,
	city NVARCHAR(50)
);
GO

IF OBJECT_ID('forecast_weather', 'U') IS NOT NULL
	DROP TABLE forecast_weather;
GO

CREATE TABLE forecast_weather (
	id INT IDENTITY(1,1) PRIMARY KEY,
	main_temp FLOAT,
	main_feels_like FLOAT,
	main_temp_max FLOAT,
	main_temp_min FLOAT,
	main_humidity INT,
	main_pressure INT,
	dt DATETIME,
	coord_lat FLOAT,
	coord_lon FLOAT,
	city NVARCHAR(50)
);
GO
