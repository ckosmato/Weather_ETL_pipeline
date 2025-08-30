'''
Weather data loading module.

This module is responsible for loading the transformed weather data
into the SQL Server database.

Usage:
    from src.load import load
    load(df)
'''

import sqlalchemy as sa
from sqlalchemy.engine import URL


def load(df, config):
    connection_url = URL.create(
        "mssql+pyodbc",
        host=config.host,
        database=config.database,
        query={
            "driver": config.driver,
            "trusted_connection": "yes"
        }
    )
    engine = sa.create_engine(connection_url)

    df.to_sql(
        df.name,
        con=engine,
        if_exists="append",
        index=False
    )
