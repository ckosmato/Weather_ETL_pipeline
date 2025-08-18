"""
Logging Configuration Module

Sets up logging for the ETL pipeline with both file and console output.

Usage:
    from config import setup_logging
    setup_logging()
"""

import logging


def setup_logging():
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("../logs/etl.log"),
            logging.StreamHandler()
        ]
    )
