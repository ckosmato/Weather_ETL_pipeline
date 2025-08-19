"""
Logging Configuration Module

Sets up logging for the ETL pipeline with both file and console output.

Usage:
    from logger import setup_logging
    setup_logging()
"""

import os
import logging

# Setup logging configuration


def setup_logging():
    os.makedirs('logs', exist_ok=True)
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("logs/etl.log"),
            logging.StreamHandler()
        ]
    )
