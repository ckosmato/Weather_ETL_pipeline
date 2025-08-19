"""
ETL Pipeline Entry Point

Loads configuration, sets up logging, ensures folder structure, 
and runs the ETL pipeline.

Usage:
    python main.py
"""


from src.extract import extract
import logging
from src.utils.logger import setup_logging
from src.utils.etl_config import setup_extraction_config

setup_logging()

logger = logging.getLogger(__name__)


def main():
    logger.info('Setting up extraction configuration')
    extract_config = setup_extraction_config()

    logger.info('Starting extraction of data from API')
    extract(extract_config)
    logger.info('Extraction of data from API finished')


if __name__ == '__main__':
    main()
