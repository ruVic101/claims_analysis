import logging

from src.common.utils.logger import create_logger
from src.extract.concrete_reader import ParquetReader


class ReaderFactory:

    def __init__(self, spark):
        self.spark = spark
        self.logger = create_logger(name=__name__, log_level=logging.INFO)

    def get_reader(self, file_format: str):
        file_format = file_format.lower()
        if file_format == 'parquet':
            return ParquetReader(self.spark)
