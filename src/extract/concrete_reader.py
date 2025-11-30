import logging
from src.common.utils.logger import create_logger
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


class ParquetReader:
    def __init__(self, spark):
        self.spark = spark
        self.logger = create_logger(name=__name__, log_level=logging.INFO)

    def read_file(self, path: str) -> DataFrame:
        self.logger.info(f"Reading Parquet file : {path}")
        return self.spark.read.format("parquet").load(path)

