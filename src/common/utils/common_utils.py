from pyspark.sql import SparkSession
from src.common.utils.logger import create_logger


def create_spark_session(app_name: str):
    logger = create_logger(__name__)
    logger.info(f"Creating Spark session: {app_name}")
    spark_session = SparkSession.builder.master('local[*]').appName(app_name).getOrCreate()
    spark_session.sparkContext.setLogLevel("ERROR")
    spark_session.conf.set("spark.hadoop.io.nativeIO.enabled", "false")
    return spark_session


