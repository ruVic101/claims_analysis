import logging
import traceback
from src.common.utils.common_utils import create_spark_session
from src.common.utils.logger import create_logger
from src.extract.reader_factory import ReaderFactory
from src.transformation.story_1 import analyze_for_story1
from src.transformation.story_2 import analyze_for_story2


def main():
    import os
    # print(os.environ.get("HADOOP_HOME"))
    # print(os.environ.get("PATH"))

    logger = create_logger(__name__, logging.INFO)
    spark = create_spark_session('swiss_re_job')
    logger.info(f"spark session : {spark.sparkContext.applicationId}")

    # print(spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion())

    factory = ReaderFactory(spark)
    claims_data_path = './data/claims.parquet'
    customers_data_path = './data/customers.parquet'
    products_data_path = './data/products.parquet'
    try:
        parquet_reader = factory.get_reader('parquet')

        # Read claims
        claims_df = parquet_reader.read_file(claims_data_path)
        # claims_df.show(10, truncate=False)

        # Read customers
        customers_df = parquet_reader.read_file(customers_data_path)
        # customers_df.show(10, truncate=False)

        # Read products
        products_df = parquet_reader.read_file(products_data_path)
        # products_df.show(10, truncate=False)

        # print(f"claims: {claims_df.columns}")
        # print(f"customers: {customers_df.columns}")
        # print(f"products: {products_df.columns}")

        # Result for Story1: Provide insights into claim behavior for our Health and Life products across various customer tiers and geographic locations.
        story_1_df = analyze_for_story1(claims_df, products_df, customers_df)
        story_1_df.coalesce(1).write.option('header','true').format("csv").mode('overwrite').save('./output/story_1/')

        story_2_df = analyze_for_story2(claims_df, products_df)
        story_2_df.coalesce(1).write.option('header','true').format("csv").mode('overwrite').save('./output/story_2/')
        # story_2_df.show(1000)

        #TODO-- do I need to write this data to some place?

    except Exception as ex:
        traceback_message = traceback.format_exc()
        logger.error(f"Error reading parquet file {claims_data_path}.")
        logger.error(f"Error : {ex}")
        logger.error(f"traceback: {traceback_message}")


if __name__ == '__main__':
    main()
