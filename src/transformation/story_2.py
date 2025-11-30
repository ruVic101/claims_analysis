from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from src.common.utils.logger import create_logger

def analyze_for_story2(claims_df: DataFrame, products_df: DataFrame) -> DataFrame:

    logger = create_logger(__name__)
    logger.info("Story2: Create a comprehensive view of claim activity for our property and casualty business, showing patterns by location, risk level, and claim characteristics.")
    logger.info("Starting series of transformation to analyze the data..")

    filtered_products = products_df \
                            .filter(col('product_code').isin(['AUTO', 'HOME'])) \
                            .withColumn('end_date', when(col('is_current') == True, current_date()))

    joined_df_auto_home = claims_df.alias('cl') \
                            .join(filtered_products.alias('p'),
                                    (col('cl.claim_type') == col('p.product_code'))
                                  & (
                                      (col('cl.claim_date') >= col('p.effective_date'))
                                      & (col('cl.claim_date') <= col('p.end_date'))
                                    ),'inner') \
                            .select(
                                col('cl.claim_id'),
                                col('p.product_code'),
                                col('cl.claim_amount'),
                                col('cl.status'),
                                col('cl.country'),
                                col('cl.claim_date'),
                                col('p.risk_category'),
                                col('p.effective_date'),
                                col('p.end_date'),
                                col('p.version')
                            )

    agg_auto_home_df = joined_df_auto_home \
                            .groupBy(
                                col('country'),
                                col('product_code'),
                                col('risk_category'),
                                col('version').alias('product_version')
                            ).agg(
                                    count(col('claim_id')).alias('number_of_claims'),
                                    count(
                                        when(upper(col('status')) == 'APPROVED', col('claim_id'))
                                          ).alias('number_of_approved_claims'),
                                    sum(col('claim_amount')).cast('bigint').alias('total_claimed_amount'),
                                    sum(
                                        when(upper(col('status')) == 'APPROVED', col('claim_amount'))
                                          ).cast('bigint').alias('approved_claim_amount'),
                                    (
                                      (
                                        (count(when(upper(col('status')) == 'APPROVED', col('claim_id'))))
                                            / (count(col('claim_id')))
                                      ) * 100
                                    ).alias('approved_claim_ratio')
                                ) \
                                .withColumn('avg_claim_amount',
                                                (col('total_claimed_amount') / col('number_of_claims'))
                                            )

    product_dim = filtered_products \
                            .select(col('product_code'),
                                    col('version'),
                                    col('avg_premium'),
                                    col('commission_rate'),
                                    col('is_current')
                                    ).distinct()

    agg_data_with_product_info = agg_auto_home_df.alias('a') \
                                    .join(product_dim.alias('p'),
                                          (col('a.product_code') == col('p.product_code'))
                                          & (col('a.product_version') == col('p.version')), 'inner') \
                                    .select(
                                        col('a.*'),
                                        col('p.avg_premium'),
                                        col('commission_rate'),
                                        when(col('p.is_current') == True, 'ACTIVE') \
                                            .otherwise('INACTIVE').alias('product_status')
                                    ).orderBy(
                                    col('country'),
                                    col('product_code')
                                    )

    return agg_data_with_product_info