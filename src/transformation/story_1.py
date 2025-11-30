from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from src.common.utils.logger import create_logger

def analyze_for_story1(claims_df: DataFrame, products_df: DataFrame, customers_df: DataFrame) -> DataFrame:

    logger = create_logger(__name__)
    logger.info("Story1: Insights into claim behavior for our Health and Life products across various customer tiers and geographic locations.")
    logger.info("Starting series of transformation to analyze the data..")

    # Is the analysis has to be done only for the active products? - I have assumed since we are analyzing claims details and not many details are being fetched from products, I have decided to us only Active products.
    joined_df = customers_df.alias('cus') \
                            .join(claims_df.filter(col('claim_type').isin(['HEALTH', 'LIFE'])).alias('cl'),
                                  col('cl.customer_id') == col('cus.customer_id'), 'leftouter') \
                            .join(products_df.alias('p').filter(col('p.is_current') == True),
                                  col('cl.claim_type') == col('p.product_code'),'inner') \
                            .withColumn('isApproved',
                                        when(col('cl.status') == 'APPROVED', True).otherwise(False)) \
                            .select(
                                when(col('cus.tier').isNull(), 'UNKNOWN').otherwise(col('cus.tier')).alias('tier'),
                                col('cl.claim_id'),
                                col('cl.customer_id'),
                                col('cl.claim_type'),
                                col('cl.claim_amount'),
                                col('cl.country'),
                                col('isApproved')
                                )

    aggregated_approved_df = joined_df.filter(col('isApproved') == True) \
                                .groupBy(col('tier'), col('country')) \
                                .agg(
                                    sum(col('claim_amount')).cast('bigint').alias('approved_claim_amount'),
                                    count(col('claim_id')).alias('count_of_approved_claims')
                                )

    aggregated_all_claims_df = joined_df.groupBy(col('tier'), col('country')) \
                                .agg(
                                    sum(coalesce(col('claim_amount'),lit(0))).cast('bigint').alias('total_claimed_amount'),
                                    count(col('claim_id')).alias('total_claims_made')
                                )

    df_with_claim_approval_ratio = aggregated_approved_df.alias('apvrd') \
                                .join(aggregated_all_claims_df.alias('all'),['tier', 'country'], 'inner') \
                                .select(
                                    col('apvrd.tier'),
                                    col('apvrd.country'),
                                    col('all.total_claimed_amount'),
                                    col('apvrd.approved_claim_amount'),
                                    col('all.total_claims_made'),
                                    col('apvrd.count_of_approved_claims'),
                                    (
                                        (col('apvrd.count_of_approved_claims') / col('all.total_claims_made')) * 100
                                    ).alias('claim_settlement_ratio')
                                ) \
                                .orderBy(col('tier'), col('country'))

    return df_with_claim_approval_ratio