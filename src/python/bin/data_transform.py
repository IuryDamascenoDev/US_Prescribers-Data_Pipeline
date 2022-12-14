import logging
import logging.config
import variables as v

from pyspark.sql.functions import countDistinct, sum, col, dense_rank
from pyspark.sql.window import Window
from udfs import column_values_count

### Loading config file
logging \
    .config \
        .fileConfig(v.LOGGING_CONF)

logger = logging.getLogger(__name__)


def city_report(df_city, df_presc):
    # City Report:
    #     Requirements:
    #        Calculate the Number of zips in each city.
    #        Calculate the number of distinct Prescribers assigned for each City.
    #        Calculate total TRX_CNT prescribed for each city.
    #        Do not report a city in the final report if no prescriber is assigned to it.

    #     Table columns:
    #        City Name
    #        State Name
    #        County Name
    #        City Population
    #        Number of Zips
    #        Prescriber Counts
    #        Total Trx counts

    try:
        logger.info("Function city_report() initiated ...")

        df_city_with_split = df_city.withColumn('zip_count', column_values_count(df_city.zips))

        df_presc_group = df_presc \
                        .groupBy("presc_state", "presc_city") \
                        .agg(countDistinct("presc_id") \
                        .alias("presc_count"), sum("trx_cnt").alias("trx_count"))

        df_city_join = df_city_with_split \
                        .join(df_presc_group, (df_city_with_split.state_id == df_presc_group.presc_state) \
                                                & (df_city_with_split.city == df_presc_group.presc_city), 'inner')

        df_city_final = df_city_join.select("city",
                                            "state_name",
                                            "county_name",
                                            "population",
                                            "zip_count",
                                            "trx_count",
                                            "presc_count")

    except Exception as e:
        logging.error(f"Error occurred in function city_report(). Check Stack Trace. {str(e)}", exc_info=True)

    logger.info("City report has been successfully created.")

    return df_city_final


def prescriber_report(df):
    # Prescriber Report:
    # Top 5 Prescribers with highest trx_cnt per each state.
    # Consider the prescribers only from 20 to 50 years of experience.
    # Layout:
    #   Prescriber ID
    #   Prescriber Full Name
    #   Prescriber State
    #   Prescriber Country
    #   Prescriber Years of Experience
    #   Total TRX Count
    #   Total Days Supply
    #   Total Drug Cost

    try:
        logger.info("Function prescriber_report() initiated ...")

        df_filter = df.select("presc_id",
                              "presc_full_name",
                              "presc_state",
                              "country_name",
                              "years_of_exp",
                              "trx_cnt",
                              "total_day_supply",
                              "total_drug_cost") \
                              .filter((df.years_of_exp >= 20) & (df.years_of_exp <= 50))

        spec = Window.partitionBy("presc_state").orderBy(col("trx_cnt").desc())
        df_rank = df_filter.withColumn("dense_rank", dense_rank().over(spec))

        df_presc_final = df_rank \
                 .filter(df_rank.dense_rank <= 5) \
                 .select("presc_id",
                         "presc_full_name",
                         "presc_state",
                         "country_name",
                         "years_of_exp",
                         "trx_cnt",
                         "total_day_supply",
                         "total_drug_cost")

    except Exception as e:
        logging.error(f"Error occurred in function prescriber_report(). Check Stack Trace. {str(e)}", exc_info=True)

    logger.info("Prescriber report has been successfully created.")

    return df_presc_final
