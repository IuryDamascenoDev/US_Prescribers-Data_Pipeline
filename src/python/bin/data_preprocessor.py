import logging
import logging.config
import variables as v

from pyspark.sql.functions import upper, lit, regexp_replace, concat_ws, coalesce, avg, col, round
from pyspark.sql.window import Window

### Loading config file
logging \
    .config \
        .fileConfig(v.LOGGING_CONF)

logger = logging.getLogger(__name__)


def data_clean(df, df2):

    ### Clean df_city DataFrame
    # 1. Selects required columns
    # 2. Converting city, state and county columns to UpperCase
    # for joining DataFrames later
    try:
        logger.info("Function data_clean() initiated for df_city...")
        logger.info("Selecting columns ...")

        df_city_filter = df.select(df.city,
                                   df.state_id,
                                   df.state_name,
                                   df.county_name,
                                   df.population,
                                   df.zips)

        logger.info("Cleaning and formatting columns ...")

        df_city_upper = df_city_filter.select(upper(df_city_filter.city).alias("city"),
                                              df_city_filter.state_id,
                                              upper(df_city_filter.state_name).alias("state_name"),
                                              upper(df_city_filter.county_name).alias("county_name"),
                                              df_city_filter.population,
                                              df_city_filter.zips)

        logger.info("Function data_clean() successfully completed for df_city...")

        ### Clean df_city DataFrame
        # 1. Selects required columns
        # 2. Rename columns
        logger.info(f"Function data_clean() initiated for df_presc ...")
        logger.info("Selecting columns ...")

        df_presc_sel = df2.select(df2.npi.alias("presc_id"), \
                                  df2.nppes_provider_last_org_name.alias("presc_lname"), \
                                  df2.nppes_provider_first_name.alias("presc_fname"),
                                  df2.nppes_provider_city.alias("presc_city"), \
                                  df2.nppes_provider_state.alias("presc_state"), \
                                  df2.specialty_description.alias("presc_spclt"), \
                                  df2.years_of_exp, \
                                  df2.drug_name, \
                                  df2.total_claim_count.alias("trx_cnt"), \
                                  df2.total_day_supply, \
                                  df2.total_drug_cost)
        
        # 3. Add country column 'USA'
        logger.info("Creating country column ...")

        df_presc_sel = df_presc_sel.withColumn("country_name",lit("USA"))

        # 4. Clean years of experience column > original format example: '= 31.0'
        logger.info("Cleaning 'years_of_exp' column ...")

        pattern = '[= ]'
        df_presc_sel = df_presc_sel.withColumn('years_of_exp', regexp_replace('years_of_exp', pattern, replacement=''))

        # 5. Convert years of experience column type to number
        logger.info("Converting type of column 'years_of_exp' to Integer...")

        df_presc_sel = df_presc_sel.withColumn('years_of_exp', df_presc_sel.years_of_exp.cast('integer'))

        # 6. Combine First and Last name
        logger.info("Combining first and last names ...")

        df_presc_sel = df_presc_sel.select("*", (concat_ws(" ", df_presc_sel.presc_fname,
                                                                df_presc_sel.presc_lname))
                                                .alias('presc_full_name'))

        df_presc_sel = df_presc_sel.drop("presc_fname", "presc_lname")

        # 7. Check and Clean Null/NaN values for prescribers and drugs
        logger.info("Cleaning Null/NaN values for prescribers identifiers and drugs ...")

        df_presc_sel = df_presc_sel.dropna(subset="presc_id")

        df_presc_sel = df_presc_sel.dropna(subset="drug_name")

        # 8. Use TRX_CNT average on null values for each prescriber
        spec = Window.partitionBy("presc_id")
        df_presc_sel = df_presc_sel.withColumn("trx_cnt", coalesce("trx_cnt",
                                               round(avg("trx_cnt").over(spec))))
        df_presc_sel = df_presc_sel.withColumn("trx_cnt", col("trx_cnt").cast('integer'))

        logger.info("Function data_clean() successfully completed for df_presc ...")

    except Exception as e:
        logger.error(f"Error occurred in function data_clean(). Check Stack Trace. {str(e)}", exc_info=True)
    
    else:
        logger.info("Function data_clean() has been successfully completed.")
    
    return df_city_upper, df_presc_sel
