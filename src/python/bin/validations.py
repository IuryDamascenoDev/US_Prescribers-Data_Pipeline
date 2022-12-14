import logging
import logging.config
import pandas as pd
import variables as v

### Loading config file
logging \
    .config \
        .fileConfig(v.LOGGING_CONF)

logger = logging.getLogger(__name__)

def spark_validate(spark):
    try:
        output_df = spark.sql(""" SELECT current_date """) 
        logger.info("Validating spark ...")
        logger.info("Getting current date ...")
        logger.info("Date: " + str(output_df.collect()[0][0]))

    except NameError as e:
        logger.error(f"NameError occurred in the function spark_validate(). Check the Stack Trace. {str(e)}", exc_info=True)
        raise

    except Exception as e:
        logger.error(f"Error occurred in the function spark_validate(). Check the Stack Trace. {str(e)}", exc_info=True)
        raise

    else:
        logger.info("Spark object has been successfully validated.")


def df_count(df, df_name):
    try:
        logger.info(f"Count validation for {df_name} started. Counting ...")
        
        df_count = df.count()

        logger.info(f"DataFrame {df_name} count = {df_count}")

    except Exception as e:
        logger.error(f"Error occurred in the function df_count(). Check the Stack Trace. {str(e)}", exc_info=True)
        raise

    else:
        logger.info(f"Count validation for {df_name} has been successfully completed.")


def df_records(df, df_name):
    try:
        logger.info(f"Records validation for {df_name} started.")
        
        df_pandas = df.limit(10).toPandas()

        logger.info("Showing top 10 records: \n \t" + df_pandas.to_string(index=False))

    except Exception as e:
        logger.error(f"Error occurred in the function df_records(). Check the Stack Trace. {str(e)}", exc_info=True)
        raise

    else:
        logger.info(f"Records validation for {df_name} has been successfully completed.")


def df_schema(df, df_name):
    try:
        logger.info(f"Schema validation for {df_name} started.")
        logger.info(f"Schema of {df_name}:")
        
        sch = df.schema.fields
        for i in sch:
            logger.info(f"\t {i}")


    except Exception as e:
        logger.error(f"Error occurred in the function df_schema(). Check the Stack Trace. {str(e)}", exc_info=True)
        raise

    else:
        logger.info(f"Schema validation for {df_name} has been successfully completed.")
