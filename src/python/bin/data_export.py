import logging
import logging.config
import variables as v

### Loading config file
logging \
    .config \
        .fileConfig(v.LOGGING_CONF)

logger = logging.getLogger(__name__)


def export_data(df, partitions_number, file_path, df_name, storage_type):

    try:
        logger.info(f"Function export_data() initiated for {df_name} to {storage_type}...")

        df.repartition(partitions_number) \
          .write \
          .format('parquet') \
          .save(file_path, compression= 'snappy')

    except Exception as e:
        logger.error(f"Error occurred in the function export_data(). Check Stack Trace. {str(e)}")
        raise

    else:
        logger.info(f"Function export_data() has been successfully completed for {df_name} to {storage_type}.")
