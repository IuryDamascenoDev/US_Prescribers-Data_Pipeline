import logging
import logging.config
import variables as v

### Loading config file
logging \
    .config \
        .fileConfig(v.LOGGING_CONF)

logger = logging.getLogger(__name__)


def load_files(spark, file_path, file_format, header, infer_schema):

    try:
        logger.info("Function load_files() initiated ...")

        if file_format == 'parquet':
            df = spark \
                .read \
                .format(file_format) \
                .load(file_path)

        elif file_format == 'csv':
            df = spark \
                .read \
                .format(file_format) \
                .options(header=header) \
                .options(inferSchema = infer_schema) \
                .load(file_path)

    except Exception as e:
        logger.error(f"Error occurred in the function load_files(). Check Stack Trace. {str(e)}")

    else:
        file_name = file_path.split('/')[-1]
        logger.info(f"The file {file_name} has been succesfully loaded to DataFrame.")

    return df
