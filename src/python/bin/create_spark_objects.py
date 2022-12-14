import logging
import logging.config
import variables as v

from pyspark.sql import SparkSession

### Loading config file
logging \
    .config \
        .fileConfig(v.LOGGING_CONF)

logger = logging.getLogger(__name__)

def get_spark(envn, app_name) -> SparkSession:

    try:
        logger.info(f"Function get_spark() initiated. The '{envn}' envn is being used.")

        if envn == 'TEST':
            master = 'local'
        else:
            master = 'yarn'

        spark = SparkSession \
                .builder \
                .master(master) \
                .appName(app_name) \
                .getOrCreate()


    except NameError as e:
        logger.error(f"NameError occurred in the function get_spark(). Check the Stack Trace. {str(e)}", exc_info=True)
        raise

    except Exception as e:
        logger.error(f"Error occurred in the function get_spark(). Check the Stack Trace. {str(e)}", exc_info=True)

    else:
        logger.info("Spark object has been successfully created.")

    return spark
