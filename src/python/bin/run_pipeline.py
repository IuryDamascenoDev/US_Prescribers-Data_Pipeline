# Import modules
import sys
import logging
import logging.config

import variables as v
from create_spark_objects import get_spark
from validations import spark_validate, df_count, df_records, df_schema
from data_ingest import load_files
from data_preprocessor import data_clean
from data_transform import city_report, prescriber_report
from data_export import export_data

### Loading Config File
logging \
    .config \
        .fileConfig(v.LOGGING_CONF)


def run_pipeline():

    try:
        logging.info("Function main() started ...")


        ### Spark Object
        spark = get_spark(v.ENVN, v.APP_NAME)
        logging.info("Spark object created.")

        # Validate spark object
        spark_validate(spark)


        ### Data Ingestion

        # Setting files variables
        dim_city_path = v.GS_DIM_CITY
        city_file_format = dim_city_path.split('.')[1]
        city_header = 'NA'
        city_infer_schema = 'NA'

        presc_data_path = v.GS_PRESC_DATA
        presc_file_format = presc_data_path.split('.')[1]
        presc_header = v.HEADER
        presc_infer_schema = v.INFER_SCHEMA

        # Load files
        df_city = load_files(spark = spark,
                            file_path = dim_city_path,
                            file_format = city_file_format,
                            header = city_header,
                            infer_schema = city_infer_schema)

        df_presc = load_files(spark = spark,
                             file_path = presc_data_path,
                             file_format = presc_file_format,
                             header = presc_header,
                             infer_schema = presc_infer_schema)

        # Validate Data Ingestion
        df_count(df = df_city, df_name = 'df_city')
        df_records(df = df_city, df_name = 'df_city')

        df_count(df = df_presc, df_name = 'df_presc')
        df_records(df = df_presc, df_name = 'df_presc')

        logging.info("Data ingestion completed and validated.")


        ### Preprocessing data

        # Data cleaning
        df_city_sel, df_presc_sel = data_clean(df_city, df_presc)

        # Validate
        df_records(df = df_city_sel, df_name = 'df_city_sel')

        df_count(df = df_presc_sel, df_name = 'df_presc_sel')
        df_records(df = df_presc_sel, df_name = 'df_presc_sel')
        df_schema(df = df_presc_sel, df_name = 'df_presc_sel')

        logging.info("Data preprocessing completed and validated.")


        ### Run transform data

        df_city_final = city_report(df_city_sel, df_presc_sel)
        df_records(df_city_final, "df_city_final")
        df_schema(df_city_final, "df_city_final")


        df_presc_final = prescriber_report(df_presc_sel)
        df_records(df_presc_final, "df_presc_final")
        df_schema(df_presc_final, "df_presc_final")

        logging.info("Data tranform completed and validated.")


        ### Run export data

        # Exporting data to hdfs for possible future transformations
        # and reports
        # And to GCS for availability
        export_data(df_city_final, 1, v.PATH_CITY_SAVE_HDFS, "df_city_final", "HDFS")
        export_data(df_city_final, 1, v.PATH_CITY_SAVE_GCS, "df_city_final", "GCS")

        export_data(df_presc_final, 5, v.PATH_PRESC_SAVE_GCS, "df_presc_final", "GCS")
        export_data(df_presc_final, 5, v.PATH_PRESC_SAVE_HDFS, "df_presc_final", "HDFS")

        # Validating export
        gcs_city_report = load_files(spark, v.PATH_CITY_SAVE_GCS, 'parquet', 'true', 'true')
        gcs_presc_report = load_files(spark, v.PATH_PRESC_SAVE_GCS, 'parquet', 'true', 'true')


        hdfs_city_report = load_files(spark, v.PATH_CITY_SAVE_HDFS, 'parquet', 'true', 'true')
        hdfs_presc_report = load_files(spark, v.PATH_PRESC_SAVE_HDFS, 'parquet', 'true', 'true')


        df_records(hdfs_city_report, 'hdfs_city_report')
        df_records(hdfs_presc_report, 'hdfs_presc_report')

        df_records(gcs_presc_report, 'gcs_presc_report')
        df_records(gcs_city_report, 'gcs_city_report')

        logging.info("Pipeline execution completed. \n")


    except Exception as e:
        logging.error(f"Error occurred in the function - main().\
             Check the Stack Trace to fix it in the respective module. {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    logging.info("Running Prescriber Pipeline.")
    run_pipeline()
