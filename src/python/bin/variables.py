# Set variables

ENVN = 'TEST'
HEADER = 'true'
INFER_SCHEMA = 'true'
LOGGING_CONF = '/opt/PrescPipeline/src/python/util/logging_to_file.conf'

# Set other variables

APP_NAME = "USA Prescriber Research Report"
GS_DIM_CITY = "gs://data-prescribers/cities_dimension/us_cities_dimension.parquet"
GS_PRESC_DATA = "gs://data-prescribers/presc_data/USA_Presc_Medicare_Data_12021.csv"

PATH_CITY_SAVE_HDFS = 'hdfs://cluster-5ed2-m/user/root/data-prescribers-pipeline/city_data/'
PATH_PRESC_SAVE_HDFS = 'hdfs://cluster-5ed2-m/user/root/data-prescribers-pipeline/presc_data/'

PATH_CITY_SAVE_GCS = 'gs://data-prescribers/reports/presc-report'
PATH_PRESC_SAVE_GCS = 'gs://data-prescribers/reports/city-report'