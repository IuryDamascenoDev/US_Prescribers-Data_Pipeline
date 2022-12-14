from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

@udf(returnType = IntegerType())
def column_values_count(column):
    # Return numbers of elements in a column
    # separated by blank spaces
    return len(column.split(' '))
