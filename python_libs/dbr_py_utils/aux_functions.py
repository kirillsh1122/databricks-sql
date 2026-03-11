from functools import reduce
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from datetime import datetime


def df_string_field_curation(dataframe: DataFrame):
    transformed_cols = []
    for field in dataframe.schema:
        if field.dataType.typeName() == "string":
            transformed_cols.append((field.name, F.nullif(F.trim(F.col(field.name)), F.lit("")).alias(field.name)))

    return reduce(lambda df, col: df.withColumn(col[0], col[1]), transformed_cols, dataframe)

def table_record_count_audit(spark: SparkSession, table_qualified_name: str, job_task_timestamp_utc: datetime):
    sql = f"""
    select '{table_qualified_name}' as table_qualified_name
        , max(insert_timestamp_utc)
        , format_number(count(*), 0) as records_inserted
    from {table_qualified_name}
    where insert_timestamp_utc = '{job_task_timestamp_utc}'
    """
    df = spark.sql(sql)
    df.display()