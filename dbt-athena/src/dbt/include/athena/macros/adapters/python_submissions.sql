{%- macro athena__py_save_table_as(compiled_code, target_relation, optional_args={}) -%}
    {% set location = optional_args.get("location") %}
    {% set format = optional_args.get("format", "parquet") %}
    {% set mode = optional_args.get("mode", "overwrite") %}
    {% set write_compression = optional_args.get("write_compression", "snappy") %}
    {% set partitioned_by = optional_args.get("partitioned_by") %}
    {% set bucketed_by = optional_args.get("bucketed_by") %}
    {% set sorted_by = optional_args.get("sorted_by") %}
    {% set merge_schema = optional_args.get("merge_schema", true) %}
    {% set bucket_count = optional_args.get("bucket_count") %}
    {% set field_delimiter = optional_args.get("field_delimiter") %}
    {% set spark_ctas = optional_args.get("spark_ctas", "") %}
    {% set submission_method = optional_args.get("submission_method", "") %}
    {% set table_type = optional_args.get("table_type", "") %}
    {% set spark_properties = optional_args.get("spark_properties", "") %}

import os
import re
import time
from datetime import datetime

import pyspark
from pyspark.sql.utils import AnalysisException
from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp, col, count, max

{% if submission_method == "lambda" %}
spark = pyspark.sql.SparkSession.builder \
    .appName("dbt_{{ target_relation.schema}}_{{ target_relation.identifier }}") \
    .master("local[*]") \
    {%- if table_type == "iceberg" %}
    .config("spark.sql.catalog.AwsDataCatalog.warehouse", "{{ location | replace('s3://', 's3a://') }}") \
    {%- endif %}
    .enableHiveSupport().getOrCreate()
{% endif %}

{% if submission_method == "emr_serverless" %}
spark = pyspark.sql.SparkSession.builder.appName("dbt_{{ target_relation.schema}}_{{ target_relation.identifier }}").enableHiveSupport().getOrCreate()
{% endif %}


{{ compiled_code }}
def materialize(spark_session, df, target_relation):
    import pandas
    if isinstance(df, pyspark.sql.dataframe.DataFrame):
        pass
    elif isinstance(df, pandas.core.frame.DataFrame):
        df = spark_session.createDataFrame(df)
    else:
        msg = f"{type(df)} is not a supported type for dbt Python materialization"
        raise Exception(msg)

{% if spark_ctas|length > 0 %}
    df.createOrReplaceTempView("{{ target_relation.schema}}_{{ target_relation.identifier }}_tmpvw")
    spark_session.sql("""
    {{ spark_ctas }}
    select * from {{ target_relation.schema}}_{{ target_relation.identifier }}_tmpvw
    """)
{% else %}
    writer = df.write \
    .format("{{ format }}") \
    .mode("{{ mode }}") \
    .option("path", "{{ location }}") \
    .option("compression", "{{ write_compression }}") \
    .option("mergeSchema", "{{ merge_schema }}") \
    .option("delimiter", "{{ field_delimiter }}")
    if {{ partitioned_by }} is not None:
        writer = writer.partitionBy({{ partitioned_by }})
    if {{ bucketed_by }} is not None:
        writer = writer.bucketBy({{ bucket_count }},{{ bucketed_by }})
    if {{ sorted_by }} is not None:
        writer = writer.sortBy({{ sorted_by }})

    writer.saveAsTable(
        name="{{ target_relation.schema}}.{{ target_relation.identifier }}",
    )
{% endif %}

    return "Success: {{ target_relation.schema}}.{{ target_relation.identifier }}"

{{ athena__py_get_spark_dbt_object() }}

def dq_check(dbt, df, source_count):
    error_msg = None
    dbt_schema = dbt.this.schema
    domain = dbt_schema.split('_')[1]
    data_source = dbt_schema.rsplit('_', maxsplit=1)[-1]
    invocation_id = f"{dbt.config.get('invocation_id')}"
    audit_table_name = 'dq_audit'
    env = dbt.config.get("target_name", "dev")
    env = "dev" if env == "default" else env
    athena_output = f's3://dlh-{domain}-{env}/athena-query-results/'
    table_name = dbt.this.identifier
    target_table = f"{dbt_schema}.{table_name}"
    max_load_date = spark.table(target_table) \
                     .agg(max("dl_load_date")).collect()[0][0]
    target_count = spark.table(target_table) \
                           .filter(col("dl_load_date") == max_load_date).count()
    log_row = Row(
        model_name=table_name,
        invocation_id=invocation_id,
        source=data_source,
        source_count=source_count,
        target_table_name=target_table,
        target_count=target_count,
        missing_count = int(source_count) - int(target_count),
        audit_datetime=current_timestamp(),
        error_msg=error_msg if error_msg else None
    )
    log_df = spark.createDataFrame([log_row])
    log_df.write \
          .format("parquet") \
          .mode("append") \
          .saveAsTable(f"{dbt_schema}.{audit_table_name}")

dbt = SparkdbtObj()
df = model(dbt, spark)
source_count = df.count()
materialize(spark, df, dbt.this)
dq_check(dbt, df, source_count)

{%- endmacro -%}

{%- macro athena__py_execute_query(query) -%}
{{ athena__py_get_spark_dbt_object() }}

def execute_query(spark_session):
    spark_session.sql("""{{ query }}""")
    return "OK"

dbt = SparkdbtObj()
execute_query(spark)
{%- endmacro -%}

{%- macro athena__py_get_spark_dbt_object() -%}
def get_spark_df(identifier):
    """
    Override the arguments to ref and source dynamically

    spark.table('awsdatacatalog.analytics_dev.model')
    Raises pyspark.sql.utils.AnalysisException:
    spark_catalog requires a single-part namespace,
    but got [awsdatacatalog, analytics_dev]

    So the override removes the catalog component and only
    provides the schema and identifer to spark.table()
    """
    return spark.table(".".join(identifier.split(".")[1:]).replace('"', ''))

class SparkdbtObj(dbtObj):
    def __init__(self):
        super().__init__(load_df_function=get_spark_df)
        self.source = lambda *args: source(*args, dbt_load_df_function=get_spark_df)
        self.ref = lambda *args: ref(*args, dbt_load_df_function=get_spark_df)

{%- endmacro -%}
