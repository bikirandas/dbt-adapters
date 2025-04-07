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

from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import current_timestamp
import pyspark

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

def dq_check(dbt_model):
    """
    Decorator function to capture the data consistency check at each model
    :param dbt_model: python model function
    :return:
    """

    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        dbt = args[0] if len(args) > 0 else kwargs.get("dbt", None)
        spark = args[1] if len(args) > 1 else kwargs.get("spark_session")
        error_msg = None

        df = dbt_model(*args, **kwargs)
        model_end_time = datetime.now()
        dbt_schema = dbt.this.schema
        model_execution_time = model_end_time - start_time
        domain = dbt_schema.split('_')[1]
        vendor = dbt_schema.rsplit('_', maxsplit=1)[-1]

       {#  TODO: Remove all hard coded values  #}
        invocation_id = f"{dbt.config.get('invocation_id')}"
        source_count = df.count()
        audit_table_name = 'dq_audit'
        env = dbt.config.get("target_name", "dev")
        env = "dev" if env == "default" else env
        athena_output = f"s3://{schema.replace('_', '-')}-{env}/athena-query-results/"
        table_name = dbt.this.identifier
        target_table = f"{dbt_schema}.{table_name}"

        sc = spark.sparkContext
        script_bucket = f"{schema.replace('_', '-')}-dev" if env == "dev" else f"{schema.replace('_', '-')}-prod"
        sc.addPyFile(f"s3://{script_bucket}/library/pymodules/dq_utils.py")

        from dq_utils import run_athena_query

        log_data = (f"('{table_name}', '{invocation_id}', '{vendor}', {source_count}, '{target_table}',"
                    f"current_timestamp, '{model_execution_time}', '{error_msg if error_msg else ''}')")

        log_query = (f"INSERT INTO {dbt_schema}.{audit_table_name} "
                     f"(model_name, invocation_id, source, source_count, target_table_name, "
                     f"audit_datetime, model_runtime, error_msg) values {log_data}")

        result = run_athena_query(dbt_schema, log_query, athena_output)
        return df

    return wrapper

@dq_check
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

dbt = SparkdbtObj()
df = model(dbt, spark)
materialize(spark, df, dbt.this)
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
