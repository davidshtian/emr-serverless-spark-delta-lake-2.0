from pyspark.sql import SparkSession
import uuid
import sys

# init output_path variable
output_path = None

# use spark session instead of spark context as the entrypoint
spark = (
    SparkSession.builder.appName("SparkSQL")
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .enableHiveSupport()
    .getOrCreate()
)

# mock changes data
changes = spark.createDataFrame([
    (3, 'jack', 'sz'),
    (4, 'rose', 'sh'),
    (5, 'tom', 'sh')], 
    schema='id int, name string, loc string')

# register temp view
changes.createOrReplaceTempView("changes")

upsert_table_sql = """
MERGE INTO default.deltatb
USING changes
ON deltatb.id = changes.id
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED
  THEN INSERT *
"""

# generate symlink_format_manifest for athena
generate_mode_sql = "GENERATE symlink_format_manifest FOR TABLE default.deltatb"

# insert into Delta table using simple values
spark.sql(upsert_table_sql)
spark.sql(generate_mode_sql)
print("Upserting data into table deltatb.")
