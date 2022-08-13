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

insert_table_sql = "OPTIMIZE default.deltatb ZORDER BY (loc)"

# generate symlink_format_manifest for athena
generate_mode_sql = "GENERATE symlink_format_manifest FOR TABLE default.deltatb"

# insert into Delta table using simple values
spark.sql(insert_table_sql)
spark.sql(generate_mode_sql)
print("Inserting data into table deltatb.")
