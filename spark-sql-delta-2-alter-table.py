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

alter_table_sql = "ALTER TABLE default.deltatb SET TBLPROPERTIES(delta.compatibility.symlinkFormatManifest.enabled=true)"

# insert into Delta table using simple values
spark.sql(alter_table_sql)

print("Inserting data into table deltatb.")
