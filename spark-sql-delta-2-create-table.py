from pyspark.sql import SparkSession
import uuid
import sys

# init output_path variable
output_path = None

# get the argument of output path from the emr job parameter
if len(sys.argv) > 1:
    output_path = sys.argv[1]
else:
    print("S3 output location not specified, just print the output")

# use spark session instead of spark context as the entrypoint
spark = (
    SparkSession.builder.appName("SparkSQL")
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .enableHiveSupport()
    .getOrCreate()
)

if output_path:

    create_table_sql = f"""CREATE OR REPLACE TABLE default.deltatb (
     id INT,
     name STRING,
     loc STRING
    ) USING DELTA
    LOCATION "{output_path}"
    """

    # generate symlink_format_manifest for athena
    generate_mode_sql = "GENERATE symlink_format_manifest FOR TABLE default.deltatb"

    # creates a Delta table and outputs to target S3 bucket
    spark.sql(create_table_sql)
    spark.sql(generate_mode_sql)
    print("Creating table deltatb at S3 output: " + output_path + ".")
else:
    spark.range(5).show()
    print("No output, printing data.")
