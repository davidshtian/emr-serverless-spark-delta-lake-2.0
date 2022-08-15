from pyspark.sql import SparkSession
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
	# providing only the startingVersion/timestamp
	cdf_df = spark.read.format("delta") \
  			 .option("readChangeFeed", "true") \
  			 .option("startingVersion", 10) \
  			 .table("default.deltatb")

    # write change data to a temp output for test
	cdf_df.write.format("parquet").save(output_path)
	print("Reading change data feed from table deltatb.")
else:
    print("No output location.")
