from pyspark.sql import SparkSession
import uuid
import sys

# get the argument of output path from the emr job parameter
if len(sys.argv) > 1:
    output_path = sys.argv[1]
else:
    print("S3 output location not specified, just print the output")

# use spark session instead of spark context as the entrypoint
spark = (
    SparkSession.builder.appName("SparkSQL")
    .config(
        "hive.metastore.client.factory.class",
        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
    )
    .enableHiveSupport()
    .getOrCreate()
)                         

if output_path:
    # generate random output path for testing
    if output_path.endswith("/"):
        url = output_path + str(uuid.uuid4())
    else:
        url = output_path + "/" + str(uuid.uuid4())   
    
    # creates a Delta table and outputs to target S3 bucket
    spark.range(5).write.format("delta").save(url)
    print("Writing range data to S3 output: " + url + ".")
    
    # reads a Delta table and outputs to target S3 bucket (shown in logs)
    spark.read.format("delta").load(url).show()
else:
    spark.range(5).show()
    print("No output, printing data.")
