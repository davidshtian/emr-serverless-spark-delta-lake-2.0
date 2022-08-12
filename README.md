# Delta Lake 2.0 @ AWS EMR Serverless Spark Example 
This is a quick minimum viable example for Delta Lake 2.0 running on AWS EMR Serverless Spark, as the Delta Lake project announces the availability of 2.0 open source release.

> Notes: Supposed you've already configured AWS EMR Serverless Application, please refer to [Getting started with Amazon EMR Serverless
](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/getting-started.html) for details.

## Simple Demo

1. First upload the script to your S3 bucket.

```
aws s3 cp ./spark-sql-delta-2-simple.py s3://<your-s3-bucket>/scripts/
```

2. Follow [Delta Lake Release Link](https://github.com/delta-io/delta/releases/tag/v2.0.0) to download *delta-core_2.12* and *delta-storage* jar file, and upload to your S3 bucket.
```
aws s3 cp ./delta-core_2.12-2.0.0.jar s3://<your-s3-bucket>/
aws s3 cp ./delta-storage-2.0.0.jar s3://<your-s3-bucket>/
```

> Notes: Please remember to download and add delta-storage jar file, otherwise you would encounter error like *java.lang.NoClassDefFoundError: io/delta/storage/LogStore*

3. Run the command below to start the job.

```
aws emr-serverless start-job-run \
    --application-id <your-emr-serverless-application-id> \
    --execution-role-arn <your-emr-serverless-role-arn> \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://<your-s3-bucket>/scripts/spark-sql-delta-2-simple.py",
            "entryPointArguments": ["s3://<your-s3-bucket>/delta-lake/output"],
            "sparkSubmitParameters": "
            --conf spark.executor.cores=1 
            --conf spark.executor.memory=4g 
            --conf spark.driver.cores=1 
            --conf spark.driver.memory=4g 
            --conf spark.executor.instances=1 
            --conf spark.default.parallelism=1 
            --conf spark.jars=s3://<your-s3-bucket>/delta-core_2.12-2.0.0.jar,s3://<your-s3-bucket>/delta-storage-2.0.0.jar"
        }
    }' \
    --configuration-overrides '{
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {
                "logUri": "s3://<your-s3-bucket>/delta-lake-logs/"
            }
        }
    }'
```

4. Check the result in S3 bucket.

The data file is written succeesfully:
<img width="1120" alt="image" src="https://user-images.githubusercontent.com/14228056/184312503-11c95c68-ec33-4323-b0ad-dedd9997f6f2.png">

Use S3 select to have a quick look at the file:
<img width="1027" alt="image" src="https://user-images.githubusercontent.com/14228056/184312670-fb91fe4b-23d4-4d6b-9f4b-d685aaf0d4ed.png">


## Create Table in Glue Catalog

1. First upload the script to your S3 bucket.

```
aws s3 cp ./spark-sql-delta-2-create-table.py s3://<your-s3-bucket>/scripts/
```

2. Run the command below to start the job.

```
aws emr-serverless start-job-run \
    --application-id <your-emr-serverless-application-id> \
    --execution-role-arn <your-emr-serverless-role-arn> \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://<your-s3-bucket>/scripts/spark-sql-delta-2-create-table.py",
            "entryPointArguments": ["s3://<your-s3-bucket>/delta-lake/deltatb/"],
            "sparkSubmitParameters": "
            --conf spark.executor.cores=1 
            --conf spark.executor.memory=4g 
            --conf spark.driver.cores=1 
            --conf spark.driver.memory=4g 
            --conf spark.executor.instances=1 
            --conf spark.default.parallelism=1 
            --conf spark.jars=s3://<your-s3-bucket>/delta-core_2.12-2.0.0.jar,s3://<your-s3-bucket>/delta-storage-2.0.0.jar"
        }
    }' \
    --configuration-overrides '{
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {
                "logUri": "s3://<your-s3-bucket>/delta-lake-logs/"
            }
        }
    }'
```

3. Check the result in Glue Catalog.

![image](https://user-images.githubusercontent.com/14228056/184355201-20b76e93-5aea-4c69-88e3-eb512e3a859a.png)

> Notes: To allow Athena to query the data, *_symlink_format_manifest* need to be generated. Please refer to [Presto, Trino, and Athena to Delta Lake integration using manifests](https://docs.delta.io/latest/presto-integration.html) for details.


## Insert Data into Table in Glue Catalog

1. First upload the script to your S3 bucket.

```
aws s3 cp ./spark-sql-delta-2-insert-table.py s3://<your-s3-bucket>/scripts/
```

2. Run the command below to start the job.

```
aws emr-serverless start-job-run \
    --application-id <your-emr-serverless-application-id> \
    --execution-role-arn <your-emr-serverless-role-arn> \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://<your-s3-bucket>/scripts/spark-sql-delta-2-insert-table.py",
            "sparkSubmitParameters": "
            --conf spark.executor.cores=1 
            --conf spark.executor.memory=4g 
            --conf spark.driver.cores=1 
            --conf spark.driver.memory=4g 
            --conf spark.executor.instances=1 
            --conf spark.default.parallelism=1 
            --conf spark.jars=s3://<your-s3-bucket>/delta-core_2.12-2.0.0.jar,s3://<your-s3-bucket>/delta-storage-2.0.0.jar"
        }
    }' \
    --configuration-overrides '{
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {
                "logUri": "s3://<your-s3-bucket>/delta-lake-logs/"
            }
        }
    }'
```

3. Check the result in S3 bucket.
<img width="1070" alt="image" src="https://user-images.githubusercontent.com/14228056/184358274-1b61d734-4448-4467-8f43-c3c9004d5634.png">

4. Query the data via AWS Athena.

First create table for Athena:
```
CREATE EXTERNAL TABLE "default"."deltatb_athena"(
  `id` int, 
  `name` string, 
  `loc` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://<your-s3-bucket>/delta-lake/deltatb/_symlink_format_manifest'
```

Then query the data.
```
SELECT * FROM "default"."deltatb_athena";
```

<img width="922" alt="image" src="https://user-images.githubusercontent.com/14228056/184358181-bb6c18ba-34ec-4f35-8fad-56b884bf0a14.png">


> Notes: To allow Athena to query the data, *_symlink_format_manifest* need to be generated and updated. Please refer to [Presto, Trino, and Athena to Delta Lake integration using manifests](https://docs.delta.io/latest/presto-integration.html) for details.

## Upsert Data into Table in Glue Catalog

TBD...
