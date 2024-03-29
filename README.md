# Delta Lake @ AWS EMR Serverless Spark Example 
This is a quick minimum viable example for Delta Lake 2.0 running on AWS EMR Serverless Spark, as the Delta Lake project announces the availability of 2.0 open source release and adds fancy features like Z-Order and Change Data Feed. 

The example also shows cross data analytics capabilities on AWS by using Athena and Redshift. And the example is use [AWS EMR Serverless 6.7.0](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/release-versions.html) where Spark is version 3.2.1.

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

> Notes: To allow Athena to query the data, *_symlink_format_manifest* need to be generated. Please refer to [Presto, Trino, and Athena to Delta Lake integration using manifests](https://docs.delta.io/latest/presto-integration.html) for details. To update manifest file automatically, you could set the table property *delta.compatibility.symlinkFormatManifest.enabled=true*, please refer to [Step 3: Update manifests](https://docs.delta.io/latest/presto-integration.html#-step-3-update-manifests) for details and use *spark-sql-delta-2-alter-table.py*.


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
1. First upload the script to your S3 bucket.

```
aws s3 cp ./spark-sql-delta-2-upsert-table.py s3://<your-s3-bucket>/scripts/
```

2. Run the command below to start the job.

```
aws emr-serverless start-job-run \
    --application-id <your-emr-serverless-application-id> \
    --execution-role-arn <your-emr-serverless-role-arn> \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://<your-s3-bucket>/scripts/spark-sql-delta-2-upsert-table.py",
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

<img width="1066" alt="image" src="https://user-images.githubusercontent.com/14228056/184373067-b6f33ec0-ede3-4990-9c72-1a9b326c9825.png">

4. Query the data via AWS Athena.

```
SELECT * FROM "default"."deltatb_athena";
```
<img width="917" alt="image" src="https://user-images.githubusercontent.com/14228056/184373014-7f4861fc-407a-4f3c-a403-d1e1b8ca66c9.png">

## Z-ORDER Data into Table in Glue Catalog
1. First upload the script to your S3 bucket.

```
aws s3 cp ./spark-sql-delta-2-zorder-table.py s3://<your-s3-bucket>/scripts/
```

2. Run the command below to start the job.

```
aws emr-serverless start-job-run \
    --application-id <your-emr-serverless-application-id> \
    --execution-role-arn <your-emr-serverless-role-arn> \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://<your-s3-bucket>/scripts/spark-sql-delta-2-zorder-table.py",
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

The files have been optimized and z-ordered, the final file number is optimized to 1 as the test data is quite small. But you can still check the delta log shown as below:
```
{
  "add": {
    "path": "part-00000-0e8b2e53-360b-4dd1-9b76-e74461999ac7-c000.snappy.parquet",
    "partitionValues": {},
    "size": 1020,
    "modificationTime": 1660391657000,
    "dataChange": false,
    "stats": "{\"numRecords\":8,\"minValues\":{\"id\":1,\"name\":\"alice\",\"loc\":\"bj\"},\"maxValues\":{\"id\":8,\"name\":\"tom\",\"loc\":\"sz\"},\"nullCount\":{\"id\":0,\"name\":0,\"loc\":0}}"
  }
}
{
  "remove": {
    "path": "part-00000-63e08eef-d894-46de-beb4-6d92647c6e05-c000.snappy.parquet",
    "deletionTimestamp": 1660391638300,
    "dataChange": false,
    "extendedFileMetadata": true,
    "partitionValues": {},
    "size": 952
  }
}
{
  "remove": {
    "path": "part-00000-07aa290f-d937-45fc-920b-b2e1ad8e8d0a-c000.snappy.parquet",
    "deletionTimestamp": 1660391638300,
    "dataChange": false,
    "extendedFileMetadata": true,
    "partitionValues": {},
    "size": 981
  }
}
{
  "commitInfo": {
    "timestamp": 1660391659684,
    "operation": "OPTIMIZE",
    "operationParameters": {
      "predicate": "[]",
      "zOrderBy": "[\"loc\"]"
    },
    "readVersion": 4,
    "isolationLevel": "SnapshotIsolation",
    "isBlindAppend": false,
    "operationMetrics": {
      "numRemovedFiles": "2",
      "numRemovedBytes": "1933",
      "p25FileSize": "1020",
      "minFileSize": "1020",
      "numAddedFiles": "1",
      "maxFileSize": "1020",
      "p75FileSize": "1020",
      "p50FileSize": "1020",
      "numAddedBytes": "1020"
    },
    "engineInfo": "Apache-Spark/3.2.1-amzn-0 Delta-Lake/2.0.0",
    "txnId": "1847b6c9-3cf1-4918-b726-968ab91b28aa"
  }
}
```

The operation is commitInfo is "OPTIMIZE" and its parameter shows "zOrderBy": "[\"loc\"]".

## Query Data using Redshift Serverless Spectrum (all cool serverless stuff)
1. Create external schema which maps to the database created before in Athena.

```
create external schema athena_schema from data catalog 
database 'default'
iam_role '<your-redshift-role-arn>'
region '<your-region>'
```

2. Run the SQL to query the data.

```
SELECT * FROM "athena_schema"."deltatb_athena" ORDER BY id;
```
<img width="1440" alt="image" src="https://user-images.githubusercontent.com/14228056/184525700-d1bd491f-3479-497b-9113-593c1518e5a3.png">

## Commit Checkpoints
By default, the reference implementation creates a checkpoint [every 10 commits](https://github.com/delta-io/delta/blob/master/PROTOCOL.md).
<img width="1071" alt="image" src="https://user-images.githubusercontent.com/14228056/184568611-fa93bd3a-a21f-43a1-8613-7f6fe72a81a5.png">

## CDC Data Handling - Batch (to be updated)

![Delta-Lake-CDC-Batch](https://user-images.githubusercontent.com/14228056/184527213-c40cf9a5-26e8-44b1-a7a7-2be0489759dc.png)

[Sample DMS files](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Target.S3.html) to be handled:
```
I,101,Smith,Bob,4-Jun-14,New York
U,101,Smith,Bob,8-Oct-15,Los Angeles
U,101,Smith,Bob,13-Mar-17,Dallas
D,101,Smith,Bob,13-Mar-17,Dallas
```

## CDC Data Handling - Streaming (to be updated)

![Delta-Lake-CDC-Streaming](https://user-images.githubusercontent.com/14228056/184527588-cdd3dba6-4cb1-4842-b577-2b1b779fd339.png)

[Sample Debezium stream event](https://debezium.io/documentation/reference/stable/connectors/mysql.html) to be handled:
```
{
  "schema": { 
    "type": "struct",
    "fields": [
    ...(omitted)
  },
  "payload": { 
    "op": "c", 
    "ts_ms": 1465491411815, 
    "before": null, 
    "after": { 
      "id": 1004,
      "first_name": "Anne",
      "last_name": "Kretchmar",
      "email": "annek@noanswer.org"
    },
    "source": { 
      "version": "1.9.5.Final",
      "connector": "mysql",
      "name": "mysql-server-1",
      "ts_ms": 0,
      "snapshot": false,
      "db": "inventory",
      "table": "customers",
      "server_id": 0,
      "gtid": null,
      "file": "mysql-bin.000003",
      "pos": 154,
      "row": 0,
      "thread": 7,
      "query": "INSERT INTO customers (first_name, last_name, email) VALUES ('Anne', 'Kretchmar', 'annek@noanswer.org')"
    }
  }
}
```

## CDC Data Handling - All in one (to be updated)

Use EMR for both batch and streaming processing jobs:

![Delta-Lake-CDC-All](https://user-images.githubusercontent.com/14228056/184533669-89311be1-5dde-4bd6-b483-3170c76b4ec1.png)

## CDF Change Data Feed (to be updated)

Delta Lake 2.0+ allows users to capture the delta changes after *delta.enableChangeDataFeed* is enabled. Please refer to the blog [How to Simplify CDC With Delta Lake’s Change Data Feed](https://www.databricks.com/blog/2021/06/09/how-to-simplify-cdc-with-delta-lakes-change-data-feed.html) for more details.

```
cdf_df = spark.read.format("delta") \
         .option("readChangeFeed", "true") \
         .option("startingVersion", 10) \
         .table("default.deltatb")
```

And *spark-sql-delta-2-cdf-table.py* is an example for handling the changed part:
```
{
  "id": 17,
  "name": "opq",
  "loc": "bj",
  "_change_type": "insert",
  "_commit_version": 11,
  "_commit_timestamp": 4.5375430199729815031898112e+25
}
{
  "id": 18,
  "name": "rst",
  "loc": "sz",
  "_change_type": "insert",
  "_commit_version": 11,
  "_commit_timestamp": 4.5375430199729815031898112e+25
}
{
  "id": 19,
  "name": "uvw",
  "loc": "sh",
  "_change_type": "insert",
  "_commit_version": 11,
  "_commit_timestamp": 4.5375430199729815031898112e+25
}
```

Note: Remember to inlude parameter enableHiveSupport() in spark session~
