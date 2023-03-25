# Lambda Function for Loading Data

This Python script is an AWS Lambda function that processes and loads data from a source S3 bucket to a destination S3 bucket.

## Prerequisites

- boto3
- pandas
- awswrangler
- datetime
- zipfile
- io

## Usage

1. Set up the necessary environment variables and input parameters for the Lambda function.
2. Deploy the function to AWS Lambda.
3. Trigger the Lambda function manually or through an event.

## Functionality

The script performs the following tasks:

1. Lists objects in the source S3 bucket with the specified prefix and substring.
2. Selects the latest object based on its last modified timestamp.
3. Reads the CSV data from the object and loads it into a pandas DataFrame.
4. Removes any existing partitions in the destination S3 bucket.
5. Writes the DataFrame to the destination S3 bucket in Parquet format, partitioned by the specified column.

## Input Parameters

The following input parameters are required:

- data_ref: Reference date (default: yesterday's date in YYYYMMDD format)
- bucket_dstn: Destination S3 bucket
- bucket_src: Source S3 bucket
- substr: Substring to filter objects in the source S3 bucket
- schema: Column names for the input CSV data
- pathoutput: Output path for the processed data
- table: Name of the table to be created
- database: Name of the database where the table will be created
- partitions_cols: Column(s) to partition the data by

## Function Definitions

The script contains the following function definitions:

1. `get_latest_matching_object(s3_client, bucket_src, prefix, substr)`: Lists objects in the source S3 bucket and returns the key of the latest object that matches the specified prefix and substring.

2. `load_data(df, pathoutput, table, database, data_ref, partitions_cols, bucket_dstn, s3_client, boto3_session)`: Loads the DataFrame to the destination S3 bucket in Parquet format, partitioned by the specified column.

3. `lambda_handler(event, context)`: Main Lambda function handler that processes the input event and context, reads data from the source S3 bucket, and loads it to the destination S3 bucket.

## Error Handling

The script contains error handling to manage exceptions that may occur during execution:

- Listing objects in the source S3 bucket
- Finding objects with the specified prefix and substring
- Reading the CSV data from the object
- Removing existing partitions in the destination S3 bucket
- Writing the DataFrame to the destination S3 bucket

In case of an error, the Lambda function returns an error message with details about the exception.

## Output

Upon successful execution, the Lambda function returns a "Success" message. In case of an error, it returns an error message with details about the exception.

## Additional Information

The script uses the following AWS SDKs and libraries:

- `boto3`: Provides an interface to interact with AWS services
- `awswrangler`: Simplifies the process of writing data to S3 in Parquet format
- `pandas`: Handles data manipulation and analysis
- `datetime`: Manipulates dates and times
- `zipfile`: Reads and writes ZIP archives
- `io`: Provides tools for working with input and output streams

## Deployment and Triggering

### Deployment

To deploy the Lambda function, follow these steps:

1. Create a new Lambda function in the AWS Management Console.
2. Set the runtime to Python 3.7 or later.
3. Upload the script file as the function code.
4. Ensure that the Lambda function has the necessary permissions and roles to access the specified S3 buckets and other resources.
5. Set the memory and timeout settings as needed.

### Triggering

You can trigger the Lambda function in one of the following ways:

1. Manually: Invoke the Lambda function directly from the AWS Management Console or using the AWS CLI.
2. Event-driven: Set up an event source, such as S3 bucket notifications, CloudWatch Events, or AWS Step Functions, to trigger the Lambda function automatically when a specified event occurs.

## Customization

You can customize the script to suit your specific use case by modifying the input parameters, function definitions, or the logic within the functions.

For example, you can:

- Change the data filtering or processing logic within the `lambda_handler` function.
- Modify the `load_data` function to support different file formats or compression algorithms.
- Update the `get_latest_matching_object` function to use a different sorting or filtering criteria.

## Limitations and Considerations

- The script assumes that the input data is in CSV format, and the output data is in Parquet format. If you need to process data in other formats, you may need to modify the script accordingly.
- The script processes a single object from the source S3 bucket at a time. If you need to process multiple objects concurrently, consider using AWS Lambda's concurrency features or parallelizing the execution using other AWS services, such as AWS Batch or AWS Step Functions.
- Be aware of AWS Lambda's limitations, such as memory, timeout, and deployment package size. Depending on your use case, you may need to adjust these settings or consider using other AWS services, such as AWS Fargate or Amazon EC2, for large-scale data processing tasks.
- When writing to the destination S3 bucket, the script overwrites any existing partitions. Be cautious when using this feature to avoid data loss.

## Testing and Debugging

To test and debug the Lambda function, follow these steps:

1. Create test events in the AWS Management Console that simulate the expected input for the Lambda function.
2. Invoke the Lambda function with the test events and review the output, logs, and metrics in the AWS Management Console.
3. Use the built-in logging features provided by AWS Lambda and the Python `logging` library to generate logs at different levels (e.g., `INFO`, `WARNING`, `ERROR`).
4. Monitor the Lambda function's execution using Amazon CloudWatch Logs and Amazon CloudWatch Metrics to identify potential issues, such as high memory usage, long execution times, or errors.
5. Set up Amazon CloudWatch Alarms to receive notifications when specific metrics thresholds are breached or when errors occur.

## Optimization and Best Practices

To optimize the performance and cost of the Lambda function, consider the following best practices:

1. Adjust the memory and timeout settings to match the requirements of your data processing task. Be aware that increasing the memory also increases the CPU power and network bandwidth proportionally.
2. Use Amazon S3 Transfer Acceleration to speed up the transfer of data between the source and destination S3 buckets, especially if they are located in different regions.
3. Enable AWS Lambda provisioned concurrency to reduce the latency caused by function initialization (i.e., "cold starts").
4. Use the AWS Lambda Power Tuning tool to find the optimal memory and timeout settings for your function based on your specific use case and cost constraints.
5. Enable Amazon S3 object lifecycle policies or Amazon S3 Inventory to manage the storage costs and lifecycle of your data.
