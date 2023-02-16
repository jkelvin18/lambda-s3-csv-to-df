# AWS Lambda S3 Data Processing

A Python script that reads a CSV file from an S3 bucket, processes the data using Pandas, and loads it into an S3 bucket as a partitioned table.

## Dependencies

- Python 3
- boto3
- pandas

## Installation

1. Clone the repository: `git clone https://github.com/yourusername/your-repo.git`
2. Install dependencies: `pip install boto3 pandas`

## Usage

1. Update the S3 bucket name, object key prefix, and substring in the `lambda_handler` function.
2. Update the schema and output path variables to match your use case.
3. Update the `load_data` function in `app.lambda_function` to match your use case.
4. Zip the files: `zip -r9 function.zip .`
5. Create the AWS Lambda function using the `function.zip` file.
6. Add an S3 trigger to the Lambda function.

## Help

If you encounter any issues or errors, refer to the AWS Lambda and S3 documentation.

## Authors

- [James G Santos](https://github.com/jkelvin18)


