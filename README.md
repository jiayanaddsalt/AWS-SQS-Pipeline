# Scalable Survey Submission Pipeline with AWS SQS and Lambda Function

This project aims to create a scalable survey submission pipeline using AWS Simple Queue Service (SQS), Lambda Function, and S3. The code uses the `boto3` library to interact with AWS services to ensure reproducibility of the architecture.

The pipeline can be invoked on a survey participant’s mobile device when they complete a survey to send their survey submission into an SQS queue, which should then trigger a Lambda function. The Lambda function will take this survey submission data and perform necessary processing and storage operations in the cloud. 

# Files
- `boto3.ipynb` contains the code of the submission pipeline using the `boto3` library.
- `process_survey.py` contains the code of the Lambda function that processes the survey submission data. `process_survey.zip` is created by running `boto3.ipynb` and is the zip file of the Lambda function. 
- `q1-test-json/` contain test JSON files for the survey questions.