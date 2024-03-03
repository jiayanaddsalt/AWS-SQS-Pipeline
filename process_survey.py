import boto3
import json

s3 = boto3.client('s3')
bucket_name = 'jyassignment5'

dynamodb = boto3.resource('dynamodb')
table_name = 'survey_results'

# Get a reference to the table
table = dynamodb.Table(table_name)

def process_survey(event, context):
    
    # process event as passed down by sqs
    for record in event['Records']:
        event = record['body'].replace('\\n', '\n').strip('"')
        print(event)
    event = json.loads(event)  # parse JSON string into a dictionary object
    print(type(event))
        
    try:
        # if invalid
        if (event['time_elapsed'] <= 3) or (len(event.get('freetext', "")) == 0):
            return 400      
        
        # if valid
        else:
            # Create a unique identifier for the S3 object key
            object_key = event['user_id'] + event['timestamp'] + '.json'
            # Store the raw JSON data in S3
            s3.put_object(Body=json.dumps(event), Bucket=bucket_name, Key=object_key)

            # Construct the item to be inserted/updated in the table
            item = {
                'user_id': event['user_id'],
                'q1': event['q1'],
                'q2': event['q2'],
                'q3': event['q3'],
                'q4': event['q4'],
                'q5': event['q5'],
                'freetext': event['freetext'],
                'num_completed': 1
            }
            
            # Attempt to get the current number of completed surveys for the user
            try:
                response = table.get_item(
                    Key={
                    'user_id': event['user_id']
                    }
                )
                num_completed = response['Item']['num_completed']

                table.update_item(
                    Key={
                    'user_id': event['user_id']
                    },
                    UpdateExpression='SET q1 = :q1, q2 = :q2, q3 = :q3, q4 = :q4, q5 = :q5, freetext = :ft, num_completed = :val',
                    ExpressionAttributeValues={
                        ':q1': item['q1'],
                        ':q2': item['q2'],
                        ':q3': item['q3'],
                        ':q4': item['q4'],
                        ':q5': item['q5'],
                        ':ft': item['freetext'],
                        ':val': num_completed + 1
                    }
                )
                print('Updated')

            except:
                print('Put item in!')
                # If the user doesn't have a record in the table
                # Insert/update the item in the table
                table.put_item(
                    Item=item
                )

            return 200     
        
    except json.decoder.JSONDecodeError as e:
        return 400