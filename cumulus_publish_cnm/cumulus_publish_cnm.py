import boto3
import json
import os


# load in Cumulus event (containing bucket and key)
# read in file, for each cnm, extract and trigger step function via SNS
def lambda_handler(event, context):
	# Environment Variables
	sns_endpoint = os.environ.get('sns_endpoint')
	list_key = os.environ.get('list_key')

	if not sns_endpoint:
		raise NameError('sns_endpoint is not set in environment')
	if not list_key:
		raise NameError('list_key is not set in environment')

	debug = os.environ.get('debug')  # Can be empty for debugging purpose

	s3 = boto3.resource('s3')
	sns = boto3.resource('sns')

	# Get 'replace' settings (S3 file content)
	bucket = event['replace']['Bucket']
	file = event['replace']['Key']

	content_object = s3.Object(bucket, file)
	file_content = content_object.get()['Body'].read().decode('utf-8')
	json_content = json.loads(file_content)

	# Trigger SNS into ingestion stream (for each granule)
	platform_endpoint = sns.PlatformEndpoint(sns_endpoint)
	response = []

	for item in json_content['payload'][list_key]:
		r = platform_endpoint.publish(
			Message=json.dumps(item)
		)
		response.append(r)

	if debug:
		print(f'sns_endpoint: {sns_endpoint}')
		print(f'list_key: {list_key}')
		print(f'debug: {debug}')
		print("\n===================================================\n")
		print(json.dumps(json_content))

	return response
