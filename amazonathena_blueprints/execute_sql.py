import time
import argparse
import os
import boto3


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--aws-access-key-id',
        dest='aws_access_key_id',
        required=True)
    parser.add_argument(
        '--aws-secret-access-key',
        dest='aws_secret_access_key',
        required=False)
    parser.add_argument(
        '--aws-default-region',
        dest='aws_default_region',
        required=True)
    parser.add_argument('--bucket-name', dest='bucket_name', required=True)
    parser.add_argument('--log-folder', dest='log_folder', required=False)
    parser.add_argument('--database', dest='database', required=False)
    parser.add_argument('--query', dest='query', required=True)
    args = parser.parse_args()
    return args


def set_environment_variables(args):
    """
    Set AWS credentials as environment variables if they're provided via keyword arguments
    rather than seeded as environment variables. This will override system defaults.
    """

    if args.aws_access_key_id:
        os.environ['AWS_ACCESS_KEY_ID'] = args.aws_access_key_id
    if args.aws_secret_access_key:
        os.environ['AWS_SECRET_ACCESS_KEY'] = args.aws_secret_access_key
    if args.aws_default_region:
        os.environ['AWS_DEFAULT_REGION'] = args.aws_default_region
    return


def poll_status(client, job_id):
    '''
    poll query status
    '''
    result = client.get_query_execution(
        QueryExecutionId=job_id
    )

    state = result['QueryExecution']['Status']['State']
    if state == 'SUCCEEDED':
        return result
    elif state == 'FAILED':
        error_msg = result['QueryExecution']['Status'].get('StateChangeReason')
        print(f'Query failed')
        print(error_msg)
        return result
    return False


def main():
    args = get_args()
    access_key = args.aws_access_key_id
    secret_key = args.aws_secret_access_key
    region_name = args.aws_default_region
    database = args.database
    bucket = args.bucket_name
    log_folder = args.log_folder
    query = args.query

    set_environment_variables(args)
    try:
        client = boto3.client(
            'athena',
            region_name=region_name,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key)
    except Exception as e:
        print(f'Failed to access Athena with specified credentials')
        raise(e)

    context = {}
    if database:
        context = {'Database': database}

    bucket = bucket.strip('/')
    if log_folder:
        log_folder = log_folder.strip('/')
        output = f's3://{bucket}/{log_folder}/'
    else:
        output = f's3://{bucket}/'

    job = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext=context,
        ResultConfiguration={'OutputLocation': output}
    )

    job_id = job['QueryExecutionId']

    status = poll_status(client, job_id)
    while not status:
        time.sleep(5)
        status = poll_status(client, job_id)

    if status['QueryExecution']['Status']['State'] != 'FAILED':
        print('Your query has been successfully executed.')


if __name__ == '__main__':
    main()
