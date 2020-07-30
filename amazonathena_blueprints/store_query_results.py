import os
import time
import argparse

import boto3


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--access-key', dest='access_key', required=True)
    parser.add_argument('--secret-key', dest='secret_key', required=False)
    parser.add_argument('--region-name', dest='region_name', required=True)
    parser.add_argument('--bucket', dest='bucket', required=True)
    parser.add_argument('--log-folder', dest='log_folder', required=False)
    parser.add_argument('--database', dest='database', required=False)
    parser.add_argument('--query', dest='query', required=True)
    parser.add_argument('--destination-file-name', dest='destination_file_name',
            default='output.csv', required=True)
    parser.add_argument('--destination-folder-name',
            dest='destination_folder_name', default='', required=False)
    args = parser.parse_args()
    return args


def convert_to_boolean(string):
    """
    Shipyard can't support passing Booleans to code, so we have to convert
    string values to their boolean values.
    """
    if string in ['True', 'true', 'TRUE']:
        value = True
    else:
        value = False
    return value


def combine_folder_and_file_name(folder_name, file_name):
    """
    Combine together the provided folder_name and file_name into one path variable.
    """
    combined_name = os.path.normpath(
        f'{folder_name}{"/" if folder_name else ""}{file_name}')

    return combined_name


def create_csv(job_id, bucket, s3_client, destination_file_path):
    """
    Read in data from an Athena query. Store the data as a csv.
    """
    try:
        response = s3_client.Bucket(bucket).download_file(f'{job_id}.csv',
                                                    destination_file_path)
    except Exception as e:
        print(f'Failed to download query results to {destination_file_path}')
        raise(e)
    print(f'Successfully downloaded query results to {destination_file_path}')


def poll_status(client, job_id):
    '''
    poll query status
    '''
    result = client.get_query_execution(
        QueryExecutionId = job_id
    )

    state = result['QueryExecution']['Status']['State']
    if state == 'SUCCEEDED':
        print(f'Query completed')
        return result
    elif state == 'FAILED':
        error_msg = result['QueryExecution']['Status'].get('StateChangeReason')
        print(f'Query failed')
        print(error_msg)
        return result
    return False


def main():
    args = get_args()
    access_key = args.access_key
    secret_key = args.secret_key
    region_name = args.region_name
    database = args.database
    bucket = args.bucket
    log_folder = args.log_folder
    query = args.query
    destination_file_name = args.destination_file_name
    destination_folder_name = args.destination_folder_name
    destination_full_path = combine_folder_and_file_name(
        folder_name=destination_folder_name, file_name=destination_file_name)

    if not os.path.exists(destination_folder_name) and (
            destination_folder_name != ''):
        os.makedirs(destination_folder_name)

    try:
        client = boto3.client('athena', region_name=region_name,
                aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    except Exception as e:
        print(f'Failed to access Athena with specified credentials')
        raise(e)

    try:
        s3_client = boto3.resource('s3', region_name=region_name,
                aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    except Exception as e:
        print(f'Failed to access S3 Storage with specified credentials')
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

    create_csv(
        job_id=job_id,
        bucket=bucket,
        s3_client=s3_client,
        destination_file_path=destination_full_path)


if __name__ == '__main__':
    main()
