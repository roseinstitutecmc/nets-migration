from boxsdk import Client, OAuth2
from boxsdk.exception import BoxAPIException
from google.cloud import bigquery
import json
import os
import pyreadstat
import sys
import time

# Retrieve Job-defined env vars
TASK_INDEX = os.getenv("CLOUD_RUN_TASK_INDEX", 0)
TASK_ATTEMPT = os.getenv("CLOUD_RUN_TASK_ATTEMPT", 0)
# Retrieve User-defined env vars
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN", 0)


def main():
    # Box Client
    auth = OAuth2(
        client_id='YOUR_CLIENT_ID',
        client_secret='YOUR_CLIENT_SECRET',
        access_token=ACCESS_TOKEN)
    boxclient = Client(auth)

    # Big Query Client
    bqclient = bigquery.Client()
    project_id = 'rosenets'

    sql = """
    SELECT *
    FROM `rosenets.nets_import.index`
    WHERE start_id IS NULL AND add_time IS NOT NULL
    LIMIT 1
    """

    # Run a Standard SQL query with the project set explicitly
    query_df = bqclient.query(sql, project=project_id).to_dataframe()

    print("query_df['file_id']=", query_df['file_id'][0])

    file_id = query_df['file_id'][0]

    try:
        # From https://github.com/box/box-python-sdk/blob/main/docs/usage/files.md#download-a-file
        print('Downloading from Box...')
        dl_start_time = time.time()

        filename = boxclient.file(file_id).get().name
        # Write the Box file contents to disk
        output_file = open(filename, 'wb')
        boxclient.file(file_id).download_to(output_file)
    except BoxAPIException as box_exception:
        print('You not authed with box lol')
        print(box_exception)

    print('file downloaded from box with name', filename)
    dl_end_time = time.time()
    dl_time = dl_end_time - dl_start_time
    print('Download took these many seconds:', dl_time)

    print('Now reading file to DataFrame...')
    df_start_time = time.time()

    dta_df, meta = pyreadstat.read_dta(filename)

    print('Done reading to DataFrame!')

    df_end_time = time.time()
    df_time = df_end_time - df_start_time 
    print('Converting took these many seconds:', df_time)

    print('Size of dta_df=      ', dta_df.__sizeof__())

    print('Shape o dta_df=', dta_df.shape)

    print('dta_df head=\n', dta_df.head())

    print('This is where df should be uploaded to BigQuery')


# Start script
if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        message = f"Task #{TASK_INDEX}, " \
                  + f"Attempt #{TASK_ATTEMPT} failed: {str(err)}"

        print(json.dumps({"message": message, "severity": "ERROR"}))
        sys.exit(1)  # Retry Job Task by exiting the process
