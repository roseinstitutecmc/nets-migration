from boxsdk import Client, OAuth2
from boxsdk.exception import BoxAPIException
import csv
from google.cloud import bigquery
import json
import os
import pandas as pd
import psutil
#import pyreadstat
import random
import sys
import time
import traceback

# Retrieve Job-defined env vars
TASK_INDEX = os.getenv("CLOUD_RUN_TASK_INDEX", 0)
TASK_ATTEMPT = os.getenv("CLOUD_RUN_TASK_ATTEMPT", 0)
# Retrieve User-defined env vars
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN", 0)


def log_memory_usage(when):
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    print(f"Memory usage {when}: {mem_info.rss / (1024 * 1024)} MB")


def main():
    # Random sleep to stagger start. Multiply by task index % 10
    sleep_time = random.uniform(1, 5) * (int(TASK_INDEX) % 10)
    print(f'Sleeping for {sleep_time} seconds...')
    time.sleep(sleep_time)
    log_memory_usage('Initial')
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

    # TEMP TODO REMOVE
    sql = """
    SELECT *
    FROM `rosenets.nets_import.index`
    WHERE path = 'NETS_var_CA_00.dta'
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

    log_memory_usage('Downloaded')

    print('Now reading file to DataFrame...')
    df_start_time = time.time()

    chunks = pd.read_stata(filename, chunksize=100000)
    header = True
    header_columns = None
    i = 0
    for chunk in chunks:
        # Get the column names from the first chunk
        if header_columns is None:
            header_columns = list(chunk.columns)
        # Process the chunk and write it to the CSV file
        chunk.to_csv((f'{filename}.csv'), header=header, mode='a', index=False)
        header = False
        log_memory_usage(f"Chunk {i}")
        i += 1

    '''
    reader = pyreadstat.read_file_in_chunks(pyreadstat.read_dta, filename, chunksize=10000)
    header = True
    i = 0
    for df, meta in reader:
        # df will contain 10K rows
        df.to_csv((f'{filename}.csv'), header=header, mode='a')
        header = False
        log_memory_usage(f"Chunk {i}")
        i += 1
    '''

    print('Done reading to DataFrame to csv!')

    df_end_time = time.time()
    df_time = df_end_time - df_start_time
    print('Converting took these many seconds:', df_time)
    
    print('Uploading csv to BigQuery')

    # From https://cloud.google.com/bigquery/docs/batch-loading-data#python
    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = f"rosenets.nets_import.{filename.split('.')[0]}"

    # Manually specify schema

    # Print the header (column names)
    print("Header (Column Names):", header_columns)

    schema = []
    schema_print = []

    # Check schema type
    if 'fix_ind' in filename:
        schema_path = 'schemas/fix_ind_schema.json'
    else:
        schema_path = 'schemas/CA_schema.json'
    
    # Load schema json
    with open(schema_path, 'r') as f:
        schema_json = json.load(f)
        # Iterate through varnames
        for varname in header_columns:
            vartype = [item['type'] for item in schema_json if item['name'] == varname][0]
            
            # Catch missing varname
            if not vartype:
                print(f'Name "{varname}" not found in the data')
                raise Exception()
            
            # Add schema field following this format
            # From https://cloud.google.com/bigquery/docs/schemas#python
            '''
            schema=[
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("post_abbr", "STRING"),
            ]
            '''
            schema.append(bigquery.SchemaField(varname, vartype))
            schema_print.append((varname, vartype))

    print('Schema is: ', schema_print)


    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, schema=schema,
    )

    with open(f'{filename}.csv', "rb") as source_file:
        job = bqclient.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result()  # Waits for the job to complete.

    table = bqclient.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )


# Start script
if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        message = f"Task #{TASK_INDEX}, " \
                  + f"Attempt #{TASK_ATTEMPT} failed: {str(err)}"
        traceback.print_exc()  # Prints the full traceback of the exception

        print(json.dumps({"message": message, "severity": "ERROR"}))
        sys.exit(1)  # Retry Job Task by exiting the process
