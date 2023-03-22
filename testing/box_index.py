from boxsdk import Client, OAuth2
from google.cloud import bigquery
import os
import pandas as pd
import time

# Box Client
auth = OAuth2(
    client_id='YOUR_CLIENT_ID',
    client_secret='YOUR_CLIENT_SECRET',
    access_token=os.getenv("ACCESS_TOKEN", 0)
)
boxclient = Client(auth)

# Big Query Client
bqclient = bigquery.Client()
project_id = 'rosenets'

# rows
rows = []


def add_rows_to_index(rows_to_insert):
    '''
    params: rows_to_insert
    adds rows to bq index
    '''
    print('add_rows_to_index called!')
    print('rows_to_insert=', rows_to_insert)
    
    pass

    # https://cloud.google.com/bigquery/docs/streaming-data-into-bigquery#streaminginsertexamples
    # TODO(developer): Set table_id to the ID of table to append to.
    table_id = "rosenets.nets_import.index"
    
    errors = bqclient.insert_rows_json(table_id, rows_to_insert)  # Make an API request.
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))


def list_box_folder(folder_id='156400695987'):
    '''
    takes folder id, defaults to "stata" folder in box
    returns list of rows to insert, where each row is a dict with filename and id
    '''
    
    folder = boxclient.folder(folder_id).get()
    print(f'Folder "{folder.name}" has {folder.item_collection["total_count"]} items in it:')
    
    items = boxclient.folder(folder_id).get_items()
    
    for item in items:
        print(f'{item.type} {item.id} is named "{item.name}"')
        if item.type == 'file':
            rows.append({"path": item.name, "file_id": item.id, "add_time": round(time.time())})
        if item.type == 'folder':
            # recursive call, ooh!
            list_box_folder(folder_id=item.id)
    
    return rows



rows_to_insert = list_box_folder()

add_rows_to_index(rows_to_insert)
