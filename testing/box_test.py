#from boxsdk import DevelopmentClient

from boxsdk import Client, OAuth2
import os
import pandas as pd


auth = OAuth2(
    client_id='YOUR_CLIENT_ID',
    client_secret='YOUR_CLIENT_SECRET',
    access_token=os.getenv("ACCESS_TOKEN", 0)
)
client = Client(auth)


# list items in folder
folder_id = '156400695987'
items = client.folder(folder_id).get_items()
for item in items:
    print(f'{item.type} {item.id} is named "{item.name}"')

# get file name
'''
file_id = '981951053614'
filename = client.file(file_id).get().name
print('filename=', filename)
'''
