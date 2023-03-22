from google.cloud import bigquery

client = bigquery.Client()

# sample query
other_sql = """
    SELECT name
    FROM `bigquery-public-data.usa_names.usa_1910_current`
    WHERE state = 'TX'
    LIMIT 100
"""

sql = """
SELECT *
FROM `rosenets.nets_import.index`
WHERE start_id IS NULL AND add_time IS NOT NULL
LIMIT 1
"""


'''
# Run a Standard SQL query using the environment's default project
df = client.query(sql).to_dataframe()
'''



# Run a Standard SQL query with the project set explicitly
project_id = 'rosenets'
df = client.query(sql, project=project_id).to_dataframe()

print('df.shape=', df.shape)
print('df.head()= \n', df.head())
