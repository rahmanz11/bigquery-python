import os
from google.cloud import bigquery
import requests

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="my-project-first-157307-7964c7f4aa40.json"
client = bigquery.Client()
# dataset_id = "{}.mydataset".format(client.project)

# dataset = bigquery.Dataset(dataset_id)
# dataset.location = "US"
# dataset = client.create_dataset(dataset, timeout=30)
# print("Created dataset {}.{}".format(client.project, dataset.dataset_id))

# table_id = "{}.mytable".format(dataset_id)

# schema = [
#     bigquery.SchemaField("id", "INTEGER"),
#     bigquery.SchemaField("email", "STRING"),
#     bigquery.SchemaField("first_name", "STRING"),
#     bigquery.SchemaField("last_name", "STRING"),    
#     bigquery.SchemaField("avatar", "STRING")  
# ]

# table = bigquery.Table(table_id, schema=schema)
# table = client.create_table(table)
# print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

table_id = "my-project-first-157307.mydataset.mytable"
url = "https://reqres.in/api/users"
response = requests.get(url, data=None,headers={"Content-Type": "application/json"})
data = response.json()['data']

errors = client.insert_rows_json(table_id, data)
if errors == []:
    print("New rows have been added.")
else:
    print("Encountered errors while inserting rows: {}".format(errors))